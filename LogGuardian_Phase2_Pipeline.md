# LogGuardian - Phase 2 : Pipeline de detection d'anomalies

## Contexte

La Phase 1 a mis en place l'infrastructure AWS (VPC, EKS, ECR, S3, CodePipeline, CloudWatch, Secrets Manager).
La Phase 2 implemente le pipeline applicatif de detection d'anomalies en 3 microservices :

```
[Sources de logs] -> log-generator -> Kafka(logs-raw) -> etl-processor -> Kafka(logs-processed)
                                                                   |              |
                                                                   v              v
                                                              S3 Parquet      ml-model
                                                                                  |
                                                                                  v
                                                                          Kafka(logs-anomalies-ml)
```

| Service | Statut | Role |
|---|---|---|
| `log-generator` | **Implemente** | Lit les fichiers de logs locaux et les publie dans Kafka |
| `etl-processor` | **Implemente** | Consomme les logs bruts, les enrichit, les stocke en Parquet |
| `ml-model` | **A faire** (cette section) | LSTM Autoencoder pour la detection d'anomalies |
| `monitoring-ui` | A faire (Phase 3) | Dashboard de visualisation des alertes |

L'infrastructure AWS de la Phase 1 est reutilisee a 100% : les images Docker sont poussees vers ECR via CodePipeline, deployees sur EKS, et les modeles + Parquet vivent dans S3.

---

# 1. log-generator (IMPLEMENTE)

## Role

Simule un flux temps reel de logs en relisant en boucle des fichiers de logs reels (datasets Loghub).
C'est le point d'entree du pipeline.

## Architecture

```
data/Linux.log         --> linux_parser.py      --.
data/SSH.log           --> ssh_parser.py        |
data/hadoop/**/        --> hadoop_parser.py     +--> LogEntry (schema unifie) --> KafkaProducer --> logs-raw
data/spark/*           --> spark_parser.py      |
data/supercomputer/    --> supercomputer_parser |
data/HDFS TraceBench/  --> hdfs_parser.py       --'
```

## Structure du projet

```
log-generator/
├── src/
│   ├── main.py                       # Point d'entree, boucle infinie de production
│   ├── config.py                     # Variables d'environnement
│   ├── producer.py                   # KafkaProducer avec retry automatique
│   └── parsers/
│       ├── __init__.py               # Registry : nom -> classe parser
│       ├── base.py                   # Classes LogEntry et BaseParser
│       ├── linux_parser.py           # Format syslog Linux
│       ├── ssh_parser.py             # Format sshd
│       ├── hadoop_parser.py          # Format YARN / MapReduce
│       ├── spark_parser.py           # Format Spark application logs
│       ├── supercomputer_parser.py   # Format BGL (Blue Gene/L)
│       └── hdfs_parser.py            # Format CSV TraceBench
├── tests/test_parsers.py
├── Dockerfile
└── requirements.txt
```

## Schema JSON unifie produit dans `logs-raw`

```json
{
  "timestamp":  "2015-10-17T15:37:56.547",
  "source":     "hadoop",
  "host":       "application_1445062781478_0011",
  "level":      "INFO",
  "component":  "org.apache.hadoop.mapreduce.v2.app.MRAppMaster",
  "message":    "Created MRAppMaster for application appattempt_...",
  "raw":        "2015-10-17 15:37:56,547 INFO [main] org.apache..."
}
```

## Logique de fonctionnement

1. Charge la config (variables d'env ou `.env`)
2. Connecte le KafkaProducer (retry 10x toutes les 5s)
3. Instancie les parsers des sources actives (`LOG_SOURCES`)
4. Boucle infinie :
   - Chaine tous les parsers en un seul flux (`itertools.chain`)
   - Pour chaque LogEntry : serialise en JSON UTF-8 et envoie dans `logs-raw`
   - Key Kafka = nom de la source, value = JSON
   - Quand tous les fichiers sont epuises, recommence depuis le debut (replay)
5. La vitesse est controlee par `REPLAY_SPEED` (messages par seconde)

## Configuration

| Variable | Defaut | Role |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Adresse du broker |
| `KAFKA_TOPIC` | `logs-raw` | Topic cible |
| `REPLAY_SPEED` | `1.0` | Messages par seconde (0 = illimite) |
| `DATA_DIR` | `/data` | Dossier des fichiers de logs |
| `LOG_SOURCES` | `linux,ssh,hadoop,spark,supercomputer,hdfs` | Sources actives |

---

# 2. etl-processor (IMPLEMENTE)

## Role

Pont entre Kafka et le reste du pipeline :
1. Consomme les logs bruts depuis `logs-raw`
2. Valide, enrichit, normalise
3. Stocke en Parquet partitionne (S3 ou local)
4. Republie dans `logs-processed` (pour le ML) et `logs-anomalies` (filtre rules-based ERROR/FATAL)

## Architecture

```
Kafka logs-raw
       |
       v
KafkaConsumer (batching : 500 msg ou 30s)
       |
       v
Transformer
  - Validation du schema
  - Normalisation timestamp (UTC ISO 8601)
  - Normalisation level (WARNING -> WARN)
  - Ajout : year / month / day / hour (partition cols)
  - Ajout : is_anomaly_candidate (level in {ERROR, FATAL})
  - Ajout : processed_at
       |
       +-----------------> Kafka logs-processed   (TOUS les logs enrichis)
       |
       +-----------------> Kafka logs-anomalies   (filtre level ERROR/FATAL, key = level)
       |
       v
S3 Parquet
  s3://logguardian-datalake-148761640356/logs/
    source=ssh/year=2024/month=04/day=23/batch_xxx.parquet
```

## Structure du projet

```
etl-processor/
├── src/
│   ├── main.py             # Boucle consume -> transform -> load -> publish
│   ├── config.py           # Variables d'environnement
│   ├── consumer.py         # KafkaConsumer + logique de batching
│   ├── transformer.py      # Validation + enrichissement + normalisation
│   ├── s3_loader.py        # Serialisation Parquet + upload S3 (ou local)
│   └── producer.py         # Publication logs-processed + logs-anomalies
├── tests/test_transformer.py
├── Dockerfile
└── requirements.txt
```

## Schema enrichi produit

Les 7 champs du log-generator + 6 champs ajoutes :

| Champ ajoute | Type | Source |
|---|---|---|
| `year` / `month` / `day` / `hour` | int | Extraits de `timestamp` |
| `is_anomaly_candidate` | bool | `level in {ERROR, FATAL}` |
| `processed_at` | string ISO | UTC now au moment du transform |

## Strategie de batching

Le batch se declenche quand l'une des deux conditions est remplie :

| Condition | Variable | Defaut |
|---|---|---|
| Nombre de messages accumules | `BATCH_SIZE` | 500 |
| Temps depuis le dernier flush | `BATCH_TIMEOUT_SEC` | 30s |

## Partitionnement S3

```
s3://logguardian-datalake-148761640356/
  logs/
    source=linux/year=2024/month=04/day=23/batch_20240423_153012.parquet
    source=ssh/year=2024/month=04/day=23/batch_20240423_153045.parquet
    source=hadoop/...
    source=spark/...
    source=supercomputer/...
    source=hdfs/...
```

Format **Parquet + compression snappy** : columnar, rapide a lire pour le ML.

## Topics Kafka produits

| Topic | Contenu | Volume | Key |
|---|---|---|---|
| `logs-processed` | Tous les logs enrichis (100%) | Plein flux | source |
| `logs-anomalies` | Filtre rules-based level=ERROR/FATAL (5-15%) | Subset | level |

---

# 3. ml-model (A IMPLEMENTER)

## Role

Detection d'anomalies semantiques via un **LSTM Autoencoder** :
- Apprend a reconstruire des sequences de logs **normales**
- A l'inference, score chaque sequence par son erreur de reconstruction
- Si erreur > seuil : anomalie publiee dans `logs-anomalies-ml`

A la difference de `logs-anomalies` (rules-based, base sur le `level`), `logs-anomalies-ml` est **base sur la semantique** : peut detecter des anomalies dans des logs `INFO` qui forment une sequence inhabituelle.

## Architecture globale

```
                    +-------------------- TRAINING (offline) --------------------+
                    |                                                            |
                    |   S3 Parquet --> Feature extraction --> LSTM Autoencoder   |
                    |   (logs/)        (vectorisation)         (PyTorch)         |
                    |                          |                     |           |
                    |                          v                     v           |
                    |                  vocabulary.pkl       lstm_autoencoder.pt  |
                    |                  feature_scaler.pkl   threshold.json       |
                    |                          |                     |           |
                    |                          +---------+-----------+           |
                    |                                    |                       |
                    |                                    v                       |
                    |                          S3 logguardian-models/            |
                    |                                    |                       |
                    +------------------------------------+-----------------------+
                                                         |
                                                         | (download au demarrage)
                                                         v
                    +------------------- INFERENCE (online) --------------------+
                    |                                                            |
                    |   Kafka logs-processed                                    |
                    |          |                                                |
                    |          v                                                |
                    |   KafkaConsumer                                           |
                    |          |                                                |
                    |          v                                                |
                    |   Buffer glissant par (source, host) - taille N=10        |
                    |          |                                                |
                    |          v                                                |
                    |   Feature extraction (meme code que training)             |
                    |          |                                                |
                    |          v                                                |
                    |   LSTM Autoencoder.score(sequence) = MSE reconstruction   |
                    |          |                                                |
                    |          +-- score <= threshold -> drop                   |
                    |          |                                                |
                    |          +-- score >  threshold -> Kafka logs-anomalies-ml|
                    |                                                            |
                    +------------------------------------------------------------+
```

## Comment fonctionne un LSTM Autoencoder pour les logs

```
Sequence de N logs              Latent vector            Sequence reconstruite
[log1, log2, ..., logN]   --->     [...]      --->      [log1', log2', ..., logN']
        Input                    Encoded                       Output

reconstruction_error = MSE(Input, Output)

Si error > seuil  ===>  ANOMALIE
```

Le modele est entraine **uniquement sur des donnees majoritairement normales**. Quand il rencontre une sequence atypique, il n'arrive pas a la reconstruire correctement et l'erreur explose.

## Structure du projet

```
ml-model/
├── src/
│   ├── trainer/
│   │   ├── __init__.py
│   │   ├── train.py              # Script d'entrainement (entry point offline)
│   │   ├── features.py           # Feature extraction + tokenization
│   │   ├── dataset.py            # PyTorch Dataset depuis Parquet
│   │   └── model.py              # Definition LSTMAutoencoder (PyTorch)
│   │
│   ├── inference/
│   │   ├── __init__.py
│   │   ├── detector.py           # Charge le modele + scoring
│   │   └── buffer.py             # Buffer glissant par (source, host)
│   │
│   ├── consumer.py               # KafkaConsumer
│   ├── producer.py               # KafkaProducer vers logs-anomalies-ml
│   ├── config.py                 # Variables d'environnement
│   └── main.py                   # Entry point inference (CMD du Dockerfile)
│
├── models/                        # Artefacts produits par train.py
│   ├── lstm_autoencoder.pt       # Poids PyTorch
│   ├── vocabulary.pkl            # Mapping token -> id
│   ├── feature_scaler.pkl        # StandardScaler sklearn
│   └── threshold.json            # Seuil de detection (percentile 95 ou 99)
│
├── tests/
│   ├── test_features.py
│   └── test_buffer.py
│
├── Dockerfile                    # Un seul Dockerfile pour MVP (training + inference)
├── requirements.txt
└── .env.example
```

## Feature extraction (vectorisation des logs)

Chaque log est transforme en un vecteur de taille fixe :

| Feature | Methode | Dimensions |
|---|---|---|
| `source` | one-hot (6 sources) | 6 |
| `level` | one-hot (5 niveaux) | 5 |
| `hour` | encoding cyclique sin/cos | 2 |
| `message` | tokenisation + moyenne d'embeddings (vocab top 5000) | 64 |
| **Total** | | **77 dims** |

## Sequencing

On groupe les logs par `source + host` et on cree des sequences glissantes de **N=10 logs consecutifs** :

```
[log1, log2, log3, log4, log5, log6, log7, log8, log9, log10]   --> sequence 1
[log2, log3, log4, log5, log6, log7, log8, log9, log10, log11]  --> sequence 2
[log3, log4, ...]                                                 --> sequence 3
```

## Architecture du modele PyTorch

```
Input:    (batch, seq_len=10, features=77)
   |
   v
Encoder LSTM (hidden_size=64, num_layers=1)
   |
   v
Latent vector  (batch, 32)
   |
   v
Decoder LSTM (hidden_size=64, num_layers=1)
   |
   v
Output:   (batch, seq_len=10, features=77)

Loss : MSE entre Input et Output
Optimizer : Adam, lr=1e-3
Epochs : 30
Batch size : 64
```

## Calcul du seuil d'anomalie

Apres l'entrainement :

1. Calcul des erreurs de reconstruction sur le **validation set**
2. Distribution des erreurs analysee (histogramme + percentiles)
3. Seuil fixe au **percentile 95** par defaut (ajustable)
4. Sauvegarde dans `threshold.json` :

```json
{
  "threshold": 0.0421,
  "percentile": 95,
  "computed_at": "2024-04-24T15:30:00Z",
  "n_samples": 12000
}
```

Tout score > 0.0421 a l'inference = anomalie.

## Logique d'inference temps reel

```
1. Demarrage du pod
   - Telecharge models/* depuis S3 (s3://logguardian-models/)
   - Charge le LSTM en RAM (PyTorch eval mode)
   - Charge le vocabulaire et le scaler
   - Charge le threshold

2. Connexion Kafka
   - Consumer: logs-processed (group.id = ml-model)
   - Producer: logs-anomalies-ml

3. Boucle infinie
   - Pour chaque message JSON recu :
     - Extrait (source, host, ...)
     - Ajoute le log au buffer glissant de cette cle (source, host)
     - Si buffer atteint N=10 logs :
       - Vectorise les 10 logs (features.py)
       - Forward pass dans le LSTM
       - Calcule MSE entre input et output
       - Si MSE > threshold : publie dans logs-anomalies-ml
       - Glisse le buffer (drop le plus ancien)
```

## Schema des messages publies dans `logs-anomalies-ml`

```json
{
  "detected_at": "2024-04-24T15:30:12.456Z",
  "source": "ssh",
  "host": "LabSZ",
  "anomaly_score": 0.0892,
  "threshold": 0.0421,
  "severity_ratio": 2.12,
  "sequence": [
    { "timestamp": "...", "level": "WARN", "message": "Invalid user webmaster" },
    { "timestamp": "...", "level": "WARN", "message": "Failed password for invalid user" },
    ...
  ],
  "model_version": "lstm_v1_20240424"
}
```

## Configuration

| Variable | Defaut | Role |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker Kafka |
| `KAFKA_INPUT_TOPIC` | `logs-processed` | Topic source |
| `KAFKA_OUTPUT_TOPIC` | `logs-anomalies-ml` | Topic des anomalies detectees |
| `KAFKA_GROUP_ID` | `ml-model` | Consumer group |
| `MODEL_DIR` | `/app/models` | Dossier local des artefacts |
| `S3_MODELS_BUCKET` | `logguardian-models-148761640356` | Bucket des modeles |
| `SEQUENCE_LENGTH` | `10` | Taille de la fenetre glissante |
| `DEVICE` | `cpu` | `cpu` ou `cuda` |
| `LOCAL_MODEL_MODE` | `false` | Si true, ne telecharge pas depuis S3 |

## Strategie : un seul Dockerfile (decision pour MVP)

Pour le MVP, on utilise **un seul Dockerfile** qui contient PyTorch + tous les outils :

| Avantages | Inconvenients |
|---|---|
| Plus simple a maintenir | Image plus grosse (~1.5 Go avec PyTorch CPU) |
| Reutilisable pour training et inference | Inference embarque des libs inutiles |
| Un seul build dans CodePipeline | Impossible de faire training GPU sans rebuild |

Le **CMD par defaut** lance l'inference. Pour entrainer, on fait :

```bash
# Training (manuel, ponctuel)
docker run --rm \
  -v $PWD/data:/data \
  -v $PWD/models:/app/models \
  logguardian/ml-model:latest \
  python -m trainer.train --input /data --output /app/models

# Inference (CMD par defaut, deploye sur EKS)
docker run logguardian/ml-model:latest
```

En Phase B (apres soutenance ou si besoin de scaler), on pourra splitter en 2 images (training avec PyTorch CUDA + inference legere).

## Deux entry points distincts

| Commande | Quand | Ou |
|---|---|---|
| `python -m trainer.train` | Une fois (puis re-train periodique) | Local pour le MVP, SageMaker plus tard |
| `python main.py` | En continu (24/7) | Container deploye sur EKS |

---

# 4. Integration avec l'infrastructure AWS existante (Phase 1)

L'implementation de la Phase 2 utilise toute l'infra deja en place :

| Service AWS | Utilisation pour la Phase 2 |
|---|---|
| **EKS cluster `logguardian`** | Heberge les pods log-generator, etl-processor, ml-model |
| **ECR `logguardian/*`** | 4 registres deja crees ; on push les images Docker dedans |
| **S3 `logguardian-datalake-148761640356`** | Stockage des Parquet de l'ETL et lecture pour le training ML |
| **CodePipeline `logguardian-pipeline`** | Build automatique des images sur chaque push develop -> push ECR |
| **CodeBuild `logguardian-build`** | Execute le `buildspec.yml` qui build les 4 Dockerfiles |
| **CloudWatch Logs** | Reception des logs de tous les pods via Fluent Bit |
| **Secrets Manager `logguardian/config`** | Stockage des config sensibles (API keys, seuils) |
| **SNS `logguardian-alerts`** | Cible des notifications quand une anomalie est detectee |

## Flux CI/CD complet

```
git push develop
       |
       v
CodePipeline.Source (GitHub via CodeStar Connections)
       |
       v
CodeBuild (buildspec.yml)
       |
       +--> docker build log-generator/   --> push ECR logguardian/log-generator:<sha>
       +--> docker build etl-processor/   --> push ECR logguardian/etl-processor:<sha>
       +--> docker build ml-model/        --> push ECR logguardian/ml-model:<sha>
       +--> docker build monitoring-ui/   --> push ECR logguardian/monitoring-ui:<sha>
       |
       v
(manuel pour MVP) kubectl apply -f k8s/
       |
       v
EKS cluster logguardian
       |
       +--> Pod log-generator    (1 replica)
       +--> Pod etl-processor    (1-2 replicas)
       +--> Pod ml-model         (1 replica, GPU optionnel plus tard)
       +--> Pod monitoring-ui    (1 replica)
       |
       v
Acces via Ingress / ALB
```

## Manifests Kubernetes a creer (k8s/)

```
k8s/
├── kafka.yaml              # Deployment Kafka + Zookeeper (ou MSK plus tard)
├── log-generator.yaml      # Deployment + ConfigMap
├── etl-processor.yaml      # Deployment + ServiceAccount IAM (acces S3)
├── ml-model.yaml           # Deployment + Volume (cache modele)
├── monitoring-ui.yaml      # Deployment + Service + Ingress
└── kustomization.yaml      # (optionnel) regroupement
```

Pour le MVP, Kafka tourne en cluster sur EKS. Pour la production, migrer vers **Amazon MSK**.

## Bucket S3 a ajouter

| Bucket | Usage |
|---|---|
| `logguardian-datalake-148761640356` | EXISTE - Parquet ETL + datasets training |
| `logguardian-models-148761640356` | A CREER - artefacts ML (lstm.pt, vocab.pkl, ...) |

```bash
aws s3api create-bucket \
  --bucket logguardian-models-148761640356 \
  --region eu-west-1 \
  --create-bucket-configuration LocationConstraint=eu-west-1
```

## IAM Service Accounts pour les pods

Pour que les pods accedent a S3 sans hardcoder de credentials, utiliser **IRSA (IAM Roles for Service Accounts)** :

| ServiceAccount | Permissions |
|---|---|
| `etl-processor-sa` | s3:PutObject sur `logguardian-datalake-*/logs/*` |
| `ml-model-sa` | s3:GetObject sur `logguardian-models-*/*`, s3:ListObjects sur `logguardian-datalake-*/logs/*` |

---

# 5. Topics Kafka du systeme complet

| Topic | Producer | Consumer(s) | Description |
|---|---|---|---|
| `logs-raw` | log-generator | etl-processor | Logs bruts non valides |
| `logs-processed` | etl-processor | ml-model | Logs enrichis (100%) |
| `logs-anomalies` | etl-processor | monitoring-ui | Filtre rules-based level=ERROR/FATAL |
| `logs-anomalies-ml` | ml-model | monitoring-ui, alert-service (futur) | Anomalies detectees par le LSTM |

---

# 6. Plan d'implementation (ordre)

| Etape | Quoi | Statut |
|---|---|---|
| 1 | log-generator + Kafka local (docker-compose) | DONE |
| 2 | etl-processor + 2 topics de sortie | DONE |
| 3 | **ml-model : feature extraction + dataset PyTorch** | TODO |
| 4 | **ml-model : LSTM Autoencoder + script de training** | TODO |
| 5 | **ml-model : service d'inference + buffer glissant** | TODO |
| 6 | **ml-model : Dockerfile + integration docker-compose** | TODO |
| 7 | Manifests Kubernetes + deploiement EKS | TODO |
| 8 | monitoring-ui (dashboard React + FastAPI) | TODO |
| 9 | Auto-remediation Kubernetes (kill/restart pods) | TODO |
| 10 | Migration training local -> SageMaker (avant soutenance) | TODO |

---

# 7. Stack technique consolidee

## log-generator + etl-processor (deja installe)

```
kafka-python==2.0.2
python-dotenv==1.0.0
pandas==2.2.2          # ETL only
pyarrow==16.0.0        # ETL only
boto3==1.34.0          # ETL only
```

## ml-model (a installer)

```
torch==2.3.0           # PyTorch CPU (pour MVP)
pandas==2.2.2
pyarrow==16.0.0        # Lecture des Parquet
numpy==1.26.4
scikit-learn==1.4.2    # StandardScaler + train_test_split
kafka-python==2.0.2
boto3==1.34.0          # Telechargement des modeles depuis S3
python-dotenv==1.0.0
```

---

# 8. Couts estimes (Phase 2)

| Element | Cout/mois |
|---|---|
| Cluster EKS (control plane) | 73$ |
| 2x t3.medium worker nodes | ~60$ |
| NAT Gateway | ~32$ |
| S3 storage (Parquet ~10 Go) | ~1$ |
| ECR storage | ~1$ |
| Kafka self-hosted sur EKS | inclus dans EC2 |
| **Total Phase 2** | **~170$/mois** |

Migration MSK + SageMaker en Phase B ajoutera ~100$/mois si laisse tourner en continu, mais peut etre a la demande pour la soutenance (~5-10$ ponctuel).

---

# Resume executif

**Ce qui marche deja :**
- Pipeline complet de logs : 6 sources -> Kafka -> Parquet S3
- Topic `logs-anomalies` avec filtre rules-based
- Tests unitaires des parsers et du transformer
- Docker Compose local fonctionnel

**Prochaine etape immediate :**
Implementer le service `ml-model` avec :
- Un seul Dockerfile (training + inference)
- LSTM Autoencoder PyTorch
- Feature extraction reutilisable
- Buffer glissant pour le streaming
- Publication des anomalies semantiques dans `logs-anomalies-ml`

**Apres :**
- Manifests K8s pour deployer sur EKS
- monitoring-ui (dashboard)
- Auto-remediation
- Migration training vers SageMaker pour la soutenance
