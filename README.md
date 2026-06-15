# LogGuardian — AIOps Platform

Plateforme de détection d'anomalies en temps réel sur des flux de logs, basée sur un LSTM Autoencoder.

---

## Stack technique

| Domaine | Technologie |
|---|---|
| Source de données | Loghub Dataset (GCS) |
| Bus de messages | Apache Kafka |
| Traitement | Python (ETL processor) |
| NLP | Tokenisation + Mean Embedding (77 dimensions) |
| Deep Learning | LSTM Autoencoder (PyTorch) |
| Explication IA | RAG (panneau dans l'UI) |
| Cloud | GCP — GKE, GCS, Cloud Build, Artifact Registry, Cloud Monitoring |
| IaC | Terraform |
| Orchestration | Docker + Kubernetes (GKE) |

---

## Informations GCP

```
Project ID   : logguardian-497218
Region       : europe-west1
Zone         : europe-west1-b
Cluster GKE  : logguardian
Node pool    : logguardian-nodes-standard (e2-standard-2, non-préemptible, autoscaling 1-3)
Namespace K8s: logguardian
```

### Buckets GCS

```
Logs Parquet : gs://logguardian-datalake-logguardian-497218/
Modèles ML   : gs://logguardian-models-logguardian-497218/
Logs bruts   : gs://logguardian-datalake-logguardian-497218/raw-logs/
```

### Artifact Registry

```
Registry : europe-west1-docker.pkg.dev/logguardian-497218/logguardian/
Images   : log-generator | etl-processor | ml-model | monitoring-ui
```

### Service Account

```
GSA  : terraform-sa@logguardian-497218.iam.gserviceaccount.com
Rôles: roles/storage.objectAdmin (les deux buckets GCS)
       Workload Identity → tous les KSA du namespace logguardian
```

---

## Architecture des services

```
log-generator → [logs-raw] → etl-processor → [logs-processed] → ml-model → [logs-anomalies-ml] → monitoring-ui
                                    ↓
                              GCS Parquet (historique + réentraînement)
```

### Topics Kafka

| Topic | Producteur | Consommateur |
|---|---|---|
| `logs-raw` | log-generator | etl-processor |
| `logs-processed` | etl-processor | ml-model |
| `logs-anomalies` | etl-processor | — (rule-based ETL) |
| `logs-anomalies-ml` | ml-model | monitoring-ui |

---

## Fichiers clés

```
logguardian/
├── k8s/
│   ├── log-generator.yaml      # PVC 10Gi + init container gsutil sync
│   ├── kafka.yaml              # Zookeeper + Kafka (retention 1h/512MB)
│   ├── etl-processor.yaml      # Workload Identity GCS
│   ├── ml-model.yaml           # Workload Identity GCS models
│   └── monitoring-ui.yaml      # LoadBalancer externe
├── log-generator/src/parsers/  # linux, ssh, hadoop, spark, supercomputer, android
├── etl-processor/src/
│   ├── main.py                 # Pipeline ETL principal
│   ├── transformer.py          # Normalisation + flag anomalie (ERROR/FATAL)
│   ├── gcs_loader.py           # Upload Parquet → GCS
│   └── config.py               # GCS_BUCKET, GCS_PREFIX
├── ml-model/src/
│   ├── main.py                 # Boucle inference Kafka
│   ├── inference/detector.py   # LSTM scoring + download GCS
│   ├── inference/buffer.py     # Buffer glissant (source, host) seq_len=10
│   ├── trainer/features.py     # Vectorisation 77 dims
│   ├── trainer/model.py        # LSTMAutoencoder PyTorch
│   └── config.py               # GCS_MODELS_BUCKET, SEQUENCE_LENGTH=10
├── monitoring-ui/src/app.py    # Dashboard Dash + consumer Kafka thread
├── ml-model/models/
│   └── threshold.json          # threshold=0.5 (modifiable)
├── cloudbuild.yaml             # CI/CD Cloud Build
├── monitoring/
│   ├── dashboard.json          # Dashboard Cloud Monitoring
│   ├── alert-disk-pressure.json
│   └── alert-pod-restarts.json
└── docs/
    └── architecture-gcp.drawio
```

---

## Modèle ML

### Artefacts dans GCS (`gs://logguardian-models-logguardian-497218/`)

```
lstm_autoencoder.pt    # Poids du modèle PyTorch
vocabulary.pkl         # Vocabulaire (5000 tokens)
embedding_table.npy    # Table d'embeddings (5001 x 64)
feature_scaler.pkl     # StandardScaler fitté sur données normales
threshold.json         # Seuil MSE = 0.5 (modifiable)
```

### Paramètres du modèle

```
n_features   : 77  (6 source + 5 level + 2 heure + 64 embedding)
seq_len      : 10  (séquences de 10 logs)
hidden_size  : 64
latent_size  : 32
threshold    : 0.5 (p50 — abaissé depuis p95=1.307 pour détecter SSH)
train_loss   : 0.2696
val_loss     : 0.2641
```

### Modifier le seuil

```bash
# Editer ml-model/models/threshold.json → changer "threshold"
gsutil cp ml-model/models/threshold.json gs://logguardian-models-logguardian-497218/threshold.json
kubectl rollout restart deployment/ml-model -n logguardian
```

### Réentraîner le modèle

```bash
gsutil -m rsync -r gs://logguardian-datalake-logguardian-497218/logs/ ./data/parquet/
cd ml-model
pip install -r requirements.txt
python src/trainer/train.py --parquet-dir ./data/parquet --output-dir ./models
gsutil -m cp ./models/* gs://logguardian-models-logguardian-497218/
kubectl rollout restart deployment/ml-model -n logguardian
```

---

## Commandes GKE essentielles

### Authentification

```bash
gcloud auth login
gcloud config set project logguardian-497218
gcloud container clusters get-credentials logguardian --zone=europe-west1-b --project=logguardian-497218
$env:USE_GKE_GCLOUD_AUTH_PLUGIN = "True"   # Windows PowerShell
```

### Suspendre le cluster (économie de coûts)

```bash
kubectl scale deployment --all --replicas=0 -n logguardian
gcloud container clusters resize logguardian --node-pool=logguardian-nodes-standard --num-nodes=0 --zone=europe-west1-b --project=logguardian-497218 --quiet
```

### Reprendre le cluster

```bash
gcloud container clusters resize logguardian --node-pool=logguardian-nodes-standard --num-nodes=2 --zone=europe-west1-b --project=logguardian-497218 --quiet
kubectl scale deployment --all --replicas=1 -n logguardian
```

### État du cluster

```bash
kubectl get pods -n logguardian
kubectl get nodes
kubectl get svc -n logguardian
kubectl get pvc -n logguardian
```

### Logs des services

```bash
kubectl logs deployment/log-generator -n logguardian --tail=20
kubectl logs deployment/etl-processor -n logguardian --tail=20
kubectl logs deployment/ml-model -n logguardian --tail=20
kubectl logs deployment/monitoring-ui -n logguardian --tail=20
kubectl logs deployment/kafka -n logguardian --tail=20
```

### Redémarrer un service

```bash
kubectl rollout restart deployment/<nom> -n logguardian
# nom = log-generator | etl-processor | ml-model | monitoring-ui | kafka | zookeeper
```

### Nettoyer les pods morts

```bash
kubectl delete pods -n logguardian --field-selector=status.phase=Failed
```

### Appliquer les manifests K8s

```bash
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/log-generator.yaml
kubectl apply -f k8s/etl-processor.yaml
kubectl apply -f k8s/ml-model.yaml
kubectl apply -f k8s/monitoring-ui.yaml
```

---

## CI/CD — Cloud Build

Déclenché automatiquement sur push vers la branche `develop`.

1. Build l'image Docker de chaque service modifié
2. Push sur Artifact Registry avec le tag `$SHORT_SHA`
3. `gcloud container clusters get-credentials` pour s'authentifier à GKE
4. `kubectl set image` pour mettre à jour chaque deployment

### Déployer manuellement une image

```bash
# Lister les tags disponibles
gcloud artifacts docker tags list europe-west1-docker.pkg.dev/logguardian-497218/logguardian/monitoring-ui --project=logguardian-497218

# Déployer un tag précis
kubectl set image deployment/monitoring-ui monitoring-ui=europe-west1-docker.pkg.dev/logguardian-497218/logguardian/monitoring-ui:<TAG> -n logguardian
```

---

## Accès au dashboard

```
URL : http://104.155.46.136
# Vérifier l'IP : kubectl get svc monitoring-ui -n logguardian
```

---

## Observabilité

```
Dashboard Cloud Monitoring : https://console.cloud.google.com/monitoring/dashboards?project=logguardian-497218
Logs                       : https://console.cloud.google.com/logs?project=logguardian-497218
Alertes actives            : DiskPressure nœud > 85% | Pod restarts > 3 en 5 min
```

---

## Variables d'environnement clés

### log-generator
```
KAFKA_BOOTSTRAP_SERVERS : kafka.logguardian.svc.cluster.local:29092
KAFKA_TOPIC             : logs-raw
REPLAY_SPEED            : 50
LOG_SOURCES             : linux,ssh,hadoop,supercomputer,android,spark
DATA_S3_URI             : gs://logguardian-datalake-logguardian-497218/raw-logs/
```

### etl-processor
```
KAFKA_INPUT_TOPIC       : logs-raw
KAFKA_OUTPUT_TOPIC      : logs-processed
KAFKA_ANOMALY_TOPIC     : logs-anomalies
GCS_BUCKET              : logguardian-datalake-logguardian-497218
BATCH_SIZE              : 500
```

### ml-model
```
KAFKA_INPUT_TOPIC       : logs-processed
KAFKA_OUTPUT_TOPIC      : logs-anomalies-ml
GCS_MODELS_BUCKET       : logguardian-models-logguardian-497218
SEQUENCE_LENGTH         : 10
DEVICE                  : cpu
```

### monitoring-ui
```
KAFKA_TOPIC             : logs-anomalies-ml
MAX_ROWS                : 2000
REFRESH_INTERVAL_MS     : 3000
```

---

## Points d'attention

- **Kafka sans PVC** : données et offsets perdus à chaque restart. Les consumers reprennent à `latest`.
- **Threshold** : valeur actuelle `0.5` dans `ml-model/models/threshold.json`. Valeur originale entraînée : `1.307` (p95).
- **Workload Identity** : WARNING checkpoint sur log-generator — fonctionnel, pas bloquant.
- **Nœuds** : non-préemptibles depuis la migration (évite les cascades de crashes).

---

## Correctifs appliqués

### Rotation des sources — log-generator (`log-generator/src/main.py`)

**Problème** : `itertools.chain.from_iterable()` traitait les sources de manière séquentielle — il épuisait entièrement `linux` avant de passer à `ssh`, etc.

**Correctif** : remplacement par un générateur `_roundrobin()` qui alterne une entrée par source à chaque tour (round-robin). Les sources épuisées sont retirées automatiquement.

```python
def _roundrobin(*iterables):
    pending = [iter(it) for it in iterables]
    while pending:
        next_pending = []
        for it in pending:
            try:
                yield next(it)
                next_pending.append(it)
            except StopIteration:
                pass
        pending = next_pending
```

### Déduplification des messages — monitoring-ui (`monitoring-ui/src/app.py`)

**Problème** : le thread Kafka ajoutait chaque message reçu dans le buffer sans vérification — les doublons s'affichaient dans l'interface.

**Correctif** : ajout d'un `set` (`_seen`) qui trace les messages par clé composite `(detected_at[:19], source, host, anomaly_score)`. Un message déjà vu est ignoré. Le set est purgé automatiquement quand il dépasse `2 × MAX_ROWS` pour éviter toute fuite mémoire.
