# 04 — Installation locale (Docker Compose)

Utile pour : développer, tester un changement de code, démo offline.

## 1. Préparer les données de logs

Le `log-generator` lit des fichiers `.log` depuis `./data/`. Deux options :

### Option A — Télécharger depuis GCS (recommandé)

```bash
mkdir -p ./data
gsutil -m rsync -r gs://logguardian-datalake-logguardian-497218/raw-logs/ ./data/
```

### Option B — Télécharger Loghub manuellement

Aller sur [Loghub](https://github.com/logpai/loghub) et télécharger les datasets : Linux, SSH, Hadoop, Spark, Supercomputer (Thunderbird), HDFS. Décompresser dans `./data/` avec la structure :

```
data/
├── Linux.log
├── SSH.log
├── hadoop/...
├── spark/...
├── supercomputer/...
└── hdfs/...
```

## 2. Configurer le fichier `.env`

Copier le template et le compléter :

```bash
cp .env.example .env    # si .env.example existe, sinon créer manuellement
```

Contenu minimum :

```bash
# === Authentification UI ===
LOGIN_USERNAME=admin
LOGIN_PASSWORD=admin
DASH_SECRET_KEY=changeme-with-a-long-random-string

# === Email (optionnel en local) ===
SMTP_USER=ton.email@gmail.com
MAIL_TO=destinataire@example.com
SENDGRID_API_KEY=SG.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# === IA explicative (optionnel) ===
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
GROQ_MODEL=openai/gpt-oss-20b
GROQ_MAX_COMPLETION_TOKENS=550
RAG_NEGATIVE_LIMIT=20

# === Fallback SMTP (non utilisé en local si SendGrid OK) ===
SMTP_PASSWORD=
```

> **Sécurité** : `.env` est dans `.gitignore` — ne jamais le commit.

## 3. Lancer le stack

```powershell
# Build initial (5-10 min la première fois)
docker compose build

# Démarrer tous les services
docker compose up -d

# Vérifier l'état
docker compose ps
```

Services exposés :

| Service | URL | Usage |
|---|---|---|
| monitoring-ui | http://localhost:8050 | Dashboard (login admin/admin) |
| kafka-ui | http://localhost:8080 | Inspection des topics Kafka |
| kafka (broker) | localhost:9092 | Connexion depuis l'hôte |

## 4. Vérifier que ça marche

```powershell
# Logs de chaque service
docker compose logs log-generator --tail=20
docker compose logs etl-processor --tail=20
docker compose logs ml-model --tail=20
docker compose logs monitoring-ui --tail=20
docker compose logs email-sender --tail=20
```

Sur le dashboard http://localhost:8050 :
- Se logger avec `admin` / `admin`.
- L'onglet "Flux logs" doit montrer des messages qui défilent.
- L'onglet "Incident board" doit s'incrémenter quand des anomalies sont détectées.

## 5. Reconstruire un service après modification

```powershell
# Si tu modifies le code Python d'un service
docker compose build monitoring-ui
docker compose up -d monitoring-ui

# Forcer un rebuild complet (cache cleared)
docker compose build --no-cache ml-model
docker compose up -d ml-model
```

## 6. Arrêter et nettoyer

```powershell
# Arrêter (garde les volumes)
docker compose down

# Tout supprimer (volumes inclus)
docker compose down -v --remove-orphans
```

## Modèle ML en local

Le service `ml-model` cherche les artefacts dans `./ml-model/models/` :

```
ml-model/models/
├── lstm_autoencoder.pt
├── vocabulary.pkl
├── embedding_table.npy
├── feature_scaler.pkl
└── threshold.json
```

Pour les obtenir :

```bash
gsutil -m cp gs://logguardian-models-logguardian-497218/* ./ml-model/models/
```

## Limites du mode local

- Pas de Workload Identity → l'etl-processor ne peut pas écrire sur GCS (mode `LOCAL_MODE=true`, écriture dans `/tmp/etl-output`).
- Kafka mono-broker, pas de réplication.
- Pas de LoadBalancer — accès uniquement via `localhost`.
