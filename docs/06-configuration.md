# 06 — Configuration & variables

## Hiérarchie des configurations

```
ConfigMap K8s ──┐
                ├──► env du pod ──► os.getenv(...) dans le code Python
Secret K8s ─────┘
```

En local Docker Compose : les variables viennent de `.env` + de la section `environment:` de `docker-compose.yml`.

En GKE : viennent des `ConfigMap` (config publique) + `Secret` (secrets).

## Variables par service

### log-generator

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Adresse du broker |
| `KAFKA_TOPIC` | `logs-raw` | Topic de sortie |
| `REPLAY_SPEED` | `1.0` | Multiplicateur de vitesse (50 = 50× temps réel, 0 = max) |
| `DATA_DIR` | `/data` | Dossier des fichiers .log |
| `LOG_SOURCES` | `linux,ssh,hadoop,spark,supercomputer,hdfs` | Sources actives, séparées par virgules |
| `S3_BUCKET` | — | Pour checkpoint S3 (legacy, optionnel) |

### etl-processor

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker |
| `KAFKA_INPUT_TOPIC` | `logs-raw` | Topic source |
| `KAFKA_OUTPUT_TOPIC` | `logs-processed` | Topic des logs normalisés |
| `KAFKA_ANOMALY_TOPIC` | `logs-anomalies` | Topic rule-based (ERROR/FATAL) |
| `KAFKA_GROUP_ID` | `etl-processor` | Group ID Kafka |
| `BATCH_SIZE` | `500` | Nombre de logs par batch Parquet |
| `BATCH_TIMEOUT_SEC` | `30` | Timeout flush si batch incomplet |
| `LOCAL_MODE` | `false` | Si `true` : écrit en local au lieu de GCS |
| `LOCAL_OUTPUT_DIR` | `/tmp/etl-output` | Dossier local en mode local |
| `GCS_BUCKET` | — | Bucket GCS pour les Parquet |
| `GCS_PREFIX` | `logs/` | Préfixe des objets GCS |

### ml-model

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker |
| `KAFKA_INPUT_TOPIC` | `logs-processed` | Source |
| `KAFKA_OUTPUT_TOPIC` | `logs-anomalies-ml` | Anomalies détectées |
| `KAFKA_GROUP_ID` | `ml-model` | Group ID |
| `MODEL_DIR` | `/app/models` | Où sont stockés les artefacts |
| `LOCAL_MODEL_MODE` | `false` | Si `true` : utilise `MODEL_DIR` directement, sinon télécharge depuis GCS |
| `GCS_MODELS_BUCKET` | — | Bucket source des artefacts |
| `SEQUENCE_LENGTH` | `10` | Taille du buffer glissant |
| `DEVICE` | `cpu` | `cpu` ou `cuda` |

### monitoring-ui

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker |
| `KAFKA_TOPIC` | `logs-anomalies-ml` | Source |
| `MAX_ROWS` | `2000` | Logs en mémoire |
| `REFRESH_INTERVAL_MS` | `3000` | Rafraîchissement UI |
| `ALERT_THRESHOLD` | `1.3` | severity_ratio mini pour compter une alerte |
| `FEEDBACK_PATH` | `/app/feedback/rag_feedback.jsonl` | Persistance feedback |
| `GROQ_API_KEY` | — | Clé Groq (RAG) |
| `GROQ_MODEL` | `openai/gpt-oss-20b` | Modèle Groq |
| `GROQ_MAX_COMPLETION_TOKENS` | `550` | Tokens max par réponse |
| `GROQ_RETRY_COMPLETION_TOKENS` | `850` | Tokens max après feedback négatif |
| `GROQ_FALLBACK_MODEL` | (vide) | Modèle de secours |
| `RAG_NEGATIVE_LIMIT` | `20` | Feedbacks négatifs conservés |
| `LOGIN_USERNAME` | `admin` | Identifiant UI |
| `LOGIN_PASSWORD` | `admin` | Mot de passe UI |
| `DASH_SECRET_KEY` | `logguardian-command-center` | Secret Flask session |

### email-sender

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Broker |
| `KAFKA_TOPIC` | `logs-anomalies-ml` | Source |
| `ALERT_THRESHOLD` | `1.3` | severity_ratio mini pour notifier |
| `BATCH_INTERVAL_SECONDS` | `900` | Fenêtre de batching (15 min) |
| `SENDGRID_API_KEY` | — | Clé SendGrid (HTTPS) |
| `SMTP_USER` | — | Email expéditeur (vérifié SendGrid) |
| `MAIL_TO` | — | Destinataire |
| `SMTP_HOST` | `smtp.gmail.com` | Legacy SMTP (non utilisé, bloqué par GCP) |
| `SMTP_PORT` | `587` | Legacy SMTP (non utilisé) |
| `SMTP_PASSWORD` | — | Legacy SMTP (non utilisé) |

## Secret Kubernetes `monitoring-ui-secrets`

Contient les valeurs sensibles partagées entre `monitoring-ui` et `email-sender` :

```bash
# Créer depuis .env
kubectl create secret generic monitoring-ui-secrets \
    --from-env-file=.env \
    -n logguardian

# Mettre à jour (apply en remplaçant)
kubectl create secret generic monitoring-ui-secrets \
    --from-env-file=.env \
    -n logguardian \
    --dry-run=client -o yaml | kubectl apply -f -

# Vérifier les clés présentes (sans afficher les valeurs)
kubectl get secret monitoring-ui-secrets -n logguardian -o jsonpath='{.data}' | \
    python -c "import sys,json,base64; d=json.load(sys.stdin); [print(k) for k in d.keys()]"
```

Clés attendues dans `.env` :
```
SMTP_USER
SMTP_PASSWORD (legacy, peut être vide)
MAIL_TO
GROQ_API_KEY
GROQ_MODEL
RAG_NEGATIVE_LIMIT
GROQ_MAX_COMPLETION_TOKENS
GROQ_RETRY_COMPLETION_TOKENS
GROQ_FALLBACK_MODEL
SENDGRID_API_KEY
LOGIN_USERNAME
LOGIN_PASSWORD
DASH_SECRET_KEY
```

## ConfigMaps publiques

Chaque service a sa ConfigMap dans son manifest K8s (`k8s/<service>.yaml`). Pour modifier sans redéploiement complet :

```bash
kubectl edit configmap monitoring-ui-config -n logguardian
kubectl rollout restart deployment/monitoring-ui -n logguardian
```

Ou pour changer juste une variable env d'un déploiement (ne modifie pas la ConfigMap) :

```bash
kubectl set env deployment/email-sender BATCH_INTERVAL_SECONDS=60 -n logguardian
```

## Rotation des secrets

**Bonnes pratiques** :
- Régénérer toute clé qui apparaît dans un log/screenshot.
- Si `.env` est committé par erreur → révoquer SendGrid, Groq, regen `DASH_SECRET_KEY`.
- SendGrid : tableau de bord → API Keys → Delete → Create new.
- Groq : console → Delete + regenerate.
- Après rotation : mettre à jour `.env` puis re-créer le secret K8s + restart les déploiements.
