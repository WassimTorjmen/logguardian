# 02 — Architecture

## Vue globale

```
┌─────────────┐                                                          ┌─────────────────┐
│  GCS bucket │                                                          │  GCS bucket     │
│  raw-logs/  │                                                          │  models/        │
└──────┬──────┘                                                          └────────┬────────┘
       │ gsutil rsync (init container)                                            │ download au boot
       ▼                                                                          ▼
┌─────────────┐  logs-raw   ┌───────────────┐  logs-processed  ┌──────────┐  logs-anomalies-ml
│log-generator├────────────►│ etl-processor ├─────────────────►│ ml-model ├──────┬──────────────┐
└─────────────┘  (Kafka)    └───────┬───────┘     (Kafka)      └──────────┘      │              │
                                    │ Parquet                                    ▼              ▼
                                    ▼                                    ┌──────────────┐ ┌──────────────┐
                            ┌──────────────┐                             │ monitoring-ui│ │ email-sender │
                            │  GCS bucket  │                             │ (Dash + auth)│ │ (SendGrid)   │
                            │  logs/       │                             └──────┬───────┘ └──────────────┘
                            └──────────────┘                                    │
                                                                                ▼ LoadBalancer ext.
                                                                          http://<IP>
```

## Topics Kafka

| Topic | Producteur | Consommateur | Format |
|---|---|---|---|
| `logs-raw` | log-generator | etl-processor | JSON (timestamp, source, host, level, message, raw) |
| `logs-processed` | etl-processor | ml-model | JSON normalisé (+ hour, is_anomaly_rule) |
| `logs-anomalies` | etl-processor | — (debug rule-based) | JSON, logs avec level ERROR/FATAL |
| `logs-anomalies-ml` | ml-model | monitoring-ui + email-sender | JSON (sequence, score, threshold, severity_ratio) |

## Flux détaillé d'un log

1. **log-generator** lit un fichier `.log` Loghub depuis `/data` (PVC, synchronisé depuis GCS).
2. Le parser correspondant (`linux_parser.py`, `ssh_parser.py`…) produit un `LogEntry`.
3. Round-robin entre toutes les sources actives → publication sur `logs-raw`.
4. **etl-processor** consomme `logs-raw`, normalise les niveaux, ajoute un flag `is_anomaly_rule` (level ∈ ERROR/FATAL), batche 500 messages → écriture Parquet sur GCS + publication sur `logs-processed`.
5. **ml-model** consomme `logs-processed`. Pour chaque clé `(source, host)`, il maintient un buffer de 10 logs. Quand le buffer est plein, il calcule la MSE de reconstruction via le LSTM Autoencoder. Si `MSE > threshold`, le batch est publié sur `logs-anomalies-ml` avec `anomaly_score`, `threshold`, `severity_ratio`.
6. **monitoring-ui** consomme `logs-anomalies-ml` dans un thread, accumule dans un buffer en mémoire (déduplication par `(detected_at[:19], source, host, score)`), expose un dashboard Dash protégé par login.
7. **email-sender** consomme `logs-anomalies-ml` en parallèle, accumule les incidents avec `severity_ratio > 1.3` sur une fenêtre de 15 minutes, envoie un email récapitulatif via SendGrid API.

## Choix d'architecture

### Pourquoi Kafka et pas Pub/Sub ?
Pub/Sub serait plus naturel sur GCP, mais Kafka est conservé pour la portabilité multi-cloud et parce que le projet a commencé sur AWS.

### Pourquoi un LoadBalancer externe sur monitoring-ui ?
GKE Standard LoadBalancer expose le dashboard publiquement. L'authentification Flask session protège l'accès. Pour la prod réelle, mettre derrière IAP ou un VPN.

### Pourquoi SendGrid et pas Gmail SMTP ?
GCP bloque les ports SMTP sortants (25, 465, 587) sur les nœuds GKE. SendGrid utilise HTTPS (port 443) et n'est pas bloqué.

### Pourquoi pas de PVC pour Kafka ?
Choix de simplicité — les logs sont synthétiques (Loghub replay) et la perte des offsets au restart est acceptable. Pour la prod, ajouter un PVC `standard-rwo` 10Gi.

### Pourquoi Workload Identity ?
Pour que les pods accèdent à GCS sans clé JSON dans un secret. Les KSA `log-generator`, `etl-processor`, `ml-model` sont liés au GSA `terraform-sa` qui a `roles/storage.objectAdmin` sur les buckets.

## Schéma drawio

Un diagramme éditable est disponible dans [`docs/architecture-gcp.drawio`](architecture-gcp.drawio).
