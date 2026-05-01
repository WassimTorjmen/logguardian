# LogGuardian

Détection d'anomalies dans les logs système par intelligence artificielle.

## Pipeline

```
log-generator → logs-raw → etl-processor → logs-processed → ml-model → logs-anomalies-ml
```

Chaque étape communique via Kafka. Le monitoring UI consomme `logs-anomalies-ml` et affiche les alertes en temps réel.

## Lancer

```bash
docker compose up -d
```

| Interface | URL |
|---|---|
| Monitoring UI | http://localhost:8050 |
| Kafka UI | http://localhost:8080 |

## Services

| Service | Rôle |
|---|---|
| `log-generator` | Rejoue de vrais fichiers de logs dans Kafka |
| `etl-processor` | Nettoie et vectorise les logs, détecte les anomalies évidentes (ERROR/FATAL) |
| `ml-model` | Détecte les anomalies sémantiques avec un LSTM Autoencoder |
| `monitoring-ui` | Dashboard temps réel des anomalies détectées |

## Modèle LSTM

Entraîné uniquement sur des logs normaux. Une séquence est anormale quand le modèle n'arrive pas à la reconstruire — l'erreur de reconstruction (MSE) devient le score d'anomalie.

- Entrée : fenêtre glissante de 10 logs (vecteur 77 dims)
- Seuil : 1.307 (percentile 95 sur validation)
- Score > seuil → anomalie publiée dans `logs-anomalies-ml`

## Structure

```
log-generator/    générateur de logs
etl-processor/    traitement ETL
ml-model/         modèle LSTM (entraînement + inférence)
monitoring-ui/    dashboard Dash
data/             datasets LogHub
```
