# Service — etl-processor

Consomme `logs-raw`, normalise, batche en Parquet sur GCS, republie sur `logs-processed`.

## Rôle

1. Lire les logs bruts depuis Kafka
2. Normaliser les champs (niveau, timestamp, source)
3. Ajouter des features dérivées (`hour`, `is_anomaly_rule`)
4. Écrire des batches Parquet sur GCS pour l'archivage / réentraînement
5. Republier les logs enrichis sur `logs-processed`

## Code

```
etl-processor/
├── Dockerfile
├── requirements.txt
└── src/
    ├── main.py             # Boucle consume/transform/batch
    ├── transformer.py      # Normalisation + flag rule-based
    ├── gcs_loader.py       # Upload Parquet → GCS
    └── config.py
```

## Transformation

`transformer.py` :
- Normalise les niveaux (`E` → `ERROR`, etc.)
- Extrait `hour` depuis le timestamp pour la feature cyclique
- Flag `is_anomaly_rule = level in {ERROR, FATAL}` pour le rule-based simple (topic `logs-anomalies`)

## Batching Parquet

Quand le buffer atteint `BATCH_SIZE` (500) ou que `BATCH_TIMEOUT_SEC` (30s) est dépassé :
- Conversion en `pyarrow.Table`
- Upload `gs://<BUCKET>/logs/<date>/<uuid>.parquet`
- Reset du buffer

## En GKE

- ServiceAccount `etl-processor` + Workload Identity pour accès GCS
- ConfigMap : `KAFKA_*`, `GCS_BUCKET`, `BATCH_SIZE`
- Resources : 100m CPU / 256Mi RAM minimum

## Format de sortie (topic `logs-processed`)

```json
{
  "timestamp": "2026-06-17T14:23:01",
  "source": "linux",
  "host": "combo",
  "level": "INFO",
  "component": "sshd",
  "message": "Accepted password for user root...",
  "hour": 14,
  "is_anomaly_rule": false
}
```

## Mode local

Avec `LOCAL_MODE=true`, écrit dans `LOCAL_OUTPUT_DIR` au lieu de GCS. Utile pour tests Docker Compose.
