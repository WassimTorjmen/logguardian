# Service — ml-model

Consomme `logs-processed`, score chaque séquence avec un LSTM Autoencoder, publie les anomalies.

## Rôle

1. Au démarrage : télécharger les artefacts depuis GCS
2. Maintenir un buffer glissant par clé `(source, host)`
3. Quand un buffer atteint 10 logs : vectoriser, scaler, scorer (MSE de reconstruction)
4. Si `score > threshold` : publier sur `logs-anomalies-ml`

## Code

```
ml-model/
├── Dockerfile
├── requirements.txt
└── src/
    ├── main.py
    ├── config.py
    ├── consumer.py
    ├── producer.py
    ├── inference/
    │   ├── detector.py     # AnomalyDetector + download GCS
    │   └── buffer.py       # Buffer par (source, host)
    └── trainer/
        ├── train.py
        ├── dataset.py
        ├── features.py     # log_to_vector → 77 dims
        └── model.py        # LSTMAutoencoder PyTorch
```

## Détecteur

`detector.py` charge 5 artefacts :

| Fichier | Contenu |
|---|---|
| `lstm_autoencoder.pt` | Poids du modèle PyTorch |
| `vocabulary.pkl` | Mapping token → index (5000 tokens) |
| `embedding_table.npy` | Table (5001, 64) — embeddings des tokens |
| `feature_scaler.pkl` | StandardScaler entraîné sur les features |
| `threshold.json` | Seuil MSE + métadonnées d'entraînement |

## Buffer

`buffer.py` maintient une `dict[str, deque]` indexée par `(source, host)`. Chaque entrée est une `deque(maxlen=10)`. Quand `len == 10`, la séquence est scorée.

## Format de sortie (topic `logs-anomalies-ml`)

```json
{
  "detected_at": "2026-06-17T14:23:01",
  "source": "linux",
  "host": "combo",
  "anomaly_score": 1.24,
  "threshold": 0.76,
  "severity_ratio": 1.62,
  "model_version": "lstm_v1_20260614",
  "sequence": [
    {"timestamp": "...", "message": "..."},
    ...
  ]
}
```

`severity_ratio = anomaly_score / threshold`. Plus c'est grand, plus l'anomalie est marquée.

## En GKE

- ServiceAccount avec Workload Identity (read sur `gs://*-models-*`)
- ConfigMap : `KAFKA_*`, `GCS_MODELS_BUCKET`, `SEQUENCE_LENGTH`
- Resources : 250m CPU / 512Mi RAM (pure CPU torch)

## Logs typiques

```
INFO detector: Modèle chargé depuis /app/models | seuil=0.763474
INFO producer: Anomalie publiée | host=combo | score=1.2223 | ratio=1.60x
```

## Voir aussi

- [13 — Modèle LSTM Autoencoder](../13-modele-ml.md) — architecture détaillée
- [14 — Réentraînement](../14-reentrainement.md)
