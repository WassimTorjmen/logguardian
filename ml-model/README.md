# LogGuardian — ML Model

Détection d'anomalies dans les logs par LSTM Autoencoder.

Ce service consomme les logs depuis Kafka, les score en temps réel, et publie les anomalies détectées.

---

## Principe

Le modèle est entraîné **uniquement sur des logs normaux**. Quand il reçoit une séquence anormale qu'il n'a jamais vue, il n'arrive pas à la reconstruire correctement. C'est cette erreur de reconstruction (MSE) qui déclenche l'alerte.

```
séquence de 10 logs → LSTM Encoder → vecteur 32 dims → LSTM Decoder → reconstruction
                                                                             ↓
                                                              MSE > seuil → anomalie
```

---

## Installation

```bash
cd ml-model
uv sync
```

---

## Entraînement

Le modèle s'entraîne sur les fichiers Parquet produits par l'ETL.

```bash
# Lancer l'ETL d'abord pour générer les données
docker compose up -d etl-processor

# Entraîner
uv run python -m trainer.train --input ../data/etl-output --output models/
```

Les artefacts sont sauvegardés dans `models/` :

| Fichier | Contenu |
|---|---|
| `lstm_autoencoder.pt` | Poids du modèle |
| `vocabulary.pkl` | Vocabulaire des 5000 tokens |
| `embedding_table.npy` | Table d'embeddings des messages |
| `feature_scaler.pkl` | Normalisation des features |
| `threshold.json` | Seuil + métadonnées d'entraînement |

---

## Inférence (mode local)

```bash
# Variables requises
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOCAL_MODEL_MODE=true
export MODEL_DIR=models/

uv run python src/main.py
```

---

## Via Docker Compose

```bash
docker compose up -d ml-model
docker logs logguardian-ml-model-1 --tail 30
```

---

## Architecture du code

```
src/
├── main.py              point d'entrée — boucle Kafka
├── config.py            variables d'environnement
├── consumer.py          consommateur Kafka
├── producer.py          producteur Kafka (publication des anomalies)
├── trainer/
│   ├── train.py         script d'entraînement
│   ├── model.py         architecture LSTM Autoencoder
│   ├── features.py      vectorisation des logs (77 dims)
│   └── dataset.py       chargement Parquet + séquences glissantes
└── inference/
    ├── detector.py      chargement modèle + scoring
    └── buffer.py        fenêtre glissante par (source, host)
```

---

## Vectorisation (77 dimensions)

Chaque log est transformé en vecteur avant d'être passé au modèle :

| Composante | Taille | Description |
|---|---|---|
| source | 6 | one-hot (linux, ssh, hadoop, spark, supercomputer, hdfs) |
| level | 5 | one-hot (DEBUG, INFO, WARN, ERROR, FATAL) |
| heure | 2 | sin/cos pour capturer la cyclicité 23h→0h |
| message | 64 | moyenne des embeddings des tokens |
| **Total** | **77** | |

---

## Évaluation

Ouvrir le notebook pour visualiser les résultats :

```bash
uv run jupyter notebook notebook_evaluation.ipynb
```

Ou générer le PDF directement :

```bash
uv run jupyter nbconvert --to pdf --execute notebook_evaluation.ipynb
```

---

## Topics Kafka

| Topic | Rôle |
|---|---|
| `logs-processed` | entrée — logs normalisés par l'ETL |
| `logs-anomalies-ml` | sortie — anomalies détectées avec score |

---

## Résultats d'entraînement

| Métrique | Valeur |
|---|---|
| Train loss | 0.2696 |
| Val loss | 0.2641 |
| Seuil (p95) | 1.3071 |
| Séquences d'entraînement | ~31 000 |
