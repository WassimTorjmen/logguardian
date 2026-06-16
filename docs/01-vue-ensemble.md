# 01 — Vue d'ensemble

## Qu'est-ce que LogGuardian ?

LogGuardian est une **plateforme AIOps** (Artificial Intelligence for IT Operations) qui détecte automatiquement les anomalies dans des flux de logs en temps réel.

Elle ingère des logs hétérogènes (Linux, SSH, Hadoop, Spark, HDFS, Supercomputer), les normalise, et utilise un **LSTM Autoencoder** pour identifier les séquences inhabituelles. Les anomalies sont affichées dans un dashboard interactif et envoyées par email à l'équipe d'astreinte.

## Objectifs

| Objectif | Comment |
|---|---|
| Détecter des incidents avant qu'ils ne deviennent critiques | Détection non-supervisée par reconstruction LSTM |
| Réduire le bruit pour les SRE | Filtrage par score, batching d'emails sur 15 minutes |
| Expliquer les anomalies en langage naturel | Panneau RAG dans l'UI utilisant Groq (LLM) |
| Tracer le retour utilisateur | Boucle de feedback positif/négatif persistée en JSONL |

## Cas d'usage type

1. Un sysadmin laisse LogGuardian tourner sur ses flux de logs (Linux + SSH + Hadoop).
2. Le pipeline ingère plusieurs milliers de lignes par seconde.
3. Une séquence inhabituelle apparaît (ex: pic d'erreurs SSH suivi de redémarrages).
4. Le ml-model lui attribue un score MSE supérieur au seuil.
5. L'anomalie apparaît dans le dashboard avec son score, son contexte, et un bouton "Analyser avec l'IA".
6. Toutes les 15 minutes, un email batché récapitule les incidents détectés.

## Stack technique synthétique

| Couche | Tech |
|---|---|
| **Sources** | Loghub Dataset (linux, ssh, hadoop, spark, supercomputer, hdfs) |
| **Messagerie** | Apache Kafka 7.5 (Confluent) |
| **ETL** | Python + pyarrow → Parquet |
| **Stockage froid** | Google Cloud Storage (GCS) |
| **Modèle ML** | PyTorch — LSTM Autoencoder bidirectionnel + attention |
| **NLP** | Tokenisation regex + mean embedding 64 dims |
| **Dashboard** | Dash + Plotly + Flask sessions |
| **IA explicative** | Groq API (openai/gpt-oss-20b) |
| **Email** | SendGrid API (HTTP) |
| **Infra Cloud** | GCP — GKE, GCS, Artifact Registry, Cloud Build, Cloud Monitoring |
| **IaC** | Terraform |
| **CI/CD** | Cloud Build sur push `develop` |

## Pourquoi LSTM Autoencoder ?

- **Non-supervisé** : pas besoin de labels d'anomalies (rares et coûteux à produire).
- **Séquentiel** : capture le contexte temporel — une erreur isolée n'est pas anormale, mais une rafale l'est.
- **Reconstruction** : le modèle apprend à reproduire les séquences "normales". Une séquence rare → grosse erreur de reconstruction → anomalie.

## Limites connues

- Le seuil de détection est **fixe** (`threshold.json`) — pas d'adaptation dynamique au volume.
- Les sources Android ont été ajoutées récemment mais ne font **pas partie** des features du modèle (réentraînement nécessaire pour les exploiter).
- Kafka ne persiste pas (pas de PVC) — les offsets repartent à `latest` à chaque restart.
- Trial SendGrid : limité à 100 emails/jour.
