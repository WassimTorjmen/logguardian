# Documentation LogGuardian

Documentation complète de la plateforme **LogGuardian** — détection d'anomalies temps réel sur flux de logs.

## Index

### Pour commencer
- [01 — Vue d'ensemble](01-vue-ensemble.md) — Qu'est-ce que LogGuardian, à quoi ça sert
- [02 — Architecture](02-architecture.md) — Diagramme global, flux de données, choix techniques

### Installation
- [03 — Prérequis](03-prerequis.md) — Outils à installer, comptes, accès
- [04 — Installation locale (Docker Compose)](04-installation-locale.md) — Pour développer / tester
- [05 — Installation GCP (GKE)](05-installation-gcp.md) — Pour déployer en production
- [06 — Configuration & variables](06-configuration.md) — Secrets, ConfigMaps, fichiers `.env`

### Services
- [07 — log-generator](services/log-generator.md) — Producteur de logs
- [08 — etl-processor](services/etl-processor.md) — Normalisation et stockage Parquet
- [09 — ml-model](services/ml-model.md) — Détection LSTM Autoencoder
- [10 — monitoring-ui](services/monitoring-ui.md) — Dashboard Dash + auth + IA Groq
- [11 — email-sender](services/email-sender.md) — Alertes email batchées (SendGrid)
- [12 — Kafka](services/kafka.md) — Bus de messages

### ML / Data
- [13 — Modèle LSTM Autoencoder](13-modele-ml.md) — Architecture, features, entraînement, seuil
- [14 — Réentraînement](14-reentrainement.md) — Procédure de mise à jour du modèle

### Opérations
- [15 — CI/CD](15-cicd.md) — Cloud Build, déploiement automatique
- [16 — Monitoring & alertes](16-observabilite.md) — Cloud Monitoring, logs, alertes GCP
- [17 — Commandes utiles](17-commandes.md) — kubectl, gcloud, gsutil
- [18 — Troubleshooting](18-troubleshooting.md) — Problèmes courants et solutions
- [19 — Coûts & maintenance](19-couts.md) — Arrêt/reprise du cluster, optimisations

### Annexes
- [A — Glossaire](annexes/glossaire.md)
- [B — Historique des décisions](annexes/decisions.md) — Pourquoi SendGrid, pourquoi round-robin, etc.

---

## Démarrage rapide

| Je veux… | Lire |
|---|---|
| Comprendre le projet | [01 — Vue d'ensemble](01-vue-ensemble.md) + [02 — Architecture](02-architecture.md) |
| Faire tourner en local | [03 — Prérequis](03-prerequis.md) → [04 — Installation locale](04-installation-locale.md) |
| Déployer sur GCP | [03 — Prérequis](03-prerequis.md) → [05 — Installation GCP](05-installation-gcp.md) |
| Modifier le modèle ML | [13 — Modèle](13-modele-ml.md) → [14 — Réentraînement](14-reentrainement.md) |
| Debugger un pod qui crash | [18 — Troubleshooting](18-troubleshooting.md) |
