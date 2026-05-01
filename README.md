# LogGuardian

Plateforme de détection d'anomalies dans les logs système par intelligence artificielle.

## Ce que ça fait

LogGuardian lit des logs en temps réel, les analyse avec un modèle LSTM, et détecte les comportements anormaux automatiquement.

```
logs bruts → Kafka → ETL → Kafka → LSTM → alertes
```

## Lancer le projet

```bash
docker compose up -d
```

Ouvrir l'interface Kafka sur http://localhost:8080 pour voir les messages en temps réel.

## Les services

| Service | Rôle |
|---|---|
| log-generator | Rejoue des vrais fichiers de logs dans Kafka |
| etl-processor | Nettoie les logs, détecte les erreurs évidentes (ERROR/FATAL) |
| ml-model | Détecte les anomalies sémantiques avec le modèle LSTM |
| kafka | Bus de communication entre tous les services |

## Le modèle LSTM

Le modèle est entraîné uniquement sur des logs normaux. Quand il voit une séquence anormale, il n'arrive pas à la reconstruire correctement — c'est ce signal d'erreur qui déclenche l'alerte.

- **Entrée** : fenêtre glissante de 10 logs
- **Seuil** : 1.307 (percentile 95 sur les données de validation)
- **Résultat** : score MSE > seuil → anomalie publiée dans `logs-anomalies-ml`

## Déploiement AWS

Les manifests Kubernetes sont dans `k8s/`. Le cluster EKS `logguardian` tourne sur `eu-west-1`.

```bash
kubectl apply -f k8s/
```

## Structure

```
log-generator/   générateur de logs
etl-processor/   traitement ETL
ml-model/        modèle LSTM (entraînement + inférence)
k8s/             manifests Kubernetes
data/            datasets LogHub
```
