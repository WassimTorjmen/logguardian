# 15 — CI/CD avec Cloud Build

## Trigger

- Push vers la branche `develop` du repo GitHub
- Cloud Build se déclenche automatiquement (trigger configuré dans la console)

## Pipeline (`cloudbuild.yaml`)

### Étape 1 — Build & Push

Pour chaque service avec un `Dockerfile` :

```bash
registry=europe-west1-docker.pkg.dev/$PROJECT_ID/logguardian
for service in log-generator etl-processor ml-model monitoring-ui email-sender; do
    if [ -f "$service/Dockerfile" ]; then
        docker pull $registry/$service:latest || true
        docker build \
            --cache-from $registry/$service:latest \
            -t $registry/$service:$SHORT_SHA \
            -t $registry/$service:latest \
            ./$service
        docker push $registry/$service:$SHORT_SHA
        docker push $registry/$service:latest
    fi
done
```

**Caching** : `docker pull :latest || true` + `--cache-from` réutilise les couches déjà buildées → build 5-10× plus rapide.

### Étape 2 — Déploiement GKE

```bash
gcloud container clusters get-credentials logguardian \
    --zone=europe-west1-b --project=$PROJECT_ID

for service in log-generator etl-processor ml-model monitoring-ui email-sender; do
    kubectl set image deployment/$service \
        $service=$registry/$service:$SHORT_SHA \
        -n logguardian || echo "Skipping $service"
done
```

## Variables Cloud Build automatiques

- `$PROJECT_ID` : ID GCP du projet
- `$SHORT_SHA` : 7 premiers chars du commit SHA — tag des images

## Optimisations passées

- Avant : ~10 min par build (CUDA, pas de cache)
- Maintenant : ~3-5 min grâce à :
  - `torch==2.2.2+cpu` au lieu de CUDA (économise ~2 GB de download)
  - `kafka-python==2.0.2` pinné (évitait des erreurs ImportError)
  - `numpy<2` (compat torch)
  - Docker layer caching via `--cache-from`

## Suivre un build

```bash
gcloud builds list --project=logguardian-497218 --limit=3
gcloud builds log <BUILD_ID> --project=logguardian-497218
```

Ou dans la console : https://console.cloud.google.com/cloud-build/builds?project=logguardian-497218

## Déclencher manuellement

```bash
gcloud builds submit . \
    --config=cloudbuild.yaml \
    --project=logguardian-497218 \
    --substitutions=SHORT_SHA=$(git rev-parse --short HEAD)
```

## Permissions du Service Account Cloud Build

Le compte `<PROJECT_NUMBER>@cloudbuild.gserviceaccount.com` doit avoir :
- `roles/container.developer` — pour `kubectl set image`
- `roles/artifactregistry.writer` — pour push les images
- `roles/storage.objectViewer` — pour pull caches

```bash
PROJECT_NUMBER=$(gcloud projects describe logguardian-497218 --format='value(projectNumber)')
gcloud projects add-iam-policy-binding logguardian-497218 \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/container.developer"
```

## Workflow recommandé

1. Travailler sur une branche feature (`features/xyz`)
2. PR vers `develop` → review → merge
3. Cloud Build → déploie auto sur GKE
4. Valider sur le cluster (http://<IP>)
5. PR `develop` → `main` pour les releases tagguées

## Branch protection

- `main` : require PR + 1 approval
- `develop` : intégration continue, push direct possible

## Rollback rapide

```bash
# Voir l'historique
kubectl rollout history deployment/ml-model -n logguardian

# Revenir à la révision précédente
kubectl rollout undo deployment/ml-model -n logguardian

# Revenir à une révision spécifique
kubectl rollout undo deployment/ml-model --to-revision=42 -n logguardian
```
