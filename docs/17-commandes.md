# 17 — Commandes utiles

## Authentification GCP

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project logguardian-497218
gcloud container clusters get-credentials logguardian \
    --zone=europe-west1-b --project=logguardian-497218
$env:USE_GKE_GCLOUD_AUTH_PLUGIN = "True"   # PowerShell
```

## kubectl

### État

```bash
kubectl get pods -n logguardian
kubectl get pods -n logguardian -w               # watch live
kubectl get nodes
kubectl get svc -n logguardian
kubectl get pvc -n logguardian
kubectl get cm -n logguardian
kubectl get secrets -n logguardian
kubectl get events -n logguardian --sort-by='.lastTimestamp' | tail -30
```

### Logs

```bash
kubectl logs deployment/<service> -n logguardian --tail=20
kubectl logs deployment/<service> -n logguardian -f          # follow
kubectl logs pod/<pod-name> -n logguardian --previous        # pod crashé
```

### Exec dans un pod

```bash
kubectl exec -n logguardian deployment/log-generator -- ls /data/
kubectl exec -it -n logguardian deployment/monitoring-ui -- bash
kubectl exec -n logguardian deployment/email-sender -- env | grep KAFKA
```

### Restart

```bash
kubectl rollout restart deployment/<service> -n logguardian
kubectl rollout status deployment/<service> -n logguardian
kubectl rollout history deployment/<service> -n logguardian
kubectl rollout undo deployment/<service> -n logguardian
```

### Scale

```bash
kubectl scale deployment/email-sender -n logguardian --replicas=0   # arrêter
kubectl scale deployment/email-sender -n logguardian --replicas=1   # relancer
kubectl scale deployment --all -n logguardian --replicas=0          # tout arrêter
```

### Modifier en direct

```bash
kubectl set image deployment/ml-model \
    ml-model=europe-west1-docker.pkg.dev/.../ml-model:<TAG> \
    -n logguardian

kubectl set env deployment/email-sender BATCH_INTERVAL_SECONDS=60 -n logguardian
kubectl edit configmap monitoring-ui-config -n logguardian
```

### Nettoyer

```bash
kubectl delete pods -n logguardian --field-selector=status.phase=Failed
kubectl delete pod <pod-name> -n logguardian
```

## Apply manifests

```bash
kubectl apply -f k8s/                  # tous
kubectl apply -f k8s/ml-model.yaml     # un seul
kubectl apply -k k8s/                  # via kustomization
```

## gcloud — cluster

```bash
# Suspendre (coût quasi-nul)
kubectl scale deployment --all --replicas=0 -n logguardian
gcloud container clusters resize logguardian \
    --node-pool=logguardian-nodes-standard \
    --num-nodes=0 \
    --zone=europe-west1-b --quiet

# Réveiller
gcloud container clusters resize logguardian \
    --node-pool=logguardian-nodes-standard \
    --num-nodes=2 \
    --zone=europe-west1-b --quiet
kubectl scale deployment --all --replicas=1 -n logguardian
```

## gsutil — buckets

```bash
# Lister
gsutil ls gs://logguardian-models-logguardian-497218/
gsutil ls -l gs://logguardian-datalake-logguardian-497218/raw-logs/

# Télécharger
gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./
gsutil -m rsync -r gs://logguardian-datalake-logguardian-497218/raw-logs/ ./data/

# Uploader
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json
gsutil -m cp ./models/* gs://logguardian-models-logguardian-497218/
```

## gcloud — Cloud Build

```bash
gcloud builds list --project=logguardian-497218 --limit=5
gcloud builds log <BUILD_ID> --project=logguardian-497218
gcloud builds submit . --config=cloudbuild.yaml --project=logguardian-497218 \
    --substitutions=SHORT_SHA=$(git rev-parse --short HEAD)
```

## gcloud — Artifact Registry

```bash
# Lister les images
gcloud artifacts docker tags list \
    europe-west1-docker.pkg.dev/logguardian-497218/logguardian/monitoring-ui \
    --project=logguardian-497218

# Pull une image
gcloud auth configure-docker europe-west1-docker.pkg.dev
docker pull europe-west1-docker.pkg.dev/logguardian-497218/logguardian/ml-model:latest
```

## Kafka

```bash
# Lister topics
kubectl exec -n logguardian deployment/kafka -- \
    kafka-topics.sh --bootstrap-server localhost:29092 --list

# Consommer
kubectl exec -n logguardian deployment/kafka -- \
    kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic logs-anomalies-ml \
    --from-beginning --max-messages 5 --timeout-ms 5000

# Lag consumer
kubectl exec -n logguardian deployment/kafka -- \
    kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
    --describe --group ml-model
```

## Docker Compose (local)

```bash
docker compose up -d
docker compose down
docker compose down -v --remove-orphans          # avec volumes
docker compose ps
docker compose logs <service> --tail=20
docker compose logs <service> -f                 # follow
docker compose build <service>
docker compose build --no-cache <service>        # rebuild propre
docker compose up -d <service>                   # un service seul
docker compose restart <service>
docker compose exec <service> bash
```

## Secrets K8s

```bash
# Créer depuis .env
kubectl create secret generic monitoring-ui-secrets \
    --from-env-file=.env -n logguardian

# Mettre à jour
kubectl create secret generic monitoring-ui-secrets \
    --from-env-file=.env -n logguardian \
    --dry-run=client -o yaml | kubectl apply -f -

# Lire les clés (pas les valeurs)
kubectl get secret monitoring-ui-secrets -n logguardian -o jsonpath='{.data}' | \
    python -c "import sys,json,base64; d=json.load(sys.stdin); [print(k) for k in d.keys()]"

# Décoder une clé spécifique
kubectl get secret monitoring-ui-secrets -n logguardian -o jsonpath='{.data.GROQ_API_KEY}' | \
    base64 -d
```
