# 05 — Installation GCP (GKE)

## Cas 1 — Le cluster existe déjà (cas le plus courant)

### 1. Se connecter au cluster existant

```bash
gcloud auth login
gcloud config set project logguardian-497218
gcloud container clusters get-credentials logguardian \
    --zone=europe-west1-b \
    --project=logguardian-497218

# Windows
$env:USE_GKE_GCLOUD_AUTH_PLUGIN = "True"
```

### 2. Vérifier l'accès

```bash
kubectl get nodes
kubectl get pods -n logguardian
```

### 3. Si les pods sont scale 0 (cluster en veille)

```bash
# Réveiller le pool de nœuds
gcloud container clusters resize logguardian \
    --node-pool=logguardian-nodes-standard \
    --num-nodes=2 \
    --zone=europe-west1-b --project=logguardian-497218 --quiet

# Relancer les déploiements
kubectl scale deployment --all --replicas=1 -n logguardian
```

### 4. Récupérer l'IP du dashboard

```bash
kubectl get svc monitoring-ui -n logguardian
# Chercher EXTERNAL-IP
```

---

## Cas 2 — Setup complet from scratch

Pour reproduire l'infrastructure dans un nouveau projet GCP.

### 1. Créer le projet GCP

```bash
gcloud projects create mon-logguardian --name="LogGuardian"
gcloud config set project mon-logguardian

# Lier une carte de crédit (Billing Account)
gcloud beta billing projects link mon-logguardian \
    --billing-account=XXXXXX-XXXXXX-XXXXXX
```

### 2. Activer les APIs

```bash
gcloud services enable \
    container.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    storage.googleapis.com \
    monitoring.googleapis.com \
    logging.googleapis.com \
    iam.googleapis.com
```

### 3. Provisionner avec Terraform

```bash
cd terraform/gcp

# Adapter variables.tf avec ton project_id
# Créer le bucket de state si nécessaire
gsutil mb -l europe-west1 gs://logguardian-terraform-state-<PROJECT_ID>

# Créer la clé JSON du compte Terraform
gcloud iam service-accounts create terraform-sa
gcloud projects add-iam-policy-binding mon-logguardian \
    --member="serviceAccount:terraform-sa@mon-logguardian.iam.gserviceaccount.com" \
    --role="roles/owner"
gcloud iam service-accounts keys create ~/.gcp/logguardian-terraform-key.json \
    --iam-account=terraform-sa@mon-logguardian.iam.gserviceaccount.com

terraform init
terraform plan
terraform apply
```

Ce que crée Terraform :
- VPC + subnet
- Cluster GKE Standard (autopilot=false), node pool `e2-standard-2` x 2, autoscaling 1-3
- 2 buckets GCS : `<PREFIX>-datalake-<PROJECT_ID>` et `<PREFIX>-models-<PROJECT_ID>`
- Artifact Registry : `<PROJECT_ID>/logguardian`
- Service account `terraform-sa` avec Workload Identity

### 4. Build et push initial des images

```bash
gcloud auth configure-docker europe-west1-docker.pkg.dev

for service in log-generator etl-processor ml-model monitoring-ui email-sender; do
    docker build -t europe-west1-docker.pkg.dev/mon-logguardian/logguardian/$service:latest ./$service
    docker push europe-west1-docker.pkg.dev/mon-logguardian/logguardian/$service:latest
done
```

### 5. Uploader les données et le modèle

```bash
# Données brutes
gsutil -m cp -r ./data/* gs://logguardian-datalake-<PROJECT_ID>/raw-logs/

# Modèle entraîné
gsutil -m cp ./ml-model/models/* gs://logguardian-models-<PROJECT_ID>/
```

### 6. Créer le namespace et les secrets

```bash
kubectl create namespace logguardian

# Créer le secret monitoring-ui-secrets depuis .env
kubectl create secret generic monitoring-ui-secrets \
    --from-env-file=.env \
    -n logguardian
```

### 7. Déployer les manifests K8s

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/log-generator.yaml
kubectl apply -f k8s/etl-processor.yaml
kubectl apply -f k8s/ml-model.yaml
kubectl apply -f k8s/monitoring-ui.yaml
kubectl apply -f k8s/email-sender.yaml

# Alternative tout-en-un
kubectl apply -k k8s/    # kustomization.yaml
```

### 8. Configurer Cloud Build

Dans la console GCP → Cloud Build → Triggers → Create :
- Source : ton repo GitHub
- Branche : `^develop$`
- Configuration : Cloud Build config file (`cloudbuild.yaml`)
- Substitutions : aucune

Donner les permissions au compte Cloud Build :

```bash
PROJECT_NUMBER=$(gcloud projects describe mon-logguardian --format='value(projectNumber)')
gcloud projects add-iam-policy-binding mon-logguardian \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/container.developer"
```

### 9. Vérifier le déploiement

```bash
kubectl get pods -n logguardian -w
kubectl get svc monitoring-ui -n logguardian

# Une fois EXTERNAL-IP attribuée
curl http://<EXTERNAL-IP>
```

## Mettre à jour un déploiement

### Via Cloud Build (recommandé)

```bash
git push origin develop
# → Cloud Build se déclenche automatiquement
# → build → push Artifact Registry → kubectl set image
```

### Manuellement

```bash
kubectl set image deployment/ml-model \
    ml-model=europe-west1-docker.pkg.dev/logguardian-497218/logguardian/ml-model:<TAG> \
    -n logguardian

kubectl rollout restart deployment/ml-model -n logguardian
```

## Coûts approximatifs

| Ressource | Coût mensuel estimé |
|---|---|
| Cluster GKE management | gratuit (1 cluster zonal) |
| 2 × e2-standard-2 (non-préemptible, 24/7) | ~50 €/mois |
| GCS (10 Go) | ~0,20 €/mois |
| Artifact Registry (5 Go) | ~0,50 €/mois |
| LoadBalancer | ~18 €/mois |
| **Total** | **~70 €/mois** |

→ Voir [19 — Coûts](19-couts.md) pour réduire la facture.
