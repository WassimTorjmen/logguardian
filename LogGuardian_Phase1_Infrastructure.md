# LogGuardian — Phase 1 : Infrastructure Cloud AWS

## Contexte du projet

LogGuardian est une plateforme AIOps de détection d'anomalies et d'auto-remédiation distribuée. Le projet vise à automatiser la surveillance d'infrastructure via un pipeline en 4 étapes :

1. **Ingestion** de logs massifs via Kafka
2. **Traitement ETL** via Spark Streaming
3. **Détection d'anomalies** par Deep Learning (Autoencoder LSTM)
4. **Auto-remédiation** via Kubernetes (kill/restart pods)

Le projet est développé par une équipe de 3 personnes dans le cadre d'une formation ESGI. Le compte AWS est un compte éducatif avec budget limité.

---

## Région et billing

- **Région AWS** : `eu-west-1` (Ireland)
- **Budget mensuel configuré** : 50$ avec alertes à 50% et 80%
- **Cost Explorer** : activé
- **Coût estimé Phase 1** : ~170$/mois (cluster EKS ~73$, 2x t3.medium ~60$, NAT Gateway ~32$, divers ~5-10$)

---

## Architecture réseau (VPC)

### VPC
- **CIDR** : `10.0.0.0/16`
- **Nom** : `logguardian-vpc`
- **DNS hostnames** : activé
- **DNS resolution** : activé

### Subnets
- **2 subnets publics** (2 AZs) — hébergent l'ALB/Ingress et le NAT Gateway
- **2 subnets privés** (2 AZs) — hébergent les worker nodes EKS
- Tags appliqués sur les subnets pour EKS :
  - Subnets publics : `kubernetes.io/role/elb = 1`, `kubernetes.io/cluster/logguardian = shared`
  - Subnets privés : `kubernetes.io/role/internal-elb = 1`, `kubernetes.io/cluster/logguardian = shared`

### Composants réseau
- **Internet Gateway** : attaché au VPC
- **NAT Gateway** : single AZ (économie de coûts, ~32$/mois au lieu de ~96$ en multi-AZ)
- **VPC Endpoint S3** : Gateway endpoint (gratuit, évite que le trafic S3 passe par le NAT)
- **Route tables** :
  - Publique : `0.0.0.0/0` → Internet Gateway
  - Privée : `0.0.0.0/0` → NAT Gateway

---

## IAM — Rôles et permissions

### 1. `logguardian-eks-cluster-role`
- **Trust** : `eks.amazonaws.com`
- **Policies** : `AmazonEKSClusterPolicy`
- **Usage** : rôle du control plane EKS

### 2. `logguardian-eks-node-role`
- **Trust** : `ec2.amazonaws.com`
- **Policies** :
  - `AmazonEKSWorkerNodePolicy`
  - `AmazonEKS_CNI_Policy`
  - `AmazonEC2ContainerRegistryReadOnly`
- **Usage** : rôle des worker nodes EC2

### 3. `logguardian-codebuild-role`
- **Trust** : `codebuild.amazonaws.com`
- **Policies** :
  - `AmazonEC2ContainerRegistryPowerUser`
  - `CloudWatchLogsFullAccess`
  - `AmazonS3FullAccess`
- **Usage** : rôle pour les builds Docker dans CodeBuild

### 4. `logguardian-codepipeline-role`
- **Trust** : `codepipeline.amazonaws.com`
- **Policies** (inline `pipeline-permissions`) :
  - `codebuild:*`, `s3:*`, `ecr:*`, `codestar-connections:UseConnection` sur `*`
- **Usage** : orchestration du pipeline CI/CD

---

## Amazon EKS

### Cluster
- **Nom** : `logguardian`
- **Version Kubernetes** : 1.35
- **Endpoint access** : Public and Private
- **Subnets** : les 4 subnets (publics + privés)
- **Logging control plane** : API server + Audit activés (authenticator, controller manager, scheduler désactivés)
- **Add-ons installés** : CoreDNS, kube-proxy, Amazon VPC CNI, EKS Pod Identity Agent

### Node group
- **Nom** : `logguardian-nodes`
- **Instance types** : `t3.medium` (2 vCPU, 4 Go RAM)
- **Capacity type** : On-Demand
- **Disk** : 20 Go
- **Scaling** : min=1, max=3, desired=2
- **Subnets** : subnets privés uniquement
- **Note** : les subnets privés nécessitent un Launch Template OU d'utiliser les subnets publics avec auto-assign public IP activé

### Accès au cluster
- **IAM access entry** configuré pour `arn:aws:iam::148761640356:user/Wassim` avec policy `AmazonEKSClusterAdminPolicy`
- **kubeconfig** généré via : `aws eks update-kubeconfig --region eu-west-1 --name logguardian`

### Pods système fonctionnels
- `aws-node` (VPC CNI) — 2 pods (DaemonSet)
- `coredns` — 2 pods
- `eks-pod-identity-agent` — 2 pods (DaemonSet)
- `kube-proxy` — 2 pods (DaemonSet)

---

## Amazon ECR — Registres d'images Docker

4 repositories créés :
- `logguardian/log-generator`
- `logguardian/etl-processor`
- `logguardian/ml-model`
- `logguardian/monitoring-ui`

URI de base : `148761640356.dkr.ecr.eu-west-1.amazonaws.com/`

---

## Amazon S3

### Bucket data lake
- **Nom** : `logguardian-datalake-148761640356`
- **Région** : eu-west-1
- **Intelligent-Tiering** : configuré avec archivage après 90 jours

### Bucket artefacts pipeline
- **Nom** : `logguardian-pipeline-artifacts-148761640356`
- **Usage** : stockage intermédiaire pour CodePipeline

---

## AWS Secrets Manager

- **Secret** : `logguardian/config`
- **Contenu actuel** (placeholder) :
  ```json
  {
    "kafka_topic": "logs-raw",
    "alert_threshold": "0.85",
    "environment": "dev"
  }
  ```

---

## Amazon CloudWatch

### Log groups
| Log group | Rétention |
|---|---|
| `/logguardian/application` | 14 jours |
| `/logguardian/etl` | 7 jours |
| `/logguardian/ml-model` | 7 jours |
| `/aws/eks/logguardian/cluster` | 7 jours (control plane) |

### Alarmes
| Alarme | Métrique | Seuil | Période |
|---|---|---|---|
| `logguardian-cpu-high` | `node_cpu_utilization` | > 80% | 5 min (2 eval) |
| `logguardian-memory-high` | `node_memory_utilization` | > 85% | 5 min (2 eval) |
| `logguardian-pod-restarts` | `pod_number_of_container_restarts` | > 3 | 10 min (1 eval) |

- **Statut actuel** : `INSUFFICIENT_DATA` (normal, Container Insights pas encore déployé)
- **Notifications** : via SNS topic `logguardian-alerts`

---

## CI/CD Pipeline

### Connexion GitHub ↔ AWS
- **Provider** : GitHub via CodeStar Connections
- **Connexion** : `logguardian-github` (statut : Available)
- **App GitHub** : AWS Connector for GitHub installé sur le repo `WassimTorjmen/logguardian`

### CodeBuild
- **Projet** : `logguardian-build`
- **Source** : CodePipeline
- **Environnement** : `aws/codebuild/amazonlinux2-x86_64-standard:5.0`, Linux, `BUILD_GENERAL1_SMALL`
- **privilegedMode** : true (nécessaire pour Docker)
- **Buildspec** : `buildspec.yml` à la racine du repo

### CodePipeline
- **Nom** : `logguardian-pipeline`
- **Type** : V2
- **Stages** :
  1. **Source** : GitHub (`WassimTorjmen/logguardian`, branche `develop`) → `SourceOutput`
  2. **Build** : CodeBuild (`logguardian-build`) → `BuildOutput`
- **Déclenchement** : automatique à chaque push sur `develop`

### Fichier buildspec.yml
Le buildspec à la racine du repo fait :
1. Login ECR
2. Build des 4 images Docker (log-generator, etl-processor, ml-model, monitoring-ui)
3. Tag avec le hash du commit
4. Push vers ECR

---

## Structure du repo GitHub

```
WassimTorjmen/logguardian (Private)
├── log-generator/
│   ├── Dockerfile          (à créer en Phase 2)
│   ├── src/
│   ├── tests/
│   └── README.md
├── etl-processor/
│   ├── Dockerfile          (à créer en Phase 2)
│   ├── src/
│   ├── tests/
│   └── README.md
├── ml-model/
│   ├── Dockerfile          (à créer en Phase 2)
│   ├── src/
│   ├── tests/
│   └── README.md
├── monitoring-ui/
│   ├── Dockerfile          (à créer en Phase 2)
│   ├── src/
│   ├── tests/
│   └── README.md
├── k8s/
│   ├── log-generator.yaml  (à créer en Phase 2)
│   ├── etl-processor.yaml  (à créer en Phase 2)
│   ├── ml-model.yaml       (à créer en Phase 2)
│   ├── monitoring-ui.yaml  (à créer en Phase 2)
│   └── README.md
├── buildspec.yml
├── .gitignore (Python)
└── README.md
```

### Stratégie de branches
- `main` — version stable/production, merge uniquement depuis `develop`
- `develop` — branche d'intégration, déclenche le pipeline CI/CD
- `feature/*` — branches de travail individuelles (feature/kafka, feature/etl-spark, feature/ml-model)

### Workflow Git pour l'équipe de 3
1. Chaque dev travaille sur sa branche `feature/*`
2. Push sur sa branche → crée une Pull Request vers `develop`
3. Au moins 1 collègue review et approuve
4. Merge dans `develop` → déclenche automatiquement CodePipeline
5. Une fois validé, merge `develop` → `main` pour release

---

## Commandes utiles

### Démarrage quotidien (recréer les nodes)
```bash
aws eks create-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes --node-role arn:aws:iam::148761640356:role/logguardian-eks-node-role --subnets SUBNET_PRIVE_1 SUBNET_PRIVE_2 --instance-types t3.medium --capacity-type ON_DEMAND --scaling-config minSize=1,maxSize=3,desiredSize=2 --disk-size 20 --region eu-west-1
```

### Arrêt du soir (supprimer les nodes)
```bash
aws eks delete-nodegroup --cluster-name logguardian --nodegroup-name logguardian-nodes --region eu-west-1
```

### Vérifications
```bash
kubectl get nodes -o wide
kubectl get pods -n kube-system
aws eks describe-cluster --name logguardian --query "cluster.status" --output text --region eu-west-1
aws codepipeline get-pipeline-state --name logguardian-pipeline --query "stageStates[*].{Stage:stageName,Status:latestExecution.status}" --output table --region eu-west-1
```

### Login ECR (pour push manuels)
```bash
aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 148761640356.dkr.ecr.eu-west-1.amazonaws.com
```

---

## Identifiants et ARN importants

| Ressource | Valeur |
|---|---|
| AWS Account ID | `148761640356` |
| Région | `eu-west-1` |
| VPC | `logguardian-vpc` |
| Cluster EKS | `logguardian` |
| IAM User | `Wassim` |
| GitHub Repo | `WassimTorjmen/logguardian` |
| ECR URI base | `148761640356.dkr.ecr.eu-west-1.amazonaws.com/logguardian/` |
| S3 Data Lake | `logguardian-datalake-148761640356` |
| S3 Pipeline Artifacts | `logguardian-pipeline-artifacts-148761640356` |
| Pipeline | `logguardian-pipeline` |
| CodeBuild Project | `logguardian-build` |
| SNS Topic | `arn:aws:sns:eu-west-1:148761640356:logguardian-alerts` |
| CodeStar Connection | `logguardian-github` |

---

## Phase 2 — Prochaines étapes

1. Créer le premier Dockerfile (log-generator) et le déployer sur EKS
2. Déployer Amazon MSK (Kafka managé) pour l'ingestion des logs
3. Développer le processeur ETL avec Spark Streaming
4. Entraîner et déployer le modèle ML (Autoencoder LSTM)
5. Créer le dashboard de monitoring (Grafana)
6. Configurer l'auto-remédiation Kubernetes (restart/kill pods)
7. Déployer Container Insights pour activer les alarmes CloudWatch
