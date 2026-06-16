# 03 — Prérequis

## Outils communs

| Outil | Version min | Pourquoi |
|---|---|---|
| Git | 2.30+ | Cloner le repo |
| Docker Desktop | 4.0+ | Build images, exécution locale |
| Docker Compose | v2 | Orchestration locale |
| Python | 3.11 | Réentraîner le modèle ML en local |

## Outils GCP (pour le déploiement cloud)

| Outil | Version min | Pourquoi |
|---|---|---|
| `gcloud` SDK | 460+ | Authentification GCP, gérer GKE |
| `gke-gcloud-auth-plugin` | dernière | Plugin auth K8s (obligatoire depuis kubectl 1.26) |
| `kubectl` | 1.27+ | Déployer sur GKE |
| `gsutil` (inclus dans gcloud) | — | Upload/download GCS |
| Terraform | 1.5+ | Provisionner l'infra (optionnel si déjà fait) |

### Installation gcloud (Windows)

```powershell
# Via installeur officiel
Invoke-WebRequest -Uri https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe -OutFile gcloud.exe
.\gcloud.exe

# Puis dans une nouvelle session PowerShell
gcloud components install kubectl gke-gcloud-auth-plugin
```

### Variable d'environnement obligatoire

```powershell
# Activer le plugin auth GKE (PowerShell)
$env:USE_GKE_GCLOUD_AUTH_PLUGIN = "True"

# Persistant (à mettre dans $PROFILE)
[Environment]::SetEnvironmentVariable("USE_GKE_GCLOUD_AUTH_PLUGIN", "True", "User")
```

## Comptes et accès nécessaires

### Compte GCP

- Projet : `logguardian-497218`
- Demander au propriétaire (Wassim) un rôle minimum :
  - `roles/container.developer` (déployer sur GKE)
  - `roles/storage.objectViewer` (lire les buckets)
  - `roles/artifactregistry.reader` (pull images)
- Pour les opérations admin : `roles/owner` ou `roles/editor`

### Authentification

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project logguardian-497218
gcloud container clusters get-credentials logguardian --zone=europe-west1-b
```

### Compte SendGrid (pour les emails)

1. Créer un compte gratuit sur [sendgrid.com](https://sendgrid.com/) (trial = 100 emails/jour pendant 60 jours).
2. **Sender Authentication** → créer un Single Sender (email du compte) et valider le lien reçu par mail.
3. **API Keys** → "Create API Key" → Full Access → copier la clé (`SG.xxxx...`).

### Compte Groq (pour l'IA explicative)

1. Créer un compte sur [console.groq.com](https://console.groq.com/).
2. **API Keys** → générer une clé.
3. Le modèle gratuit recommandé : `openai/gpt-oss-20b`.

## Cloner le repo

```bash
git clone https://github.com/WassimTorjmen/logguardian.git
cd logguardian
git checkout develop   # branche de travail
```

## Vérification

```bash
docker --version          # >= 20.10
docker compose version    # v2.x
python --version          # 3.11
gcloud --version          # 460+
kubectl version --client  # 1.27+
```
