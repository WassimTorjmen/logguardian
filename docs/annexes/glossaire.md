# Glossaire

| Terme | Définition |
|---|---|
| **AIOps** | Artificial Intelligence for IT Operations — usage du ML pour gérer l'infra |
| **Anomaly score** | MSE de reconstruction par l'autoencoder |
| **Artifact Registry** | Registre d'images Docker de GCP (successeur de Container Registry) |
| **Autoencoder** | Réseau de neurones qui apprend à reconstruire son entrée — la qualité de reconstruction sert d'indicateur |
| **Cloud Build** | Service CI/CD de GCP |
| **ConfigMap** | Objet K8s qui stocke de la configuration non sensible (clé/valeur) |
| **Cooldown** | Délai entre deux notifications pour le même type d'alerte (deprecated, remplacé par batching) |
| **Deployment** | Objet K8s qui gère un ensemble de pods identiques |
| **Embedding** | Représentation vectorielle d'un mot/token |
| **ETL** | Extract Transform Load |
| **GCS** | Google Cloud Storage |
| **GKE** | Google Kubernetes Engine |
| **Groq** | API LLM cloud (utilisée pour le RAG explicatif) |
| **Kustomization** | Outil natif `kubectl` pour personnaliser des manifests YAML |
| **Latent** | Représentation compressée intermédiaire dans l'autoencoder |
| **Loghub** | Dataset académique de logs réels — github.com/logpai/loghub |
| **LSTM** | Long Short-Term Memory — un type de RNN qui gère les dépendances longues |
| **MSE** | Mean Squared Error — métrique de reconstruction |
| **Parquet** | Format colonnaire pour le stockage analytique |
| **PVC** | PersistentVolumeClaim — demande de volume persistant K8s |
| **RAG** | Retrieval-Augmented Generation — LLM enrichi par du contexte |
| **Round-robin** | Algorithme d'interleaving cyclique |
| **Secret** | Objet K8s qui stocke des données sensibles (encodées base64) |
| **SendGrid** | Service d'envoi d'emails transactionnels (filiale Twilio) |
| **Severity ratio** | `anomaly_score / threshold` — multiplicateur du seuil |
| **Threshold** | Seuil au-delà duquel un score est considéré comme une anomalie |
| **Vertex AI** | Plateforme ML managée de GCP |
| **Workload Identity** | Mécanisme GKE qui lie un KSA (K8s Service Account) à un GSA (Google SA) pour accéder aux API GCP sans clé JSON |
