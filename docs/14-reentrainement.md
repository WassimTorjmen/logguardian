# 14 — Réentraînement du modèle

## Quand réentraîner ?

- Drift : les anomalies ne sont plus détectées correctement, le seuil ne marche plus
- Nouvelle source : ajouter Android dans `SOURCES` puis réentraîner
- Plus de données : un dataset plus grand devrait améliorer la généralisation
- Changement d'architecture : modifier `hidden_size`, `latent_size`, etc.

## Procédure — en local (CPU)

### 1. Récupérer les données

```bash
mkdir -p ./data/parquet
gsutil -m rsync -r gs://logguardian-datalake-logguardian-497218/logs/ ./data/parquet/
```

### 2. Installer les deps

```bash
cd ml-model
python -m venv .venv
.\.venv\Scripts\Activate.ps1     # Windows
# source .venv/bin/activate      # Linux/Mac
pip install -r requirements.txt
```

### 3. Lancer l'entraînement

```bash
cd ml-model
python -m src.trainer.train \
    --input ./data/parquet \
    --output ./models \
    --device cpu
```

**Arguments** :
- `--input` : dossier contenant les fichiers Parquet
- `--output` : où écrire les artefacts
- `--device` : `cpu` ou `cuda` (si GPU disponible)

**Durée** :
- ~36k séquences, CPU MacBook M4 : ~4h
- GPU CUDA : ~15-30 min

### 4. Vérifier les artefacts

```bash
ls -la ./models/
# Tu dois avoir :
# lstm_autoencoder.pt
# best_model.pt
# vocabulary.pkl
# embedding_table.npy
# feature_scaler.pkl
# threshold.json
```

Vérifier les clés du state dict :

```python
import torch
sd = torch.load("./models/lstm_autoencoder.pt", map_location="cpu", weights_only=True)
print(list(sd.keys())[:5])
# Doit contenir 'encoder.weight_ih_l0_reverse', 'attention.W', 'fc.weight'
# Si non → tu as un ancien modèle (mauvaise architecture)
```

### 5. Uploader sur GCS

```bash
gsutil -m cp ./models/lstm_autoencoder.pt \
              ./models/vocabulary.pkl \
              ./models/embedding_table.npy \
              ./models/feature_scaler.pkl \
              ./models/threshold.json \
              gs://logguardian-models-logguardian-497218/
```

### 6. Redéployer le ml-model

```bash
kubectl rollout restart deployment/ml-model -n logguardian
kubectl logs deployment/ml-model -n logguardian -f
```

Tu dois voir : `Modèle chargé depuis /app/models | seuil=X.XXX`.

## Procédure — sur Vertex AI (cloud)

Pour un réentraînement plus rapide (GPU), utiliser Vertex AI Custom Training. Voir le code dans `ml-model/vertex/` si présent, sinon créer manuellement :

```bash
# Conteneur de training
gcloud ai custom-jobs create \
    --region=europe-west1 \
    --display-name=lstm-train \
    --worker-pool-spec=machine-type=n1-standard-8,replica-count=1,accelerator-type=NVIDIA_TESLA_T4,accelerator-count=1,container-image-uri=europe-west1-docker.pkg.dev/logguardian-497218/logguardian/ml-model:latest,local-package-path=./ml-model
```

## Ajouter une source

Pour intégrer Android (ou une autre source) au modèle :

1. Modifier `ml-model/src/trainer/features.py` :
   ```python
   SOURCES = ["linux", "ssh", "hadoop", "spark", "supercomputer", "hdfs", "android"]
   ```
2. **Conséquence** : `N_FEATURES` passe de 77 à 78 → incompatible avec ancien modèle
3. Réentraîner complètement (étapes 1-6 ci-dessus)
4. Réactiver Android dans `k8s/log-generator.yaml` :
   ```yaml
   LOG_SOURCES: linux,ssh,hadoop,supercomputer,spark,android
   ```

## Valider qualitativement

Après déploiement, vérifier que :
- Le ratio anomalies / logs total est raisonnable (1-5% en steady state)
- Les anomalies pointent vers des événements vraiment suspects (logs ERROR, séquences de redémarrage, brute force SSH)
- Le seuil n'est pas trop bas (trop de faux positifs) ni trop haut (rate les vraies anomalies)

Ajuster `THRESHOLD_PCT` (95 par défaut) :
- 99 → plus sélectif (1% de logs normaux comptés anomalies)
- 90 → moins sélectif (10% comptés anomalies)
