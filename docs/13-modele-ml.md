# 13 — Modèle LSTM Autoencoder

## Architecture

```
Input: séquence de 10 logs × 77 features
   │
   ▼
LSTM Encoder bidirectionnel (2 couches, hidden=64)
   │
   ▼
Attention sur la sortie encoder
   │
   ▼
Latent: 32 dims
   │
   ▼
LSTM Decoder (2 couches)
   │
   ▼
Output: séquence reconstruite 10 × 77

Loss: MSE(input, output)
```

## Features (77 dimensions)

Pour chaque log, `log_to_vector()` produit un vecteur de 77 dimensions :

| Bloc | Dims | Description |
|---|---|---|
| Source one-hot | 6 | `linux, ssh, hadoop, spark, supercomputer, hdfs` |
| Level one-hot | 5 | `DEBUG, INFO, WARN, ERROR, FATAL` |
| Hour cyclique | 2 | `sin(2π·h/24)`, `cos(...)` pour éviter discontinuité 23h→0h |
| Message embedding | 64 | Mean des embeddings des tokens du message |

### Tokenisation message

```python
tokens = re.findall(r"[a-z0-9]+", text.lower())
indices = [vocab.get(t, 0) for t in tokens]
embedding = embedding_table[indices].mean(axis=0)
```

- Vocabulaire de 5000 tokens (top fréquence à l'entraînement)
- Token inconnu → index 0 → vecteur zéro
- Embeddings 64 dims initialisés N(0, 0.1²), seed fixé pour reproductibilité

## Hyperparamètres

```python
HIDDEN_SIZE   = 64
LATENT_SIZE   = 32
NUM_LAYERS    = 2
DROPOUT       = 0.2
SEQ_LEN       = 10
BATCH_SIZE    = 64
EPOCHS        = 50           # avec early stopping patience=7
LR            = 1e-3
GRAD_CLIP     = 1.0
THRESHOLD_PCT = 95           # percentile sur erreurs de validation
```

## Calcul du seuil

À la fin de l'entraînement :
1. Reconstruction MSE sur le set de validation
2. `threshold = np.percentile(val_errors, 95)`
3. Conséquence : **5% des séquences "normales" passent au-dessus** du seuil

→ Si le modèle est mal calibré, beaucoup de faux positifs. Voir [troubleshooting](18-troubleshooting.md).

## Artefacts produits

Tous dans le dossier `output` du training :

| Fichier | Contenu | Taille typique |
|---|---|---|
| `lstm_autoencoder.pt` | Poids PyTorch | ~500 KB |
| `vocabulary.pkl` | dict {token: idx} | ~100 KB |
| `embedding_table.npy` | array (5001, 64) | ~1.2 MB |
| `feature_scaler.pkl` | StandardScaler | ~5 KB |
| `threshold.json` | Métadonnées + seuil | ~500 B |
| `best_model.pt` | Checkpoint early-stopping | ~500 KB |

## Métadonnées `threshold.json`

```json
{
  "threshold": 0.763,
  "percentile": 95,
  "computed_at": "2026-06-14T13:04:18+00:00",
  "n_samples": 36366,
  "n_features": 77,
  "seq_len": 10,
  "hidden_size": 64,
  "latent_size": 32,
  "num_layers": 2,
  "dropout": 0.2,
  "vocab_size": 5000,
  "embed_dim": 64,
  "train_loss": 0.299,
  "val_loss": 0.225,
  "best_val_loss": 0.225
}
```

## Modifier le seuil sans réentraîner

Si trop de faux positifs : multiplier par 2 (ou 3) directement dans `threshold.json` :

```bash
# Télécharger
gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./threshold.json

# Éditer "threshold": 0.763... → 1.527 (= ×2)

# Uploader
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json

# Redémarrer le pod
kubectl rollout restart deployment/ml-model -n logguardian
```

Vérifier dans les logs :
```
INFO detector: Modèle chargé depuis /app/models | seuil=1.526948
```

## Limites du modèle actuel

- **`android` non supporté** : pas dans la liste `SOURCES` de `features.py` — les logs Android ont leur source one-hot à zéro, mauvaise reconstruction. Désactivé via `LOG_SOURCES` actuellement.
- **Pas d'attention sur les tokens** : l'embedding est une moyenne, pas pondérée.
- **Seuil global** : un seul `threshold` pour toutes les sources. Idéalement un seuil par `(source, host)`.
- **Entraînement non-incrémental** : pour ajouter une source, il faut tout réentraîner.
