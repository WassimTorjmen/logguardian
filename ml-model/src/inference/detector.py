"""
Chargement du modèle entraîné et scoring d'une séquence.
"""
import json
import logging
import os
import pickle

import numpy as np
import torch
from google.cloud import storage as gcs

from trainer.features import log_to_vector
from trainer.model import LSTMAutoencoder

log = logging.getLogger(__name__)


class AnomalyDetector:
    def __init__(self, model_dir: str, device: str = "cpu"):
        self.device    = device
        self.model_dir = model_dir
        self._load_artifacts()

    def _load_artifacts(self):
        meta_path = os.path.join(self.model_dir, "threshold.json")
        with open(meta_path) as f:
            self.meta = json.load(f)

        with open(os.path.join(self.model_dir, "vocabulary.pkl"), "rb") as f:
            self.vocab = pickle.load(f)

        self.embedding_table = np.load(os.path.join(self.model_dir, "embedding_table.npy"))

        with open(os.path.join(self.model_dir, "feature_scaler.pkl"), "rb") as f:
            self.scaler = pickle.load(f)

        self.model = LSTMAutoencoder(
            n_features=self.meta["n_features"],
            hidden_size=self.meta["hidden_size"],
            latent_size=self.meta["latent_size"],
            seq_len=self.meta["seq_len"],
            num_layers=self.meta.get("num_layers", 2),
            dropout=self.meta.get("dropout", 0.2),
        ).to(self.device)
        self.model.load_state_dict(
            torch.load(
                os.path.join(self.model_dir, "lstm_autoencoder.pt"),
                map_location=self.device,
                weights_only=True,
            )
        )
        self.model.eval()
        log.info("Modèle chargé depuis %s | seuil=%.6f", self.model_dir, self.meta["threshold"])

    def score(self, sequence: list[dict]) -> float:
        """Retourne le score MSE de reconstruction pour une séquence de logs."""
        vecs = np.stack([
            log_to_vector(r, self.vocab, self.embedding_table) for r in sequence
        ])
        vecs_scaled = self.scaler.transform(vecs).astype(np.float32)
        tensor = torch.tensor(vecs_scaled[np.newaxis], dtype=torch.float32).to(self.device)
        return float(self.model.reconstruction_error(tensor).item())

    def is_anomaly(self, sequence: list[dict]) -> tuple[bool, float]:
        """Retourne (est_anomalie, score)."""
        score = self.score(sequence)
        return score > self.meta["threshold"], score

    @property
    def threshold(self) -> float:
        return self.meta["threshold"]

    @property
    def model_version(self) -> str:
        return f"lstm_v1_{self.meta['computed_at'][:10].replace('-', '')}"


def download_models_from_gcs(bucket: str, model_dir: str):
    """Télécharge les artefacts depuis GCS au démarrage du pod (Workload Identity)."""
    os.makedirs(model_dir, exist_ok=True)
    client = gcs.Client()
    gcs_bucket = client.bucket(bucket)
    artifacts = [
        "lstm_autoencoder.pt",
        "vocabulary.pkl",
        "embedding_table.npy",
        "feature_scaler.pkl",
        "threshold.json",
    ]
    for name in artifacts:
        dest = os.path.join(model_dir, name)
        log.info("Téléchargement gs://%s/%s → %s", bucket, name, dest)
        gcs_bucket.blob(name).download_to_filename(dest)
    log.info("Artefacts téléchargés depuis GCS.")
