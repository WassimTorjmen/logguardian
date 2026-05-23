"""
Script d'entraînement offline du LSTM Autoencoder.

Usage :
    python -m trainer.train --input /tmp/etl-output-sample --output models/
"""
import argparse
import json
import logging
import os
import pickle
from datetime import datetime, timezone

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from .dataset import LogSequenceDataset, prepare_datasets
from .features import EMBED_DIM, N_FEATURES, VOCAB_SIZE
from .model import LSTMAutoencoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("trainer")

# Hyperparamètres
HIDDEN_SIZE   = 64
LATENT_SIZE   = 32
BATCH_SIZE    = 64
EPOCHS        = 30
LR            = 1e-3
THRESHOLD_PCT = 95


def train(parquet_dir: str, output_dir: str, device: str):
    os.makedirs(output_dir, exist_ok=True)

    # ── Données ──────────────────────────────────────────────────────────────
    train_seqs, val_seqs, all_seqs, all_labels, vocab, embedding_table, scaler = prepare_datasets(
        parquet_dir=parquet_dir,
        seq_len=10,
    )

    train_loader = DataLoader(LogSequenceDataset(train_seqs), batch_size=BATCH_SIZE, shuffle=True)
    val_loader   = DataLoader(LogSequenceDataset(val_seqs),   batch_size=BATCH_SIZE, shuffle=False)

    # ── Modèle ───────────────────────────────────────────────────────────────
    model = LSTMAutoencoder(
        n_features=N_FEATURES,
        hidden_size=HIDDEN_SIZE,
        latent_size=LATENT_SIZE,
        seq_len=10,
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    criterion = nn.MSELoss()

    log.info("Début entraînement sur %s | %d séquences train | %d val", device, len(train_seqs), len(val_seqs))

    # ── Boucle d'entraînement ─────────────────────────────────────────────────
    train_losses, val_losses = [], []

    for epoch in range(1, EPOCHS + 1):
        model.train()
        epoch_loss = 0.0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            loss = criterion(model(batch), batch)
            loss.backward()
            optimizer.step()
            epoch_loss += loss.item()
        train_loss = epoch_loss / len(train_loader)

        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch in val_loader:
                batch = batch.to(device)
                val_loss += criterion(model(batch), batch).item()
        val_loss /= len(val_loader)

        train_losses.append(train_loss)
        val_losses.append(val_loss)

        if epoch % 5 == 0 or epoch == 1:
            log.info("Epoch %3d/%d | train=%.6f | val=%.6f", epoch, EPOCHS, train_loss, val_loss)

    # ── Seuil d'anomalie ──────────────────────────────────────────────────────
    model.eval()
    val_errors = []
    val_loader_full = DataLoader(LogSequenceDataset(val_seqs), batch_size=256)
    with torch.no_grad():
        for batch in val_loader_full:
            val_errors.extend(model.reconstruction_error(batch.to(device)).cpu().numpy())
    val_errors = np.array(val_errors)
    threshold  = float(np.percentile(val_errors, THRESHOLD_PCT))
    log.info("Seuil (p%d) : %.6f", THRESHOLD_PCT, threshold)

    # ── Sauvegarde des artefacts ──────────────────────────────────────────────
    torch.save(model.state_dict(), os.path.join(output_dir, "lstm_autoencoder.pt"))

    with open(os.path.join(output_dir, "vocabulary.pkl"), "wb") as f:
        pickle.dump(vocab, f)

    np.save(os.path.join(output_dir, "embedding_table.npy"), embedding_table)

    with open(os.path.join(output_dir, "feature_scaler.pkl"), "wb") as f:
        pickle.dump(scaler, f)

    threshold_data = {
        "threshold":   threshold,
        "percentile":  THRESHOLD_PCT,
        "computed_at": datetime.now(tz=timezone.utc).isoformat(),
        "n_samples":   len(val_seqs),
        "n_features":  N_FEATURES,
        "seq_len":     10,
        "hidden_size": HIDDEN_SIZE,
        "latent_size": LATENT_SIZE,
        "vocab_size":  VOCAB_SIZE,
        "embed_dim":   EMBED_DIM,
        "train_loss":  train_losses[-1],
        "val_loss":    val_losses[-1],
    }
    with open(os.path.join(output_dir, "threshold.json"), "w") as f:
        json.dump(threshold_data, f, indent=2)

    log.info("Artefacts sauvegardés dans %s", output_dir)
    log.info("Train loss finale : %.6f | Val loss finale : %.6f", train_losses[-1], val_losses[-1])
    return threshold_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="/tmp/etl-output-sample", help="Dossier Parquet ETL")
    parser.add_argument("--output", default="models",                  help="Dossier de sortie")
    parser.add_argument("--device", default="cpu",                     help="cpu ou cuda")
    args = parser.parse_args()

    train(args.input, args.output, args.device)
