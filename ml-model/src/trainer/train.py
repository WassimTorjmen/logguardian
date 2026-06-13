"""
Version améliorée :
- Early stopping : arrête si le modèle ne s'améliore plus
- Gradient clipping : évite les explosions de gradients
- LR scheduler : réduit le learning rate quand on stagne
"""
import argparse, json, logging, os, pickle
from datetime import datetime, timezone
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from .dataset import LogSequenceDataset, prepare_datasets
from .features import N_FEATURES, VOCAB_SIZE, EMBED_DIM
from .model import LSTMAutoencoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("trainer")

HIDDEN_SIZE   = 64
LATENT_SIZE   = 32
BATCH_SIZE    = 64
EPOCHS        = 50      # Early stopping s'en chargera
LR            = 1e-3
THRESHOLD_PCT = 95
PATIENCE      = 7       # Arrêt si pas d'amélioration après 7 epochs
GRAD_CLIP     = 1.0     # Plafonne les gradients
NUM_LAYERS    = 2
DROPOUT       = 0.2


def train(parquet_dir: str, output_dir: str, device: str):
    os.makedirs(output_dir, exist_ok=True)

    train_seqs, val_seqs, all_seqs, all_labels, vocab, embedding_table, scaler = prepare_datasets(
        parquet_dir=parquet_dir,
        seq_len=10,
    )

    train_loader = DataLoader(
        LogSequenceDataset(train_seqs),
        batch_size=BATCH_SIZE,
        shuffle=True,
        num_workers=2,
        pin_memory=(device == "cuda"),
    )
    val_loader = DataLoader(
        LogSequenceDataset(val_seqs),
        batch_size=BATCH_SIZE,
        shuffle=False,
        num_workers=2,
    )

    model = LSTMAutoencoder(
        n_features=N_FEATURES,
        hidden_size=HIDDEN_SIZE,
        latent_size=LATENT_SIZE,
        seq_len=10,
        num_layers=NUM_LAYERS,
        dropout=DROPOUT,
    ).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    criterion = nn.MSELoss()

    # Réduit le LR si val_loss ne baisse plus après 3 epochs
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=3, factor=0.5
    )

    log.info("Début entraînement | %d train | %d val", len(train_seqs), len(val_seqs))

    best_val_loss    = float("inf")
    patience_counter = 0
    train_losses, val_losses = [], []

    for epoch in range(1, EPOCHS + 1):

        # ── Phase entraînement ────────────────────────────────
        model.train()
        epoch_loss = 0.0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()
            loss = criterion(model(batch), batch)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)
            optimizer.step()
            epoch_loss += loss.item()
        train_loss = epoch_loss / len(train_loader)

        # ── Phase validation ──────────────────────────────────
        model.eval()
        val_loss = 0.0
        with torch.no_grad():
            for batch in val_loader:
                val_loss += criterion(model(batch.to(device)), batch.to(device)).item()
        val_loss /= len(val_loader)

        scheduler.step(val_loss)
        train_losses.append(train_loss)
        val_losses.append(val_loss)

        # ── Early stopping ────────────────────────────────────
        if val_loss < best_val_loss:
            best_val_loss    = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), os.path.join(output_dir, "best_model.pt"))
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                log.info("Early stopping à l'epoch %d (patience=%d)", epoch, PATIENCE)
                break

        if epoch % 5 == 0 or epoch == 1:
            log.info(
                "Epoch %3d | train=%.6f | val=%.6f | lr=%.5f | patience=%d/%d",
                epoch, train_loss, val_loss,
                optimizer.param_groups[0]["lr"],
                patience_counter, PATIENCE,
            )

    # Recharge le meilleur modèle pour calculer le seuil
    model.load_state_dict(torch.load(os.path.join(output_dir, "best_model.pt"), map_location=device))

    # ── Calcul du seuil d'anomalie ────────────────────────────
    model.eval()
    val_errors = []
    with torch.no_grad():
        for batch in DataLoader(LogSequenceDataset(val_seqs), batch_size=256):
            val_errors.extend(model.reconstruction_error(batch.to(device)).cpu().numpy())
    val_errors = np.array(val_errors)
    threshold  = float(np.percentile(val_errors, THRESHOLD_PCT))
    log.info("Seuil (p%d) : %.6f", THRESHOLD_PCT, threshold)

    # ── Sauvegarde des artefacts ──────────────────────────────
    torch.save(model.state_dict(), os.path.join(output_dir, "lstm_autoencoder.pt"))
    with open(os.path.join(output_dir, "vocabulary.pkl"), "wb") as f:
        pickle.dump(vocab, f)
    np.save(os.path.join(output_dir, "embedding_table.npy"), embedding_table)
    with open(os.path.join(output_dir, "feature_scaler.pkl"), "wb") as f:
        pickle.dump(scaler, f)

    meta = {
        "threshold":     threshold,
        "percentile":    THRESHOLD_PCT,
        "computed_at":   datetime.now(tz=timezone.utc).isoformat(),
        "n_samples":     len(val_seqs),
        "n_features":    N_FEATURES,
        "seq_len":       10,
        "hidden_size":   HIDDEN_SIZE,
        "latent_size":   LATENT_SIZE,
        "num_layers":    NUM_LAYERS,
        "dropout":       DROPOUT,
        "vocab_size":    VOCAB_SIZE,
        "embed_dim":     EMBED_DIM,
        "train_loss":    train_losses[-1],
        "val_loss":      val_losses[-1],
        "best_val_loss": best_val_loss,
    }
    with open(os.path.join(output_dir, "threshold.json"), "w") as f:
        json.dump(meta, f, indent=2)

    log.info("Terminé. Best val loss : %.6f", best_val_loss)
    return meta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="/tmp/etl-output-sample")
    parser.add_argument("--output", default="models")
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()
    train(args.input, args.output, args.device)
