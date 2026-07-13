"""
train.py — LE CHEF D'ORCHESTRE de l'entraînement

Ce fichier utilise tous les autres fichiers et lance l'entraînement complet.

Améliorations apportées :
- Early stopping    : arrête si le modèle ne s'améliore plus (patience=7)
- Gradient clipping : évite les explosions de gradients (max=1.0)
- LR scheduler      : réduit le learning rate quand on stagne
- Courbe de loss    : sauvegardée en image (loss_curve.png)
- Matrice de confusion : sauvegardée en image (confusion_matrix.png)
- Résumé final      : affiché dans le terminal à la fin

RÉSUMÉ :
1. Charge les données       → prepare_datasets()
2. Crée le modèle           → LSTMAutoencoder()
3. Boucle d'entraînement    → 50 epochs max
4. Calcule le seuil         → percentile 95
5. Sauvegarde les artefacts → 5 fichiers dans models/
6. Calcule les métriques    → F1, AUC-ROC, précision, rappel
7. Sauvegarde les graphiques → loss_curve.png, confusion_matrix.png
"""
import argparse, json, logging, os, pickle
from datetime import datetime, timezone
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from sklearn.metrics import f1_score, roc_auc_score, classification_report, precision_score, recall_score, confusion_matrix
import matplotlib
matplotlib.use("Agg")  # pas besoin d'écran pour sauvegarder en fichier
import matplotlib.pyplot as plt
import seaborn as sns

from .dataset import LogSequenceDataset, prepare_datasets
from .features import N_FEATURES, VOCAB_SIZE, EMBED_DIM
from .model import LSTMAutoencoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("trainer")

# ── Hyperparamètres ───────────────────────────────────────────────────────────
HIDDEN_SIZE   = 64    # taille de la mémoire interne du LSTM
LATENT_SIZE   = 32    # taille du vecteur compressé (cœur de l'autoencoder)
BATCH_SIZE    = 64    # nombre de séquences traitées en même temps
EPOCHS        = 50    # nombre maximum de passages sur toutes les données
LR            = 1e-3  # learning rate = vitesse d'apprentissage (0.001)
THRESHOLD_PCT = 95    # percentile pour calculer le seuil d'anomalie
PATIENCE      = 7     # early stopping : arrêt après 7 epochs sans amélioration
GRAD_CLIP     = 1.0   # limite maximale des gradients pour éviter les explosions
NUM_LAYERS    = 2     # nombre de couches LSTM empilées
DROPOUT       = 0.2   # 20% des neurones désactivés pendant l'entraînement


def train(parquet_dir: str, output_dir: str, device: str):
    # Crée le dossier de sortie si il n'existe pas
    os.makedirs(output_dir, exist_ok=True)

    # ── Chargement et préparation des données ─────────────────────────────────
    # Appelle prepare_datasets de dataset.py :
    # charge les Parquet, vectorise, construit les séquences, sépare normal/suspect
    train_seqs, val_seqs, all_seqs, all_labels, vocab, embedding_table, scaler = prepare_datasets(
        parquet_dir=parquet_dir,
        seq_len=10,
    )

    # ── DataLoaders ───────────────────────────────────────────────────────────
    # Le DataLoader distribue les données au modèle par paquets de 64 séquences
    train_loader = DataLoader(
        LogSequenceDataset(train_seqs),
        batch_size=BATCH_SIZE,
        shuffle=True,      # mélange les données à chaque epoch
        num_workers=2,
        pin_memory=(device == "cuda"),
    )
    val_loader = DataLoader(
        LogSequenceDataset(val_seqs),
        batch_size=BATCH_SIZE,
        shuffle=False,     # pas besoin de mélanger pour la validation
        num_workers=2,
    )

    # ── Création du modèle ────────────────────────────────────────────────────
    model = LSTMAutoencoder(
        n_features=N_FEATURES,   # 77
        hidden_size=HIDDEN_SIZE, # 64
        latent_size=LATENT_SIZE, # 32
        seq_len=10,
        num_layers=NUM_LAYERS,   # 2
        dropout=DROPOUT,         # 0.2
    ).to(device)  # envoie le modèle sur CPU ou GPU

    # ── Optimiseur et fonction de loss ────────────────────────────────────────
    # Adam : algorithme qui ajuste les poids du modèle après chaque batch
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    # MSELoss : mesure l'erreur entre la reconstruction et l'original
    criterion = nn.MSELoss()

    # LR Scheduler : si val_loss ne baisse plus pendant 3 epochs → divise LR par 2
    # Le modèle apprend plus lentement mais plus précisément
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", patience=3, factor=0.5
    )

    log.info("Début entraînement | %d train | %d val", len(train_seqs), len(val_seqs))

    best_val_loss    = float("inf")  # meilleure val_loss vue jusqu'ici
    patience_counter = 0             # compteur pour l'early stopping
    train_losses, val_losses = [], [] # historique des losses pour le graphique

    # ── Boucle d'entraînement ─────────────────────────────────────────────────
    for epoch in range(1, EPOCHS + 1):

        # ── Phase entraînement ────────────────────────────────
        model.train()  # active le mode entraînement (dropout actif)
        epoch_loss = 0.0
        for batch in train_loader:
            batch = batch.to(device)
            optimizer.zero_grad()                                    # remet les gradients à zéro
            loss = criterion(model(batch), batch)                    # calcule l'erreur MSE
            loss.backward()                                          # calcule les gradients
            torch.nn.utils.clip_grad_norm_(model.parameters(), GRAD_CLIP)  # plafonne les gradients
            optimizer.step()                                         # ajuste les poids
            epoch_loss += loss.item()
        train_loss = epoch_loss / len(train_loader)  # moyenne sur tous les batchs

        # ── Phase validation ──────────────────────────────────
        model.eval()  # désactive le dropout (mode évaluation)
        val_loss = 0.0
        with torch.no_grad():  # pas de calcul de gradients en validation
            for batch in val_loader:
                val_loss += criterion(model(batch.to(device)), batch.to(device)).item()
        val_loss /= len(val_loader)

        # Met à jour le LR si nécessaire
        scheduler.step(val_loss)
        train_losses.append(train_loss)
        val_losses.append(val_loss)

        # ── Early stopping ────────────────────────────────────
        if val_loss < best_val_loss:
            # La val_loss s'améliore → on sauvegarde le meilleur modèle
            best_val_loss    = val_loss
            patience_counter = 0
            torch.save(model.state_dict(), os.path.join(output_dir, "best_model.pt"))
        else:
            # Pas d'amélioration → on incrémente le compteur
            patience_counter += 1
            if patience_counter >= PATIENCE:
                # 7 epochs sans amélioration → on arrête
                log.info("Early stopping à l'epoch %d (patience=%d)", epoch, PATIENCE)
                break

        log.info(
            "Epoch %3d | train=%.6f | val=%.6f | lr=%.5f | patience=%d/%d",
            epoch, train_loss, val_loss,
            optimizer.param_groups[0]["lr"],
            patience_counter, PATIENCE,
        )

    # ── Calcul du seuil ───────────────────────────────────────────────────────
    # On recharge le MEILLEUR modèle sauvegardé pendant l'entraînement
    model.load_state_dict(torch.load(os.path.join(output_dir, "best_model.pt"), map_location=device))

    model.eval()
    val_errors = []
    with torch.no_grad():
        for batch in DataLoader(LogSequenceDataset(val_seqs), batch_size=256):
            # Calcule le score MSE de chaque séquence de validation
            val_errors.extend(model.reconstruction_error(batch.to(device)).cpu().numpy())
    val_errors = np.array(val_errors)
    # Le seuil = percentile 95 des scores → 95% des séquences normales sont en dessous
    threshold  = float(np.percentile(val_errors, THRESHOLD_PCT))
    log.info("Seuil (p%d) : %.6f", THRESHOLD_PCT, threshold)

    # ── Sauvegarde des artefacts ──────────────────────────────────────────────
    # Ces 5 fichiers sont nécessaires pour que detector.py fonctionne en production
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

    # ── Évaluation finale ─────────────────────────────────────────────────────
    # On teste sur TOUTES les séquences (normales + suspectes)
    # C'est la seule fois où le modèle voit des séquences suspectes
    log.info("Évaluation F1 / AUC-ROC sur %d séquences étiquetées...", len(all_seqs))
    all_scores = []
    with torch.no_grad():
        for batch in DataLoader(LogSequenceDataset(all_seqs), batch_size=256):
            all_scores.extend(model.reconstruction_error(batch.to(device)).cpu().numpy())

    all_scores = np.array(all_scores)
    # Si score > seuil → prédit comme suspect (1), sinon normal (0)
    all_preds  = (all_scores > threshold).astype(int)
    # Les vraies étiquettes (1 = suspect, 0 = normal) — viennent de l'ETL
    all_true   = all_labels.astype(int)

    # ── Calcul des métriques ──────────────────────────────────────────────────
    # F1 = équilibre entre précision et rappel
    f1        = f1_score(all_true, all_preds, zero_division=0)
    # AUC-ROC = capacité à distinguer normal/suspect indépendamment du seuil
    auc       = roc_auc_score(all_true, all_scores)
    # Précision = parmi les alertes levées, combien sont vraiment suspectes ?
    precision = precision_score(all_true, all_preds, zero_division=0)
    # Rappel = parmi toutes les séquences suspectes, combien ont été détectées ?
    recall    = recall_score(all_true, all_preds, zero_division=0)

    log.info("── Métriques de détection ──────────────────")
    log.info("F1-score  : %.4f", f1)
    log.info("AUC-ROC   : %.4f", auc)
    log.info("Précision : %.4f  (sur les alertes levées, combien sont vraies)", precision)
    log.info("Rappel    : %.4f  (sur les vraies anomalies, combien détectées)", recall)
    log.info("\n%s", classification_report(all_true, all_preds,
             target_names=["Normal", "Anomalie"], zero_division=0))

    meta["f1_score"]  = f1
    meta["auc_roc"]   = auc
    meta["precision"] = precision
    meta["recall"]    = recall

    # Sauvegarde toutes les métriques + le seuil dans threshold.json
    with open(os.path.join(output_dir, "threshold.json"), "w") as f:
        json.dump(meta, f, indent=2)

    # ── Courbe de loss ────────────────────────────────────────
    _save_loss_curve(train_losses, val_losses, output_dir)

    # ── Matrice de confusion ──────────────────────────────────
    _save_confusion_matrix(all_true, all_preds, output_dir)

    # ── Résumé final ──────────────────────────────────────────
    log.info("")
    log.info("╔══════════════════════════════════════════╗")
    log.info("║           RÉSUMÉ ENTRAÎNEMENT            ║")
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Epochs effectuées  : %3d / %3d           ║", len(train_losses), EPOCHS)
    log.info("║  Meilleure val loss : %.6f             ║", best_val_loss)
    log.info("║  Seuil anomalie     : %.6f             ║", threshold)
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  F1-score           : %.4f               ║", f1)
    log.info("║  AUC-ROC            : %.4f               ║", auc)
    log.info("║  Précision          : %.4f               ║", precision)
    log.info("║  Rappel             : %.4f               ║", recall)
    log.info("╠══════════════════════════════════════════╣")
    log.info("║  Graphiques sauvegardés dans %s/   ║", output_dir)
    log.info("║    - loss_curve.png                      ║")
    log.info("║    - confusion_matrix.png                ║")
    log.info("╚══════════════════════════════════════════╝")

    return meta


def _save_loss_curve(train_losses: list, val_losses: list, output_dir: str):
    """Sauvegarde la courbe train loss vs val loss en PNG."""
    epochs = range(1, len(train_losses) + 1)
    plt.figure(figsize=(10, 5))
    plt.plot(epochs, train_losses, label="Train loss", linewidth=2)
    plt.plot(epochs, val_losses,   label="Val loss",   linewidth=2, linestyle="--")
    plt.xlabel("Epoch")
    plt.ylabel("MSE Loss")
    plt.title("Courbe d'apprentissage — LSTM Autoencoder")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    path = os.path.join(output_dir, "loss_curve.png")
    plt.savefig(path, dpi=150)
    plt.close()
    log.info("Courbe de loss sauvegardée : %s", path)


def _save_confusion_matrix(all_true: np.ndarray, all_preds: np.ndarray, output_dir: str):
    """Sauvegarde la matrice de confusion en PNG."""
    cm = confusion_matrix(all_true, all_preds)
    plt.figure(figsize=(6, 5))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=["Normal", "Anomalie"],
        yticklabels=["Normal", "Anomalie"],
    )
    plt.xlabel("Prédiction")
    plt.ylabel("Réalité")
    plt.title("Matrice de confusion")
    plt.tight_layout()
    path = os.path.join(output_dir, "confusion_matrix.png")
    plt.savefig(path, dpi=150)
    plt.close()
    log.info("Matrice de confusion sauvegardée : %s", path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="/tmp/etl-output-sample")
    parser.add_argument("--output", default="models")
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()
    train(args.input, args.output, args.device)
