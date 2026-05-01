"""
Charge les fichiers Parquet produits par l'ETL et construit les séquences
glissantes par (source, host) pour le LSTM Autoencoder.
"""
import glob
import logging

import numpy as np
import pandas as pd
from torch.utils.data import Dataset

from .features import N_FEATURES, build_embedding_table, build_vocabulary, log_to_vector

log = logging.getLogger(__name__)


def load_parquet(parquet_dir: str) -> pd.DataFrame:
    files = glob.glob(f"{parquet_dir}/**/*.parquet", recursive=True)
    if not files:
        raise FileNotFoundError(f"Aucun fichier Parquet dans {parquet_dir}")
    log.info("Chargement de %d fichiers Parquet...", len(files))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    log.info("DataFrame chargé : %d lignes", len(df))
    return df


def build_sequences(
    df: pd.DataFrame,
    vectors: np.ndarray,
    seq_len: int,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Construit les séquences glissantes par (source, host).

    Retourne :
        sequences : (N, seq_len, n_features)  float32
        labels    : (N,)                       bool — True si ≥1 anomalie dans la fenêtre
    """
    seqs, labels = [], []

    df = df.copy()
    df["_vec_idx"] = range(len(df))

    for (source, host), group in df.groupby(["source", "host"]):
        group = group.sort_values("timestamp").reset_index(drop=True)
        idxs  = group["_vec_idx"].values
        flags = group["is_anomaly_candidate"].values

        if len(idxs) < seq_len:
            continue

        for i in range(len(idxs) - seq_len + 1):
            seqs.append(vectors[idxs[i : i + seq_len]])
            labels.append(flags[i : i + seq_len].any())

    return np.array(seqs, dtype=np.float32), np.array(labels, dtype=bool)


class LogSequenceDataset(Dataset):
    def __init__(self, sequences: np.ndarray):
        import torch
        self.data = torch.tensor(sequences, dtype=torch.float32)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx]


def prepare_datasets(
    parquet_dir: str,
    seq_len: int,
    val_ratio: float = 0.2,
) -> tuple:
    """
    Pipeline complet : Parquet → features → séquences → split train/val.

    Retourne :
        train_seqs, val_seqs  : np.ndarray (normales uniquement)
        all_seqs, all_labels  : np.ndarray (toutes)
        vocab, embedding_table, scaler
    """
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    df = load_parquet(parquet_dir)

    # Vocabulaire et table d'embeddings
    vocab           = build_vocabulary(df["message"].fillna("").tolist())
    embedding_table = build_embedding_table()

    # Vectorisation
    log.info("Vectorisation de %d logs...", len(df))
    records = df.to_dict(orient="records")
    vectors = np.stack([log_to_vector(r, vocab, embedding_table) for r in records])

    # Scaler fitté uniquement sur les données normales
    normal_mask = ~df["is_anomaly_candidate"].values
    scaler = StandardScaler()
    scaler.fit(vectors[normal_mask])
    vectors_scaled = scaler.transform(vectors)

    # Séquences
    log.info("Construction des séquences glissantes (seq_len=%d)...", seq_len)
    all_seqs, all_labels = build_sequences(df, vectors_scaled, seq_len)
    log.info("Séquences : %d total | %d anomalies", len(all_seqs), all_labels.sum())

    # Split train/val sur les séquences normales uniquement
    normal_seqs = all_seqs[~all_labels]
    train_seqs, val_seqs = train_test_split(normal_seqs, test_size=val_ratio, random_state=42)
    log.info("Train: %d | Val: %d", len(train_seqs), len(val_seqs))

    return train_seqs, val_seqs, all_seqs, all_labels, vocab, embedding_table, scaler
