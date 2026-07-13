"""
Charge les fichiers Parquet produits par l'ETL et construit les séquences
glissantes par (source, host) pour le LSTM Autoencoder.

RÉSUMÉ : Ce fichier fait deux choses :
  1. Charger les données (fichiers Parquet) dans un tableau
  2. Construire des séquences glissantes de 10 logs consécutifs
"""
import glob
import logging

import numpy as np
import pandas as pd
from torch.utils.data import Dataset

# On importe les fonctions de vectorisation depuis features.py
from .features import N_FEATURES, build_embedding_table, build_vocabulary, log_to_vector

log = logging.getLogger(__name__)


def load_parquet(parquet_dir: str) -> pd.DataFrame:
    # Trouve tous les fichiers .parquet dans le dossier et ses sous-dossiers
    files = glob.glob(f"{parquet_dir}/**/*.parquet", recursive=True)
    if not files:
        raise FileNotFoundError(f"Aucun fichier Parquet dans {parquet_dir}")
    log.info("Chargement de %d fichiers Parquet...", len(files))
    # Charge tous les fichiers et les colle en un seul grand tableau
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

    Principe de la fenêtre glissante :
      Log1, Log2, Log3 ... Log10  → Séquence 1
      Log2, Log3, Log4 ... Log11  → Séquence 2
      Log3, Log4, Log5 ... Log12  → Séquence 3

    Retourne :
        sequences : (N, seq_len, n_features)  float32
        labels    : (N,) bool — True si la séquence contient au moins 1 anomalie
    """
    seqs, labels = [], []

    df = df.copy()
    # Ajoute une colonne pour retrouver l'index du vecteur correspondant à chaque log
    df["_vec_idx"] = range(len(df))

    # Groupe les logs par (source, host) — on ne mélange pas les logs de machines différentes
    for (source, host), group in df.groupby(["source", "host"]):
        # Trie les logs par ordre chronologique
        group = group.sort_values("timestamp").reset_index(drop=True)
        idxs  = group["_vec_idx"].values
        flags = group["is_anomaly_candidate"].values

        # Si le groupe a moins de 10 logs, on ne peut pas faire de séquence → on passe
        if len(idxs) < seq_len:
            continue

        # Fenêtre glissante : on avance d'un log à chaque fois
        for i in range(len(idxs) - seq_len + 1):
            seqs.append(vectors[idxs[i : i + seq_len]])
            # La séquence est anormale si au moins 1 log dans la fenêtre est anormal
            labels.append(flags[i : i + seq_len].any())

    return np.array(seqs, dtype=np.float32), np.array(labels, dtype=bool)


class LogSequenceDataset(Dataset):
    # Classe PyTorch qui emballe les séquences pour les donner au modèle pendant l'entraînement
    def __init__(self, sequences: np.ndarray):
        import torch
        self.data = torch.tensor(sequences, dtype=torch.float32)

    def __len__(self):
        # Retourne le nombre total de séquences
        return len(self.data)

    def __getitem__(self, idx):
        # Retourne une séquence par son index — utilisé par le DataLoader
        return self.data[idx]


def prepare_datasets(
    parquet_dir: str,
    seq_len: int,
    val_ratio: float = 0.2,
    max_rows: int = 500_000,
) -> tuple:
    """
    FONCTION PRINCIPALE — Pipeline complet :
    Parquet → features → séquences → split train/val

    Retourne :
        train_seqs, val_seqs  : séquences normales uniquement (pour entraîner)
        all_seqs, all_labels  : toutes les séquences (pour évaluer à la fin)
        vocab, embedding_table, scaler
    """
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    df = load_parquet(parquet_dir)

    # Limite le nombre de lignes pour éviter les problèmes mémoire sur petites machines
    if max_rows and len(df) > max_rows:
        log.info("Limitation à %d lignes (sur %d disponibles)", max_rows, len(df))
        df = df.sample(n=max_rows, random_state=42).reset_index(drop=True)

    # Construit le vocabulaire des 5000 mots les plus fréquents
    vocab           = build_vocabulary(df["message"].fillna("").tolist())
    # Crée la table d'embeddings (vecteurs aléatoires pour chaque mot)
    embedding_table = build_embedding_table()

    # Transforme chaque log en vecteur de 77 nombres
    log.info("Vectorisation de %d logs...", len(df))
    records = df.to_dict(orient="records")
    vectors = np.stack([log_to_vector(r, vocab, embedding_table) for r in records])

    # Normalisation : on centre et réduit les données
    # IMPORTANT : le scaler est fitté uniquement sur les logs normaux
    # pour ne pas que les anomalies influencent la normalisation
    normal_mask = ~df["is_anomaly_candidate"].values
    scaler = StandardScaler()
    scaler.fit(vectors[normal_mask])
    vectors_scaled = scaler.transform(vectors)

    # Construit les séquences glissantes de 10 logs
    log.info("Construction des séquences glissantes (seq_len=%d)...", seq_len)
    all_seqs, all_labels = build_sequences(df, vectors_scaled, seq_len)
    log.info("Séquences : %d total | %d anomalies", len(all_seqs), all_labels.sum())

    # FILTRE CLÉ : on garde uniquement les séquences normales pour l'entraînement
    # Les séquences anormales sont exclues — le modèle ne doit jamais les voir pendant l'entraînement
    normal_seqs = all_seqs[~all_labels]
    # Split 80% entraînement / 20% validation
    train_seqs, val_seqs = train_test_split(normal_seqs, test_size=val_ratio, random_state=42)
    log.info("Train: %d | Val: %d", len(train_seqs), len(val_seqs))

    return train_seqs, val_seqs, all_seqs, all_labels, vocab, embedding_table, scaler
