"""
Feature extraction : transforme un log dict en vecteur numpy de taille fixe.

Vecteur = [source one-hot (6)] + [level one-hot (5)] + [hour sin/cos (2)] + [message embedding (64)]
          = 77 dimensions
"""
import re
from collections import Counter

import numpy as np

SOURCES = ["linux", "ssh", "hadoop", "spark", "supercomputer", "hdfs"]
LEVELS  = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

VOCAB_SIZE = 5000
EMBED_DIM  = 64
N_FEATURES = len(SOURCES) + len(LEVELS) + 2 + EMBED_DIM  # 77


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def build_vocabulary(messages: list[str]) -> dict[str, int]:
    counter = Counter()
    for msg in messages:
        counter.update(tokenize(msg))
    # index 0 = token inconnu, index 1..VOCAB_SIZE = tokens connus
    return {tok: idx + 1 for idx, (tok, _) in enumerate(counter.most_common(VOCAB_SIZE))}


def build_embedding_table(vocab_size: int = VOCAB_SIZE, embed_dim: int = EMBED_DIM) -> np.ndarray:
    rng = np.random.default_rng(42)
    table = rng.standard_normal((vocab_size + 1, embed_dim)).astype(np.float32) * 0.1
    table[0] = 0.0  # index 0 → vecteur zéro (tokens inconnus)
    return table


def _one_hot(value: str, categories: list[str]) -> np.ndarray:
    vec = np.zeros(len(categories), dtype=np.float32)
    if value in categories:
        vec[categories.index(value)] = 1.0
    return vec


def _hour_cyclic(hour: int) -> np.ndarray:
    # sin/cos pour éviter la discontinuité 23h → 0h
    angle = 2 * np.pi * int(hour) / 24
    return np.array([np.sin(angle), np.cos(angle)], dtype=np.float32)


def _message_embedding(text: str, vocab: dict[str, int], embedding_table: np.ndarray) -> np.ndarray:
    tokens = tokenize(text)
    if not tokens:
        return np.zeros(EMBED_DIM, dtype=np.float32)
    indices = [vocab.get(t, 0) for t in tokens]
    return embedding_table[indices].mean(axis=0)


def log_to_vector(
    record: dict,
    vocab: dict[str, int],
    embedding_table: np.ndarray,
) -> np.ndarray:
    src = _one_hot(record.get("source", ""), SOURCES)
    lvl = _one_hot(record.get("level",  ""), LEVELS)
    hr  = _hour_cyclic(record.get("hour", 0))
    msg = _message_embedding(str(record.get("message", "")), vocab, embedding_table)
    return np.concatenate([src, lvl, hr, msg])  # (77,)
