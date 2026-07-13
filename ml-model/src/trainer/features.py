"""
Feature extraction : transforme un log dict en vecteur numpy de taille fixe.

Vecteur = [source one-hot (6)] + [level one-hot (5)] + [hour sin/cos (2)] + [message embedding (64)]
          = 77 dimensions

RÉSUMÉ : Le modèle ne comprend pas les mots, seulement des nombres.
Ce fichier traduit chaque log (texte) en un vecteur de 77 nombres.
"""
import re
from collections import Counter

import numpy as np

# Les 6 sources de logs possibles dans notre système
SOURCES = ["linux", "ssh", "hadoop", "spark", "supercomputer", "hdfs"]

# Les 5 niveaux de sévérité possibles
LEVELS  = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

# On retient les 5000 mots les plus fréquents dans les messages
VOCAB_SIZE = 5000

# Chaque mot est représenté par un vecteur de 64 nombres
EMBED_DIM  = 64

# Taille totale du vecteur final : 6 + 5 + 2 + 64 = 77
N_FEATURES = len(SOURCES) + len(LEVELS) + 2 + EMBED_DIM  # 77


def tokenize(text: str) -> list[str]:
    # Découpe le texte en mots simples en minuscules, ignore la ponctuation
    # Exemple : "Failed password for ROOT" → ["failed", "password", "for", "root"]
    return re.findall(r"[a-z0-9]+", text.lower())


def build_vocabulary(messages: list[str]) -> dict[str, int]:
    # Compte combien de fois chaque mot apparaît dans tous les messages
    counter = Counter()
    for msg in messages:
        counter.update(tokenize(msg))
    # Garde les 5000 mots les plus fréquents et leur attribue un numéro
    # index 0 = mot inconnu, index 1..5000 = mots connus
    return {tok: idx + 1 for idx, (tok, _) in enumerate(counter.most_common(VOCAB_SIZE))}


def build_embedding_table(vocab_size: int = VOCAB_SIZE, embed_dim: int = EMBED_DIM) -> np.ndarray:
    # Crée une table de 5001 lignes x 64 colonnes
    # Chaque ligne = un vecteur de 64 nombres représentant un mot
    # Les valeurs sont aléatoires (seed=42 pour avoir toujours les mêmes résultats)
    rng = np.random.default_rng(42)
    table = rng.standard_normal((vocab_size + 1, embed_dim)).astype(np.float32) * 0.1
    # La ligne 0 = que des zéros → pour les mots inconnus (pas dans le vocabulaire)
    table[0] = 0.0
    return table


def _one_hot(value: str, categories: list[str]) -> np.ndarray:
    # Crée un vecteur de 0 avec un seul 1 à la position de la catégorie
    # Exemple : _one_hot("ERROR", LEVELS) → [0, 0, 0, 1, 0]
    # Si la valeur n'existe pas dans la liste → que des zéros
    vec = np.zeros(len(categories), dtype=np.float32)
    if value in categories:
        vec[categories.index(value)] = 1.0
    return vec


def _hour_cyclic(hour: int) -> np.ndarray:
    # Encode l'heure avec sin et cos pour éviter la discontinuité 23h → 0h
    # Sans ça : 23 et 0 sont loin numériquement mais proches dans la réalité
    # Avec sin/cos : 23h et 0h donnent des valeurs proches → le modèle comprend la cyclicité
    angle = 2 * np.pi * int(hour) / 24
    return np.array([np.sin(angle), np.cos(angle)], dtype=np.float32)


def _message_embedding(text: str, vocab: dict[str, int], embedding_table: np.ndarray) -> np.ndarray:
    # Découpe le message en mots
    tokens = tokenize(text)
    # Si le message est vide → vecteur de zéros
    if not tokens:
        return np.zeros(EMBED_DIM, dtype=np.float32)
    # Pour chaque mot, récupère son numéro dans le vocabulaire (0 si inconnu)
    indices = [vocab.get(t, 0) for t in tokens]
    # Retourne la moyenne des vecteurs de tous les mots → 1 vecteur de 64 dims
    return embedding_table[indices].mean(axis=0)


def log_to_vector(
    record: dict,
    vocab: dict[str, int],
    embedding_table: np.ndarray,
) -> np.ndarray:
    # FONCTION PRINCIPALE : transforme un log complet en vecteur de 77 nombres
    # record = dictionnaire Python contenant un log (source, level, hour, message)

    src = _one_hot(record.get("source", ""), SOURCES)                          # 6 dims
    lvl = _one_hot(record.get("level",  ""), LEVELS)                           # 5 dims
    hr  = _hour_cyclic(record.get("hour", 0))                                  # 2 dims
    msg = _message_embedding(str(record.get("message", "")), vocab, embedding_table)  # 64 dims

    # Colle les 4 parties bout à bout → vecteur final de 77 dimensions
    return np.concatenate([src, lvl, hr, msg])  # (77,)
