import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

import numpy as np
from trainer.features import (
    N_FEATURES,
    build_embedding_table,
    build_vocabulary,
    log_to_vector,
    tokenize,
)


def _sample_record(**kwargs):
    base = {
        "source": "ssh",
        "level": "WARN",
        "hour": 14,
        "message": "Invalid user webmaster from 173.234.31.186",
    }
    base.update(kwargs)
    return base


def test_tokenize():
    tokens = tokenize("Invalid user webmaster from 173.234.31.186")
    assert "invalid" in tokens
    assert "user" in tokens
    assert "webmaster" in tokens


def test_build_vocabulary():
    messages = ["hello world", "hello kafka", "world is big"]
    vocab = build_vocabulary(messages)
    assert "hello" in vocab
    assert "world" in vocab
    assert isinstance(vocab["hello"], int)


def test_embedding_table_shape():
    table = build_embedding_table(vocab_size=100, embed_dim=32)
    assert table.shape == (101, 32)
    assert (table[0] == 0).all()  # index 0 = zéro


def test_log_to_vector_shape():
    vocab = build_vocabulary(["Invalid user webmaster"])
    table = build_embedding_table()
    vec = log_to_vector(_sample_record(), vocab, table)
    assert vec.shape == (N_FEATURES,), f"Expected ({N_FEATURES},), got {vec.shape}"
    assert vec.dtype == np.float32


def test_log_to_vector_unknown_source():
    vocab = build_vocabulary(["test"])
    table = build_embedding_table()
    vec = log_to_vector(_sample_record(source="unknown_source"), vocab, table)
    # one-hot source doit être tout à zéro
    assert vec[:6].sum() == 0.0


def test_log_to_vector_known_level():
    vocab = build_vocabulary(["test"])
    table = build_embedding_table()
    vec = log_to_vector(_sample_record(level="ERROR"), vocab, table)
    # one-hot level : ERROR est à l'index 3 dans LEVELS
    level_vec = vec[6:11]
    assert level_vec[3] == 1.0
    assert level_vec.sum() == 1.0


def test_hour_cyclic_continuity():
    vocab = build_vocabulary(["test"])
    table = build_embedding_table()
    vec_23 = log_to_vector(_sample_record(hour=23), vocab, table)
    vec_0  = log_to_vector(_sample_record(hour=0),  vocab, table)
    hour_23 = vec_23[11:13]
    hour_0  = vec_0[11:13]
    # sin/cos de 23h et 0h doivent être proches
    dist = np.linalg.norm(hour_23 - hour_0)
    assert dist < 0.3, f"Distance trop grande entre 23h et 0h : {dist:.4f}"
