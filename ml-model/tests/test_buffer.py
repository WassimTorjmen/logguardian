import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

from inference.buffer import SlidingBuffer


def _make_record(source="ssh", host="LabSZ", msg="test"):
    return {"source": source, "host": host, "level": "INFO", "message": msg, "hour": 10}


def test_buffer_returns_none_until_full():
    buf = SlidingBuffer(seq_len=3)
    assert buf.add(_make_record()) is None
    assert buf.add(_make_record()) is None
    result = buf.add(_make_record())
    assert result is not None
    assert len(result) == 3


def test_buffer_slides():
    buf = SlidingBuffer(seq_len=3)
    for i in range(3):
        buf.add(_make_record(msg=f"msg{i}"))
    # 4ème log → nouvelle séquence [msg1, msg2, msg3]
    result = buf.add(_make_record(msg="msg3"))
    assert result is not None
    assert result[-1]["message"] == "msg3"


def test_buffer_separate_keys():
    buf = SlidingBuffer(seq_len=2)
    # Deux hosts différents → buffers indépendants
    buf.add(_make_record(host="hostA"))
    result_b = buf.add(_make_record(host="hostB"))
    assert result_b is None  # hostB n'a qu'1 log
    result_a = buf.add(_make_record(host="hostA"))
    assert result_a is not None  # hostA a 2 logs
    assert all(r["host"] == "hostA" for r in result_a)


def test_buffer_len():
    buf = SlidingBuffer(seq_len=5)
    buf.add(_make_record(host="A"))
    buf.add(_make_record(host="B"))
    assert len(buf) == 2
