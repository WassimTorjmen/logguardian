import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))

import pytest
from transformer import transform_batch


def _make_record(**kwargs):
    base = {
        "timestamp": "2015-10-17T15:37:56.547",
        "source": "hadoop",
        "host": "node-01",
        "level": "INFO",
        "component": "org.apache.hadoop.App",
        "message": "All systems nominal",
        "raw": "2015-10-17 15:37:56,547 INFO [main] org.apache.hadoop.App: All systems nominal",
    }
    base.update(kwargs)
    return base


def test_transform_adds_partition_columns():
    records = [_make_record()]
    df = transform_batch(records)
    assert not df.empty
    for col in ("year", "month", "day", "hour"):
        assert col in df.columns


def test_transform_adds_anomaly_flag_false_for_info():
    df = transform_batch([_make_record(level="INFO")])
    assert not df["is_anomaly_candidate"].iloc[0]


def test_transform_adds_anomaly_flag_true_for_error():
    df = transform_batch([_make_record(level="ERROR")])
    assert df["is_anomaly_candidate"].iloc[0]


def test_transform_adds_anomaly_flag_true_for_fatal():
    df = transform_batch([_make_record(level="FATAL")])
    assert df["is_anomaly_candidate"].iloc[0]


def test_transform_normalises_warning_to_warn():
    df = transform_batch([_make_record(level="WARNING")])
    assert df["level"].iloc[0] == "WARN"


def test_transform_adds_processed_at():
    df = transform_batch([_make_record()])
    assert "processed_at" in df.columns
    assert df["processed_at"].iloc[0] != ""


def test_transform_drops_invalid_records():
    bad = {"source": "linux", "message": "incomplete"}  # missing required fields
    good = _make_record()
    df = transform_batch([bad, good])
    assert len(df) == 1


def test_transform_empty_batch_returns_empty_df():
    df = transform_batch([])
    assert df.empty


def test_transform_all_fields_present():
    df = transform_batch([_make_record()])
    expected = {
        "timestamp", "source", "host", "level", "component",
        "message", "raw", "year", "month", "day", "hour",
        "is_anomaly_candidate", "processed_at",
    }
    assert expected.issubset(set(df.columns))


def test_transform_correct_partition_values():
    record = _make_record(timestamp="2015-10-17T15:37:56.547")
    df = transform_batch([record])
    assert df["year"].iloc[0] == 2015
    assert df["month"].iloc[0] == 10
    assert df["day"].iloc[0] == 17
    assert df["hour"].iloc[0] == 15
