import logging
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

REQUIRED_FIELDS = {"timestamp", "source", "host", "level", "component", "message", "raw"}
VALID_LEVELS = {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"}
ANOMALY_LEVELS = {"ERROR", "FATAL"}


def _parse_timestamp(ts_str: str) -> datetime:
    """Try multiple ISO 8601 variants, fall back to UTC now."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            return datetime.strptime(ts_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(tz=timezone.utc)


def _validate(record: dict) -> bool:
    """Return True if the record has all required fields and a known source."""
    missing = REQUIRED_FIELDS - record.keys()
    if missing:
        log.debug("Dropping record missing fields: %s", missing)
        return False
    if not record.get("source"):
        return False
    return True


def transform_batch(records: list[dict]) -> pd.DataFrame:
    """
    Validate, enrich and normalise a batch of raw log records.

    Steps:
      1. Drop invalid records (missing fields)
      2. Parse and normalise timestamp to UTC
      3. Normalise level (WARNING -> WARN)
      4. Add partition columns: year, month, day, hour
      5. Add is_anomaly_candidate flag
      6. Add processed_at timestamp
    """
    valid = [r for r in records if _validate(r)]
    dropped = len(records) - len(valid)
    if dropped:
        log.warning("Dropped %d invalid records out of %d", dropped, len(records))

    if not valid:
        return pd.DataFrame()

    df = pd.DataFrame(valid)

    # --- Timestamp normalisation ---
    df["_dt"] = df["timestamp"].apply(_parse_timestamp)
    df["timestamp"] = df["_dt"].apply(lambda d: d.isoformat())

    # --- Partition columns ---
    df["year"]  = df["_dt"].apply(lambda d: d.year)
    df["month"] = df["_dt"].apply(lambda d: d.month)
    df["day"]   = df["_dt"].apply(lambda d: d.day)
    df["hour"]  = df["_dt"].apply(lambda d: d.hour)

    # --- Level normalisation ---
    df["level"] = df["level"].str.upper().replace({"WARNING": "WARN"})

    # --- Anomaly candidate flag ---
    df["is_anomaly_candidate"] = df["level"].isin(ANOMALY_LEVELS)

    # --- Processing metadata ---
    df["processed_at"] = datetime.now(tz=timezone.utc).isoformat()

    # Drop internal helper column
    df.drop(columns=["_dt"], inplace=True)

    return df
