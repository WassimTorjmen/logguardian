import io
import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

# Parquet schema enforced on every write
PARQUET_SCHEMA = pa.schema([
    pa.field("timestamp",             pa.string()),
    pa.field("source",                pa.string()),
    pa.field("host",                  pa.string()),
    pa.field("level",                 pa.string()),
    pa.field("component",             pa.string()),
    pa.field("message",               pa.string()),
    pa.field("raw",                   pa.string()),
    pa.field("year",                  pa.int32()),
    pa.field("month",                 pa.int32()),
    pa.field("day",                   pa.int32()),
    pa.field("hour",                  pa.int32()),
    pa.field("is_anomaly_candidate",  pa.bool_()),
    pa.field("processed_at",          pa.string()),
])


def _build_s3_key(source: str, year: int, month: int, day: int, prefix: str) -> str:
    batch_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    return (
        f"{prefix}/"
        f"source={source}/"
        f"year={year}/month={month:02d}/day={day:02d}/"
        f"batch_{batch_id}.parquet"
    )


def _df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, safe=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def load_to_s3(
    df: pd.DataFrame,
    bucket: str,
    prefix: str,
    region: str,
) -> int:
    """
    Write one Parquet file per (source, year, month, day) partition to S3.
    Returns the number of files written.
    """
    if df.empty:
        return 0

    s3 = boto3.client("s3", region_name=region)
    files_written = 0

    for (source, year, month, day), group in df.groupby(["source", "year", "month", "day"]):
        key = _build_s3_key(source, year, month, day, prefix)
        data = _df_to_parquet_bytes(group.reset_index(drop=True))
        s3.put_object(Bucket=bucket, Key=key, Body=data)
        log.info("S3 upload: s3://%s/%s (%d rows)", bucket, key, len(group))
        files_written += 1

    return files_written


def load_to_local(
    df: pd.DataFrame,
    output_dir: str,
) -> int:
    """
    Write Parquet files locally (LOCAL_MODE=true).
    Same partitioning as S3 for easy migration.
    Returns the number of files written.
    """
    if df.empty:
        return 0

    files_written = 0

    for (source, year, month, day), group in df.groupby(["source", "year", "month", "day"]):
        partition_dir = os.path.join(
            output_dir,
            f"source={source}",
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}",
        )
        os.makedirs(partition_dir, exist_ok=True)
        batch_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        path = os.path.join(partition_dir, f"batch_{batch_id}.parquet")
        data = _df_to_parquet_bytes(group.reset_index(drop=True))
        with open(path, "wb") as f:
            f.write(data)
        log.info("Local write: %s (%d rows)", path, len(group))
        files_written += 1

    return files_written
