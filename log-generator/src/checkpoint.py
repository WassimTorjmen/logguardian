import json
import logging

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)

_CHECKPOINT_KEY = "checkpoints/log-generator/checkpoint.json"


def load_checkpoint(bucket: str, region: str) -> dict:
    """
    Load checkpoint from S3.
    Returns dict with keys: cycle (int), offset (int within current cycle).
    Returns default {cycle: 0, offset: 0} if no checkpoint exists or S3 not configured.
    """
    if not bucket:
        log.info("No S3_BUCKET configured — checkpoint disabled, starting from beginning")
        return {"cycle": 0, "offset": 0}

    try:
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bucket, Key=_CHECKPOINT_KEY)
        data = json.loads(obj["Body"].read())
        log.info(
            "Checkpoint loaded: cycle=%d offset=%d (total_sent=%d)",
            data.get("cycle", 0),
            data.get("offset", 0),
            data.get("total_sent", 0),
        )
        return data
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            log.info("No checkpoint found in S3 — starting from beginning")
            return {"cycle": 0, "offset": 0}
        log.warning("Failed to load checkpoint: %s — starting from beginning", e)
        return {"cycle": 0, "offset": 0}
    except Exception as e:
        log.warning("Unexpected error loading checkpoint: %s — starting from beginning", e)
        return {"cycle": 0, "offset": 0}


def save_checkpoint(bucket: str, region: str, cycle: int, offset: int, total_sent: int) -> None:
    """
    Persist current position to S3.
    Called every 1000 messages and at graceful shutdown.
    """
    if not bucket:
        return

    try:
        s3 = boto3.client("s3", region_name=region)
        payload = json.dumps({"cycle": cycle, "offset": offset, "total_sent": total_sent})
        s3.put_object(
            Bucket=bucket,
            Key=_CHECKPOINT_KEY,
            Body=payload,
            ContentType="application/json",
        )
        log.debug("Checkpoint saved: cycle=%d offset=%d", cycle, offset)
    except Exception as e:
        log.warning("Failed to save checkpoint: %s", e)
