import json
import logging
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

log = logging.getLogger(__name__)


def make_producer(bootstrap_servers: str) -> KafkaProducer:
    for attempt in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8"),
                acks="all",
                retries=3,
            )
        except NoBrokersAvailable:
            log.warning("Kafka not ready for producer, retrying in 5s... (%d/10)", attempt + 1)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka producer after 10 attempts")


def publish_processed(
    producer: KafkaProducer,
    df: pd.DataFrame,
    topic: str,
) -> int:
    """
    Publish all enriched log records to logs-processed.
    Returns the number of messages sent.
    """
    if df.empty:
        return 0

    sent = 0
    for record in df.to_dict(orient="records"):
        payload = json.dumps(record)
        producer.send(topic, value=payload, key=record["source"].encode())
        sent += 1

    producer.flush()
    return sent


def publish_anomalies(
    producer: KafkaProducer,
    df: pd.DataFrame,
    topic: str,
) -> int:
    """
    Publish only anomaly candidates (level ERROR or FATAL) to logs-anomalies.
    Each message carries an extra field 'anomaly_published_at' for traceability.
    Returns the number of anomaly messages sent.
    """
    if df.empty:
        return 0

    anomalies = df[df["is_anomaly_candidate"]].copy()
    if anomalies.empty:
        return 0

    import datetime
    anomalies["anomaly_published_at"] = datetime.datetime.utcnow().isoformat()

    sent = 0
    for record in anomalies.to_dict(orient="records"):
        payload = json.dumps(record)
        # Key = level (ERROR ou FATAL) pour faciliter le partitionnement cote consumer
        producer.send(topic, value=payload, key=record["level"].encode())
        sent += 1

    producer.flush()
    log.info("Published %d anomaly candidates to topic '%s'", sent, topic)
    return sent
