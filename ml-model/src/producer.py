import json
import logging
import time
from datetime import datetime, timezone

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
            log.warning("Kafka non disponible pour le producer, retry %d/10...", attempt + 1)
            time.sleep(5)
    raise RuntimeError("Impossible de connecter le producer Kafka après 10 tentatives")


def publish_anomaly(
    producer: KafkaProducer,
    topic: str,
    sequence: list[dict],
    score: float,
    threshold: float,
    model_version: str,
):
    payload = {
        "detected_at":    datetime.now(tz=timezone.utc).isoformat(),
        "source":         sequence[-1].get("source", ""),
        "host":           sequence[-1].get("host", ""),
        "anomaly_score":  round(score, 6),
        "threshold":      round(threshold, 6),
        "severity_ratio": round(score / threshold, 3) if threshold > 0 else 0,
        "sequence":       [
            {
                "timestamp": r.get("timestamp", ""),
                "level":     r.get("level", ""),
                "message":   r.get("message", ""),
            }
            for r in sequence
        ],
        "model_version": model_version,
    }
    key = sequence[-1].get("source", "unknown").encode()
    producer.send(topic, value=json.dumps(payload), key=key)
    log.info(
        "Anomalie publiée | host=%s | score=%.4f | ratio=%.2fx",
        payload["host"], score, payload["severity_ratio"],
    )
