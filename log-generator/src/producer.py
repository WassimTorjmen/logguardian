import time
import logging
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
            log.warning("Kafka not ready, retrying in %ds... (%d/10)", 5, attempt + 1)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")
