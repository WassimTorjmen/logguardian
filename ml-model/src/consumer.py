import json
import logging
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

log = logging.getLogger(__name__)


def make_consumer(bootstrap_servers: str, topic: str, group_id: str) -> KafkaConsumer:
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            log.info("Connecté à Kafka %s → topic %s", bootstrap_servers, topic)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka non disponible, retry %d/10...", attempt + 1)
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka après 10 tentatives")
