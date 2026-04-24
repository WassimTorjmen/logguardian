import json
import logging
import time
from typing import Iterator
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
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000,
            )
            log.info("Connected to Kafka broker at %s", bootstrap_servers)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready, retrying in 5s... (%d/10)", attempt + 1)
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


def consume_batch(
    consumer: KafkaConsumer,
    batch_size: int,
    timeout_sec: int,
) -> Iterator[list[dict]]:
    """
    Yields batches of raw log records.
    A batch is flushed when either:
      - batch_size messages have been collected, OR
      - timeout_sec seconds have elapsed since the last flush
    """
    batch = []
    deadline = time.monotonic() + timeout_sec

    while True:
        try:
            for message in consumer:
                batch.append(message.value)
                if len(batch) >= batch_size:
                    yield batch
                    consumer.commit()
                    batch = []
                    deadline = time.monotonic() + timeout_sec

                if time.monotonic() >= deadline:
                    break
        except StopIteration:
            pass

        # Flush on timeout even if batch is not full
        if batch and time.monotonic() >= deadline:
            yield batch
            consumer.commit()
            batch = []

        deadline = time.monotonic() + timeout_sec
