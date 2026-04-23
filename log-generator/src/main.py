import logging
import time
import itertools

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, REPLAY_SPEED, DATA_DIR, LOG_SOURCES
from parsers import ALL_PARSERS
from producer import make_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("log-generator")


def build_stream(data_dir: str, sources: list):
    parsers = [ALL_PARSERS[s]() for s in sources if s in ALL_PARSERS]
    if not parsers:
        raise ValueError(f"No valid sources in {sources}")

    generators = [p.parse(data_dir) for p in parsers]
    return itertools.chain.from_iterable(generators)


def main():
    log.info("Starting log-generator | sources=%s | topic=%s", LOG_SOURCES, KAFKA_TOPIC)
    producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)

    sent = 0
    interval = 1.0 / REPLAY_SPEED if REPLAY_SPEED > 0 else 0

    while True:
        stream = build_stream(DATA_DIR, LOG_SOURCES)
        for entry in stream:
            payload = entry.to_json()
            producer.send(KAFKA_TOPIC, value=payload, key=entry.source.encode())
            sent += 1
            if sent % 1000 == 0:
                log.info("Sent %d messages", sent)
            if interval > 0:
                time.sleep(interval)

        producer.flush()
        log.info("Completed one pass (%d total messages). Restarting...", sent)


if __name__ == "__main__":
    main()
