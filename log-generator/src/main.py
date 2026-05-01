import logging
import signal
import time
import itertools

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, REPLAY_SPEED,
    DATA_DIR, LOG_SOURCES, S3_BUCKET, AWS_REGION,
)
from parsers import ALL_PARSERS
from producer import make_producer
from checkpoint import load_checkpoint, save_checkpoint

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("log-generator")

# Save checkpoint every N messages
CHECKPOINT_INTERVAL = 1000

# Graceful shutdown flag
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received (%s) — will stop after current message", signum)
    _shutdown = True


def build_stream(data_dir: str, sources: list):
    parsers = [ALL_PARSERS[s]() for s in sources if s in ALL_PARSERS]
    if not parsers:
        raise ValueError(f"No valid sources in {sources}")
    generators = [p.parse(data_dir) for p in parsers]
    return itertools.chain.from_iterable(generators)


def main():
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    log.info(
        "Starting log-generator | sources=%s | topic=%s | checkpoint_bucket=%s",
        LOG_SOURCES, KAFKA_TOPIC, S3_BUCKET or "disabled",
    )

    producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)
    interval = 1.0 / REPLAY_SPEED if REPLAY_SPEED > 0 else 0

    # Load checkpoint (cycle + offset within current cycle)
    ckpt = load_checkpoint(S3_BUCKET, AWS_REGION)
    cycle = ckpt.get("cycle", 0)
    offset = ckpt.get("offset", 0)
    total_sent = ckpt.get("total_sent", 0)

    while not _shutdown:
        stream = build_stream(DATA_DIR, LOG_SOURCES)
        cycle_sent = 0  # messages sent in this cycle

        # Skip already-processed entries from checkpoint (fast, no sleep)
        if offset > 0:
            log.info("Skipping %d already-processed entries (cycle %d)...", offset, cycle)
            for _ in itertools.islice(stream, offset):
                pass
            cycle_sent = offset
            log.info("Resumed at cycle=%d offset=%d", cycle, offset)
            offset = 0  # only skip on first iteration after restart

        for entry in stream:
            if _shutdown:
                break

            payload = entry.to_json()
            producer.send(KAFKA_TOPIC, value=payload, key=entry.source.encode())

            cycle_sent += 1
            total_sent += 1

            if total_sent % CHECKPOINT_INTERVAL == 0:
                log.info("Sent %d messages (cycle=%d offset=%d)", total_sent, cycle, cycle_sent)
                save_checkpoint(S3_BUCKET, AWS_REGION, cycle, cycle_sent, total_sent)

            if interval > 0:
                time.sleep(interval)

        if not _shutdown:
            producer.flush()
            log.info("Completed cycle %d (%d messages). Starting cycle %d...", cycle, cycle_sent, cycle + 1)
            # Reset offset for next cycle
            cycle += 1
            save_checkpoint(S3_BUCKET, AWS_REGION, cycle, 0, total_sent)

    # Graceful shutdown: flush and save final checkpoint
    log.info("Shutting down — flushing producer and saving checkpoint...")
    producer.flush()
    save_checkpoint(S3_BUCKET, AWS_REGION, cycle, cycle_sent, total_sent)
    log.info("Checkpoint saved at cycle=%d offset=%d total_sent=%d. Goodbye.", cycle, cycle_sent, total_sent)


if __name__ == "__main__":
    main()
