import logging

from config import (
    AWS_REGION,
    BATCH_SIZE,
    BATCH_TIMEOUT_SEC,
    KAFKA_ANOMALY_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_INPUT_TOPIC,
    KAFKA_OUTPUT_TOPIC,
    LOCAL_MODE,
    LOCAL_OUTPUT_DIR,
    S3_BUCKET,
    S3_PREFIX,
)
from consumer import consume_batch, make_consumer
from producer import make_producer, publish_anomalies, publish_processed
from s3_loader import load_to_local, load_to_s3
from transformer import transform_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("etl-processor")


def main():
    log.info("Starting ETL Processor")
    log.info("  Input     : %s -> %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_INPUT_TOPIC)
    log.info("  Processed : %s -> %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_OUTPUT_TOPIC)
    log.info("  Anomalies : %s -> %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_ANOMALY_TOPIC)
    log.info("  Storage   : %s", "LOCAL:" + LOCAL_OUTPUT_DIR if LOCAL_MODE else "S3:" + S3_BUCKET)
    log.info("  Batch     : %d messages or %ds timeout", BATCH_SIZE, BATCH_TIMEOUT_SEC)

    kafka_consumer = make_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_INPUT_TOPIC, KAFKA_GROUP_ID)
    kafka_producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)

    total_records   = 0
    total_anomalies = 0
    total_files     = 0

    for raw_batch in consume_batch(kafka_consumer, BATCH_SIZE, BATCH_TIMEOUT_SEC):
        log.info("Received batch of %d raw records", len(raw_batch))

        # --- Transform ---
        df = transform_batch(raw_batch)
        if df.empty:
            log.warning("Batch produced empty DataFrame, skipping")
            continue

        n_anomalies = int(df["is_anomaly_candidate"].sum())
        log.info(
            "Transformed %d records | anomaly candidates: %d (%.1f%%)",
            len(df),
            n_anomalies,
            100 * n_anomalies / len(df),
        )

        # --- Load to storage (S3 ou local) ---
        if LOCAL_MODE:
            files = load_to_local(df, LOCAL_OUTPUT_DIR)
        else:
            files = load_to_s3(df, S3_BUCKET, S3_PREFIX, AWS_REGION)

        # --- Publish : tous les logs enrichis -> logs-processed ---
        sent_processed = publish_processed(kafka_producer, df, KAFKA_OUTPUT_TOPIC)

        # --- Publish : anomalies uniquement -> logs-anomalies ---
        sent_anomalies = publish_anomalies(kafka_producer, df, KAFKA_ANOMALY_TOPIC)

        total_records   += len(df)
        total_anomalies += sent_anomalies
        total_files     += files

        log.info(
            "Batch done | files: %d | processed: %d | anomalies: %d | "
            "total records: %d | total anomalies: %d",
            files,
            sent_processed,
            sent_anomalies,
            total_records,
            total_anomalies,
        )


if __name__ == "__main__":
    main()
