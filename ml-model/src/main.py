"""
Entry point du service d'inférence (CMD Docker).

Boucle :
  Kafka logs-processed → buffer glissant → LSTM → si score > seuil → Kafka logs-anomalies-ml
"""
import logging

from config import (
    DEVICE,
    GCS_MODELS_BUCKET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_INPUT_TOPIC,
    KAFKA_OUTPUT_TOPIC,
    LOCAL_MODEL_MODE,
    MODEL_DIR,
    SEQUENCE_LENGTH,
)
from consumer import make_consumer
from inference.buffer import SlidingBuffer
from inference.detector import AnomalyDetector, download_models_from_gcs
from producer import make_producer, publish_anomaly

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ml-model")


def main():
    log.info("Démarrage du service ml-model")
    log.info("  Input  : %s → %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_INPUT_TOPIC)
    log.info("  Output : %s → %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_OUTPUT_TOPIC)
    log.info("  Device : %s | seq_len=%d", DEVICE, SEQUENCE_LENGTH)

    # Téléchargement des artefacts depuis S3 si nécessaire
    if not LOCAL_MODEL_MODE:
        download_models_from_gcs(GCS_MODELS_BUCKET, MODEL_DIR)

    detector = AnomalyDetector(model_dir=MODEL_DIR, device=DEVICE)
    buffer   = SlidingBuffer(seq_len=SEQUENCE_LENGTH)

    consumer = make_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_INPUT_TOPIC, KAFKA_GROUP_ID)
    producer = make_producer(KAFKA_BOOTSTRAP_SERVERS)

    log.info("En écoute sur %s...", KAFKA_INPUT_TOPIC)
    scored = 0
    detected = 0

    while True:
        for message in consumer:
            record   = message.value
            sequence = buffer.add(record)

            if sequence is None:
                continue  # buffer pas encore plein

            is_anom, score = detector.is_anomaly(sequence)
            scored += 1

            if is_anom:
                detected += 1
                publish_anomaly(
                    producer=producer,
                    topic=KAFKA_OUTPUT_TOPIC,
                    sequence=sequence,
                    score=score,
                    threshold=detector.threshold,
                    model_version=detector.model_version,
                )

            if scored % 1000 == 0:
                log.info("Séquences scorées : %d | anomalies détectées : %d", scored, detected)


if __name__ == "__main__":
    main()
