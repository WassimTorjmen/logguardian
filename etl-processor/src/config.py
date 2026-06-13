import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "logs-raw")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "logs-processed")
KAFKA_ANOMALY_TOPIC = os.getenv("KAFKA_ANOMALY_TOPIC", "logs-anomalies")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "etl-processor")

# Batching
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
BATCH_TIMEOUT_SEC = int(os.getenv("BATCH_TIMEOUT_SEC", "30"))

# GCS
GCS_BUCKET = os.getenv("GCS_BUCKET", "logguardian-datalake-logguardian-497218")
GCS_PREFIX = os.getenv("GCS_PREFIX", "logs")

# Mode local : si True, ecrit les Parquet en local au lieu de S3
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "/tmp/etl-output")
