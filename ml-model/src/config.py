import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC        = os.getenv("KAFKA_INPUT_TOPIC",  "logs-processed")
KAFKA_OUTPUT_TOPIC       = os.getenv("KAFKA_OUTPUT_TOPIC", "logs-anomalies-ml")
KAFKA_GROUP_ID           = os.getenv("KAFKA_GROUP_ID",     "ml-model")

MODEL_DIR         = os.getenv("MODEL_DIR",     "/app/models")
S3_MODELS_BUCKET  = os.getenv("S3_MODELS_BUCKET", "logguardian-models-148761640356")
AWS_REGION        = os.getenv("AWS_REGION",    "eu-west-1")
LOCAL_MODEL_MODE  = os.getenv("LOCAL_MODEL_MODE", "true").lower() == "true"

SEQUENCE_LENGTH = int(os.getenv("SEQUENCE_LENGTH", "10"))
DEVICE          = os.getenv("DEVICE", "cpu")
