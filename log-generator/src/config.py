import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-raw")
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "1.0"))  # 0 = as fast as possible
DATA_DIR = os.getenv("DATA_DIR", "/data")
LOG_SOURCES = os.getenv("LOG_SOURCES", "linux,ssh,hadoop,spark,supercomputer,hdfs").split(",")
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_REGION = os.getenv("AWS_REGION", "eu-west-1")
