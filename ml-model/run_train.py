import sys
import logging

sys.path.insert(0, "/app/src")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)

from trainer.train import train

train("/tmp/etl-output", "/app/models", "cpu")
