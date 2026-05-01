FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app/src
ENV MODEL_DIR=/app/models
ENV LOCAL_MODEL_MODE=true
ENV DEVICE=cpu

# Training (manuel) : docker run ... python -m trainer.train --input /data --output /app/models
# Inference (défaut) : docker run ...
CMD ["python", "src/main.py"]
