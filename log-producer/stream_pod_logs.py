"""
Streaming des logs Kubernetes vers Kafka LogGuardian.

Lance un `kubectl logs -f` sur chaque pod du namespace choisi,
et pousse chaque ligne dans le topic logs-raw en temps réel.

Usage :
  python stream_pod_logs.py --namespace default
  python stream_pod_logs.py --namespace logguardian --exclude kafka,zookeeper,log-generator,etl-processor,ml-model,monitoring-ui,email-sender
  python stream_pod_logs.py --namespace default --source k8s-prod
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-raw")


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}", file=sys.stderr)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 100,
        "acks": "1",
    })


def _guess_level(line: str) -> str:
    upper = line.upper()
    if "FATAL" in upper or "CRITICAL" in upper or "PANIC" in upper:
        return "FATAL"
    if "ERROR" in upper or "EXCEPTION" in upper or "TRACEBACK" in upper:
        return "ERROR"
    if "WARN" in upper:
        return "WARN"
    if "DEBUG" in upper:
        return "DEBUG"
    return "INFO"


def list_pods(namespace: str, exclude: list[str]) -> list[str]:
    """Retourne la liste des pods du namespace, sauf ceux excluis."""
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "json"],
            capture_output=True, text=True, check=True,
        )
        data = json.loads(result.stdout)
        pods = []
        for item in data.get("items", []):
            name = item["metadata"]["name"]
            phase = item.get("status", {}).get("phase", "")
            if phase != "Running":
                continue
            if any(x in name for x in exclude):
                continue
            pods.append(name)
        return pods
    except subprocess.CalledProcessError as e:
        print(f"[FATAL] kubectl error: {e.stderr}", file=sys.stderr)
        return []


def stream_pod(producer: Producer, namespace: str, pod: str, source: str, since_seconds: int):
    """Lance kubectl logs -f et pousse chaque ligne à Kafka."""
    cmd = [
        "kubectl", "logs", "-f",
        "--tail", "10",
        "--since", f"{since_seconds}s",
        "-n", namespace, pod,
    ]
    print(f"[START] Streaming {namespace}/{pod}")

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
    except FileNotFoundError:
        print(f"[FATAL] kubectl not found", file=sys.stderr)
        return

    try:
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue

            payload = {
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                "source": source,
                "host": pod,
                "level": _guess_level(line),
                "component": namespace,
                "message": line[:2000],
                "raw": line[:2000],
            }
            producer.produce(
                KAFKA_TOPIC,
                json.dumps(payload).encode("utf-8"),
                on_delivery=_delivery_report,
            )
            producer.poll(0)
    except Exception as e:
        print(f"[WARN] Stream {pod} stopped: {e}", file=sys.stderr)
    finally:
        proc.terminate()
        print(f"[STOP] {namespace}/{pod}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--namespace", required=True, help="Namespace Kubernetes à monitorer")
    parser.add_argument("--exclude", default="", help="Substrings de noms de pods à exclure (séparés par virgule)")
    parser.add_argument("--source", default="k8s", help="Nom de la source LogGuardian (défaut: k8s)")
    parser.add_argument("--since", type=int, default=5, help="Nombre de secondes de logs passés à récupérer (défaut: 5)")
    parser.add_argument("--refresh", type=int, default=30, help="Intervalle de scan pour nouveaux pods (défaut: 30s)")
    args = parser.parse_args()

    exclude = [e.strip() for e in args.exclude.split(",") if e.strip()]

    producer = build_producer()
    tracked: dict[str, threading.Thread] = {}

    print(f"Monitoring namespace='{args.namespace}' | source='{args.source}' | exclude={exclude}")
    print(f"Broker: {KAFKA_BOOTSTRAP_SERVERS}\n")

    try:
        while True:
            pods = list_pods(args.namespace, exclude)

            for pod in pods:
                if pod not in tracked or not tracked[pod].is_alive():
                    t = threading.Thread(
                        target=stream_pod,
                        args=(producer, args.namespace, pod, args.source, args.since),
                        daemon=True,
                    )
                    t.start()
                    tracked[pod] = t

            time.sleep(args.refresh)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
