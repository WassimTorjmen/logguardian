"""
Auto-remédiation LogGuardian.

Consomme le topic logs-anomalies-ml en continu, et applique des actions
correctives sur les pods du namespace surveillé selon le severity_ratio :

  - ratio >= CRITICAL_THRESHOLD  → delete pod (K8s recrée automatiquement)
  - ratio >= WARN_THRESHOLD      → log seulement
  - Cooldown par pod pour éviter le flapping
  - Liste de protection : les pods critiques du pipeline sont intouchables
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime

from confluent_kafka import Consumer
from kubernetes import client, config
from kubernetes.client.rest import ApiException


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("auto-remediator")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka.logguardian.svc.cluster.local:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
GROUP_ID                = os.getenv("KAFKA_GROUP_ID", "auto-remediator")

NAMESPACE_TO_WATCH      = os.getenv("NAMESPACE_TO_WATCH", "demo-apps")
CRITICAL_THRESHOLD      = float(os.getenv("CRITICAL_THRESHOLD", "2.0"))
WARN_THRESHOLD          = float(os.getenv("WARN_THRESHOLD", "1.3"))
COOLDOWN_SECONDS        = int(os.getenv("COOLDOWN_SECONDS", "180"))
DRY_RUN                 = os.getenv("DRY_RUN", "false").lower() == "true"

# Pods du pipeline LogGuardian et infra : jamais supprimés
PROTECTED_SUBSTRINGS = [
    "log-generator", "etl-processor", "ml-model", "monitoring-ui",
    "email-sender", "auto-remediator", "kafka", "zookeeper",
]


def is_protected(pod_name: str) -> bool:
    return any(p in pod_name for p in PROTECTED_SUBSTRINGS)


def load_k8s_config():
    """
    En cluster : utilise le ServiceAccount du pod (in-cluster config).
    En local (dev) : utilise le kubeconfig usuel.
    """
    try:
        config.load_incluster_config()
        log.info("Kubernetes in-cluster config chargée (ServiceAccount)")
    except config.ConfigException:
        config.load_kube_config()
        log.info("Kubernetes kubeconfig local chargé (dev)")


_pod_cache: dict[str, float] = {}
_pod_cache_ts: float = 0.0
_POD_CACHE_TTL = 15.0


def list_pods_cached(api: client.CoreV1Api, namespace: str) -> set[str]:
    """Liste les pods réels du namespace, avec cache TTL 15s."""
    global _pod_cache, _pod_cache_ts
    now = time.time()
    if now - _pod_cache_ts < _POD_CACHE_TTL and _pod_cache:
        return set(_pod_cache.keys())

    try:
        pods = api.list_namespaced_pod(namespace=namespace, timeout_seconds=5)
        names = {p.metadata.name for p in pods.items}
        _pod_cache = {n: now for n in names}
        _pod_cache_ts = now
        return names
    except ApiException as e:
        log.error("Erreur list_namespaced_pod: %s", e.reason)
        return set()


def restart_pod(api: client.CoreV1Api, pod: str, namespace: str) -> bool:
    """
    Supprime le pod → le Deployment le recrée automatiquement.
    Retourne True si la suppression a réussi.
    """
    if DRY_RUN:
        log.info("  [DRY-RUN] delete_namespaced_pod(name=%s, ns=%s)", pod, namespace)
        return True

    try:
        api.delete_namespaced_pod(
            name=pod,
            namespace=namespace,
            grace_period_seconds=0,
        )
        log.info("  ✓ Pod supprimé (K8s va le recréer)")
        return True
    except ApiException as e:
        if e.status == 404:
            log.warning("  ⊘ Pod déjà disparu (404)")
            return False
        log.error("  ✗ Erreur K8s API (%d) : %s", e.status, e.reason)
        return False


def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           GROUP_ID,
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })


def main():
    log.info("=" * 60)
    log.info("Auto-remediator starting")
    log.info("  broker     : %s", KAFKA_BOOTSTRAP_SERVERS)
    log.info("  topic      : %s", KAFKA_TOPIC)
    log.info("  namespace  : %s", NAMESPACE_TO_WATCH)
    log.info("  critical   : severity_ratio >= %.2f (delete pod)", CRITICAL_THRESHOLD)
    log.info("  warn       : severity_ratio >= %.2f (log only)", WARN_THRESHOLD)
    log.info("  cooldown   : %ds per pod", COOLDOWN_SECONDS)
    log.info("  dry-run    : %s", DRY_RUN)
    log.info("=" * 60)

    load_k8s_config()
    api = client.CoreV1Api()

    consumer = build_consumer()
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka consumer prêt sur '%s'", KAFKA_TOPIC)

    last_action: dict[str, float] = {}
    stats = {
        "seen": 0, "warn": 0, "critical": 0,
        "restarted": 0, "skipped_cooldown": 0, "skipped_protected": 0,
        "skipped_not_in_ns": 0,
    }
    last_stat_log = time.time()

    while True:
        msg = consumer.poll(1.0)

        # Log périodique des stats (toutes les 5 min)
        if time.time() - last_stat_log > 300:
            log.info("Stats: %s", stats)
            last_stat_log = time.time()

        if not msg or msg.error():
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        stats["seen"] += 1
        host   = str(event.get("host", "")).strip()
        source = str(event.get("source", "")).strip()
        ratio  = float(event.get("severity_ratio", 0.0))
        score  = float(event.get("anomaly_score", 0.0))

        if not host or ratio < WARN_THRESHOLD:
            continue

        if is_protected(host):
            stats["skipped_protected"] += 1
            continue

        if ratio < CRITICAL_THRESHOLD:
            stats["warn"] += 1
            log.info("WARN     src=%s host=%s score=%.3f ratio=%.2fx", source, host, score, ratio)
            continue

        stats["critical"] += 1
        now = time.time()
        since = now - last_action.get(host, 0)

        log.info("CRITICAL src=%s host=%s score=%.3f ratio=%.2fx", source, host, score, ratio)

        if since < COOLDOWN_SECONDS:
            stats["skipped_cooldown"] += 1
            log.info("  ⊘ Cooldown actif (%ds restants)", int(COOLDOWN_SECONDS - since))
            continue

        # Ne tenter la rémédiation que si le host correspond à un vrai pod du namespace
        real_pods = list_pods_cached(api, NAMESPACE_TO_WATCH)
        if host not in real_pods:
            stats["skipped_not_in_ns"] += 1
            log.info("  ⊘ Host '%s' n'est pas un pod du namespace '%s' (ignoré)", host, NAMESPACE_TO_WATCH)
            last_action[host] = now  # cooldown pour éviter le spam
            continue

        log.info("  → REMEDIATION : delete pod %s in namespace %s", host, NAMESPACE_TO_WATCH)
        last_action[host] = now  # cooldown activé même si delete échoue
        if restart_pod(api, host, NAMESPACE_TO_WATCH):
            stats["restarted"] += 1


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped by user")
        sys.exit(0)
