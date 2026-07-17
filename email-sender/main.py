"""
email-sender — LogGuardian
Consomme logs-anomalies-ml et envoie un récapitulatif périodique par batch.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("alert-email-sender")

# ── Configuration Kafka ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "logguardian-alert-email-sender")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")

# ── Configuration batch ────────────────────────────────────────────────────
ALERT_BATCH_SECONDS  = int(os.getenv("ALERT_BATCH_SECONDS",  "900"))
ALERT_BATCH_MAX_SIZE = int(os.getenv("ALERT_BATCH_MAX_SIZE", "100"))

# ── Configuration envoi (SendGrid API — HTTPS, non bloqué sortant sur GCP) ──
# GCP bloque les ports SMTP sortants (25/465/587) sur les nœuds GKE : l'envoi
# passe donc par l'API HTTPS de SendGrid plutôt que par smtplib direct.
SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
SMTP_USER        = os.getenv("SMTP_USER", "")   # réutilisé comme adresse expéditeur (vérifiée côté SendGrid)
MAIL_TO          = os.getenv("MAIL_TO",   "")

# Délais de retry progressifs (secondes)
_SEND_RETRY_DELAYS = [5, 15, 30, 60]

_running = True


def _handle_signal(sig, _frame) -> None:
    global _running
    log.info("Signal %s reçu — arrêt propre en cours…", sig)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ── Helpers ────────────────────────────────────────────────────────────────

def _extract_message(event: dict) -> str:
    sequence = event.get("sequence") or []
    if isinstance(sequence, list) and sequence:
        last = sequence[-1]
        if isinstance(last, dict):
            return str(last.get("message", ""))[:400]
        return str(last)[:400]
    return str(event.get("message", ""))[:400]


# ── Construction de l'email ────────────────────────────────────────────────

def _build_digest_body(
    batch: list[dict],
    period_start: datetime,
    period_end: datetime,
) -> tuple[str, str]:
    """Retourne (subject, body) pour le digest."""
    n       = len(batch)
    sources = sorted({e.get("source", "unknown") for e in batch})
    hosts   = sorted({e.get("host",   "unknown") for e in batch})

    fmt        = "%Y-%m-%d %H:%M:%S UTC"
    period_str = f"{period_start.strftime(fmt)} → {period_end.strftime(fmt)}"
    sep        = "═" * 62

    lines: list[str] = [
        "Bonjour,",
        "",
        f"LogGuardian a détecté {n} anomalie(s) dans la période :",
        f"  {period_str}",
        "",
        f"Sources concernées : {', '.join(sources)}",
        f"Hôtes concernés    : {', '.join(hosts)}",
        "",
        sep,
        "DÉTAIL DES ANOMALIES",
        sep,
    ]

    for i, event in enumerate(batch, 1):
        lines += [
            "",
            f"── Anomalie {i}/{n} " + "─" * 42,
            f"  Timestamp : {event.get('detected_at', '—')}",
            f"  Source    : {event.get('source', '—')}",
            f"  Hôte      : {event.get('host', '—')}",
            f"  Statut    : ANOMALIE",
            f"  Score IA  : {event.get('anomaly_score', '—')}",
            f"  Ratio     : {event.get('severity_ratio', '—')}",
            f"  Seuil     : {event.get('threshold', '—')}",
            f"  Modèle    : {event.get('model_version', '—')}",
            f"  Message   : {_extract_message(event)}",
        ]

    generated_at = datetime.now(tz=timezone.utc).strftime(fmt)
    lines += [
        "",
        "─" * 62,
        "LogGuardian — AIOps Command Center",
        f"Email généré le : {generated_at}",
    ]

    subject = f"[LogGuardian] Récapitulatif de {n} anomalie(s)"
    body    = "\n".join(lines)
    return subject, body


# ── SendGrid ───────────────────────────────────────────────────────────────

def _send_digest(
    batch: list[dict],
    period_start: datetime,
    period_end: datetime,
) -> None:
    if not SENDGRID_API_KEY or not SMTP_USER or not MAIL_TO:
        log.warning("Configuration SendGrid incomplète — envoi du digest ignoré.")
        return

    subject, body = _build_digest_body(batch, period_start, period_end)
    message = Mail(
        from_email=SMTP_USER,
        to_emails=MAIL_TO,
        subject=subject,
        plain_text_content=body,
    )
    SendGridAPIClient(SENDGRID_API_KEY).send(message)

    log.info("Alert digest sent | anomalies=%d | to=%s", len(batch), MAIL_TO)


# ── Flush avec retry ───────────────────────────────────────────────────────

def _flush_with_retry(
    consumer: Consumer,
    batch: list[dict],
    period_start: datetime,
    period_end: datetime,
) -> None:
    """Envoie le digest. Retry progressif. Commit tous les offsets après succès."""
    retry_delays = list(_SEND_RETRY_DELAYS)

    while True:
        try:
            _send_digest(batch, period_start, period_end)
            # Succès → valider les offsets de tout le batch
            consumer.commit(asynchronous=False)
            return
        except Exception as exc:
            if not retry_delays:
                log.error(
                    "Envoi echec définitif — batch de %d anomalie(s) non envoyé : %s",
                    len(batch), exc,
                )
                # Pas de commit : le batch sera rejoué au prochain démarrage
                return
            delay = retry_delays.pop(0)
            log.error("SendGrid error | retry_in=%ds | error=%s", delay, exc)
            time.sleep(delay)


# ── Kafka ──────────────────────────────────────────────────────────────────

def _make_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers":     KAFKA_BOOTSTRAP_SERVERS,
        "group.id":              KAFKA_GROUP_ID,
        "auto.offset.reset":     KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit":    False,
        "heartbeat.interval.ms": 3000,
        "session.timeout.ms":    30000,
        "max.poll.interval.ms":  300000,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info(
        "Alert email sender started | topic=%s | batch_seconds=%d | batch_max=%d",
        KAFKA_TOPIC, ALERT_BATCH_SECONDS, ALERT_BATCH_MAX_SIZE,
    )
    return consumer


def _close_consumer(consumer: Consumer | None) -> None:
    if consumer is None:
        return
    try:
        consumer.close()
    except Exception:
        pass


# ── Boucle principale ──────────────────────────────────────────────────────

def main() -> None:
    consumer: Consumer | None = None
    batch: list[dict] = []
    batch_start = datetime.now(tz=timezone.utc)

    while _running:
        # (Re)connexion Kafka
        if consumer is None:
            try:
                consumer   = _make_consumer()
                batch_start = datetime.now(tz=timezone.utc)
            except Exception as exc:
                log.error("Kafka connect error: %s — retry in 5s", exc)
                time.sleep(5)
                continue

        # Polling
        try:
            msg = consumer.poll(1.0)
        except KafkaException as exc:
            log.error("Kafka transport error: %s — reconnect", exc)
            _close_consumer(consumer)
            consumer = None
            time.sleep(3)
            continue

        now     = datetime.now(tz=timezone.utc)
        elapsed = (now - batch_start).total_seconds()

        if msg is None:
            # Pas de message — vérifier expiration du batch
            if batch and elapsed >= ALERT_BATCH_SECONDS:
                log.info(
                    "Batch timeout (%ds) — flush de %d anomalie(s)",
                    ALERT_BATCH_SECONDS, len(batch),
                )
                _flush_with_retry(consumer, batch, batch_start, now)
                batch       = []
                batch_start = datetime.now(tz=timezone.utc)
            continue

        if msg.error():
            log.warning("Kafka message error: %s", msg.error())
            continue

        # Accumulation
        try:
            event = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            log.warning("JSON parse error: %s", exc)
            consumer.commit(message=msg, asynchronous=False)
            continue

        batch.append(event)
        log.info("Anomaly added to batch | batch_size=%d", len(batch))

        # Flush si taille max ou délai dépassé
        if len(batch) >= ALERT_BATCH_MAX_SIZE or elapsed >= ALERT_BATCH_SECONDS:
            log.info(
                "Flush déclenché | taille=%d | elapsed=%.0fs",
                len(batch), elapsed,
            )
            _flush_with_retry(consumer, batch, batch_start, now)
            batch       = []
            batch_start = datetime.now(tz=timezone.utc)

    # Arrêt propre
    log.info("Arrêt demandé — fermeture du consumer Kafka.")
    _close_consumer(consumer)


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════
# ROLLBACK — ancienne version SMTP Gmail (désactivée, inerte)
#
# Cassée en prod : GCP bloque les ports SMTP sortants (25/465/587) sur les
# nœuds GKE, d'où l'échec systématique (535 Bad Credentials n'était qu'un
# symptôme secondaire — même avec des identifiants valides, la connexion au
# port 587 est filtrée côté réseau GCP).
#
# Pour revenir en arrière en urgence : supprimer le bloc SendGrid ci-dessus
# (imports, SENDGRID_API_KEY, _build_digest_body/_send_digest) et coller
# le code ci-dessous à sa place. Nécessite aussi de remettre
# `import smtplib` et `from email.message import EmailMessage` en haut du
# fichier, et SMTP_HOST/SMTP_PORT/SMTP_PASSWORD dans la ConfigMap.
# ═══════════════════════════════════════════════════════════════════════════
_DISABLED_GMAIL_SMTP_VERSION = r'''
SMTP_HOST     = os.getenv("SMTP_HOST",     "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",     "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
MAIL_TO       = os.getenv("MAIL_TO",       "")

def _build_digest_email(batch, period_start, period_end):
    n       = len(batch)
    sources = sorted({e.get("source", "unknown") for e in batch})
    hosts   = sorted({e.get("host",   "unknown") for e in batch})
    fmt        = "%Y-%m-%d %H:%M:%S UTC"
    period_str = f"{period_start.strftime(fmt)} -> {period_end.strftime(fmt)}"
    lines = ["Bonjour,", "", f"LogGuardian a detecte {n} anomalie(s) dans la periode :", f"  {period_str}"]
    for i, event in enumerate(batch, 1):
        lines += [f"[{i}/{n}] {event.get('source')} / {event.get('host')} - {event.get('anomaly_score')}"]
    email = EmailMessage()
    email["Subject"] = f"[LogGuardian] Recapitulatif de {n} anomalie(s)"
    email["From"]    = SMTP_USER
    email["To"]      = MAIL_TO
    email.set_content("\n".join(lines), charset="utf-8")
    return email

def _smtp_connect():
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(SMTP_USER, SMTP_PASSWORD)
    return server

def _send_digest(batch, period_start, period_end):
    if not SMTP_USER or not SMTP_PASSWORD or not MAIL_TO:
        log.warning("SMTP config incomplete - envoi du digest ignore.")
        return
    email  = _build_digest_email(batch, period_start, period_end)
    server = _smtp_connect()
    try:
        server.send_message(email)
    finally:
        try:
            server.quit()
        except Exception:
            pass
    log.info("Alert digest sent | anomalies=%d | to=%s", len(batch), MAIL_TO)
'''
