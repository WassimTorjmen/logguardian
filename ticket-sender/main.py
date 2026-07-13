"""
ticket-sender — LogGuardian
Consomme support-tickets et envoie chaque ticket immédiatement par email.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import signal
import smtplib
import time
from datetime import datetime, timezone
from email.message import EmailMessage

from confluent_kafka import Consumer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("support-ticket-sender")

# ── Configuration Kafka ────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_SUPPORT_TOPIC     = os.getenv("KAFKA_SUPPORT_TOPIC",    "support-tickets")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID",         "logguardian-support-ticket-sender")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# ── Configuration SMTP ─────────────────────────────────────────────────────
SMTP_HOST       = os.getenv("SMTP_HOST",       "smtp.gmail.com")
SMTP_PORT       = int(os.getenv("SMTP_PORT",   "587"))
SMTP_USER       = os.getenv("SMTP_USER",       "")
SMTP_PASSWORD   = os.getenv("SMTP_PASSWORD",   "")
MAIL_TO         = os.getenv("MAIL_TO",         "")
SUPPORT_MAIL_TO = os.getenv("SUPPORT_MAIL_TO", "").strip()

# Délais de retry progressifs (secondes)
_SMTP_RETRY_DELAYS = [5, 15, 30, 60]

_running = True


def _handle_signal(sig, _frame) -> None:
    global _running
    log.info("Signal %s reçu — arrêt propre en cours…", sig)
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ── Helpers ────────────────────────────────────────────────────────────────

def _recipient() -> str:
    """Adresse destinataire : SUPPORT_MAIL_TO si défini, sinon MAIL_TO."""
    return SUPPORT_MAIL_TO or MAIL_TO


def _ticket_id(ticket: dict) -> str:
    """Identifiant court et stable dérivé du ticket."""
    raw = f"{ticket.get('submitted_at', '')}{ticket.get('log_id', '')}"
    return hashlib.sha1(raw.encode()).hexdigest()[:10].upper()


# ── Construction de l'email ────────────────────────────────────────────────

def _build_ticket_email(ticket: dict) -> EmailMessage:
    tid       = _ticket_id(ticket)
    recipient = _recipient()
    user_msg  = (ticket.get("user_message") or "").strip() or "Aucun commentaire fourni."
    sep       = "═" * 62
    fmt       = "%Y-%m-%d %H:%M:%S UTC"

    lines: list[str] = [
        "Bonjour,",
        "",
        "Un ticket de support LogGuardian requiert une analyse manuelle.",
        "",
        sep,
        "INFORMATIONS DU TICKET",
        sep,
        f"  Identifiant ticket   : {tid}",
        f"  Créé le              : {ticket.get('submitted_at', '—')}",
        f"  Réponses IA rejetées : {ticket.get('negative_feedback_count', '—')}",
        "",
        sep,
        "MESSAGE DE L'OPÉRATEUR",
        sep,
        user_msg,
        "",
        sep,
        "LOG CONCERNÉ",
        sep,
        f"  Identifiant log : {ticket.get('log_id', '—')}",
        f"  Timestamp       : {ticket.get('timestamp', '—')}",
        f"  Source          : {ticket.get('source', '—')}",
        f"  Hôte            : {ticket.get('host', '—')}",
        f"  Statut          : {ticket.get('statut', '—')}",
        f"  Score IA        : {ticket.get('score_ia', '—')}",
        f"  Ratio           : {ticket.get('ratio', '—')}",
        f"  Modèle LSTM     : {ticket.get('model_version', '—')}",
        "",
        "Message du log :",
        ticket.get("message", "—"),
        "",
        sep,
        "DERNIÈRE ANALYSE IA REJETÉE",
        sep,
        ticket.get("last_analysis", "—"),
        "",
        sep,
        "DERNIÈRE RECOMMANDATION IA REJETÉE",
        sep,
        ticket.get("last_action", "—"),
        "",
        "─" * 62,
        "LogGuardian — AIOps Command Center",
        f"Email généré le : {datetime.now(tz=timezone.utc).strftime(fmt)}",
    ]

    email = EmailMessage()
    email["Subject"] = f"[LogGuardian][Ticket {tid}] Analyse manuelle demandée"
    email["From"]    = SMTP_USER
    email["To"]      = recipient
    email.set_content("\n".join(lines), charset="utf-8")
    return email


# ── SMTP ───────────────────────────────────────────────────────────────────

def _smtp_connect() -> smtplib.SMTP:
    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(SMTP_USER, SMTP_PASSWORD)
    return server


def _send_ticket_email(ticket: dict) -> None:
    recipient = _recipient()
    if not SMTP_USER or not SMTP_PASSWORD or not recipient:
        log.warning("SMTP config incomplète — ticket ignoré.")
        return

    email  = _build_ticket_email(ticket)
    server = _smtp_connect()
    try:
        server.send_message(email)
    finally:
        try:
            server.quit()
        except Exception:
            pass

    tid = _ticket_id(ticket)
    log.info("Support ticket sent | ticket_id=%s | recipient=%s", tid, recipient)


# ── Traitement avec retry ──────────────────────────────────────────────────

def _process_with_retry(consumer: Consumer, ticket: dict, msg) -> None:
    """Envoie le ticket. Retry progressif. Commit l'offset après succès."""
    tid          = _ticket_id(ticket)
    retry_delays = list(_SMTP_RETRY_DELAYS)

    while True:
        try:
            _send_ticket_email(ticket)
            consumer.commit(message=msg, asynchronous=False)
            return
        except Exception as exc:
            if not retry_delays:
                log.error(
                    "SMTP echec définitif | ticket_id=%s | error=%s — offset non validé",
                    tid, exc,
                )
                # Pas de commit : le ticket sera rejoué au prochain démarrage
                return
            delay = retry_delays.pop(0)
            log.error(
                "SMTP error | ticket_id=%s | retry_in=%ds | error=%s",
                tid, delay, exc,
            )
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
    consumer.subscribe([KAFKA_SUPPORT_TOPIC])
    log.info(
        "Support ticket sender started | topic=%s | recipient=%s",
        KAFKA_SUPPORT_TOPIC, _recipient() or "(non configuré)",
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

    while _running:
        # (Re)connexion Kafka
        if consumer is None:
            try:
                consumer = _make_consumer()
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

        if msg is None:
            continue

        if msg.error():
            log.warning("Kafka message error: %s", msg.error())
            continue

        # Traitement
        try:
            ticket = json.loads(msg.value().decode("utf-8"))
        except Exception as exc:
            log.warning("JSON parse error: %s — message ignoré", exc)
            consumer.commit(message=msg, asynchronous=False)
            continue

        tid = _ticket_id(ticket)
        log.info("Support ticket received | ticket_id=%s", tid)
        _process_with_retry(consumer, ticket, msg)

    # Arrêt propre
    log.info("Arrêt demandé — fermeture du consumer Kafka.")
    _close_consumer(consumer)


if __name__ == "__main__":
    main()
