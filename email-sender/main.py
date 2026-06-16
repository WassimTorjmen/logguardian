import json
import os
import re
import time
from datetime import datetime

from confluent_kafka import Consumer
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
ALERT_THRESHOLD         = float(os.getenv("ALERT_THRESHOLD", "1.3"))
BATCH_INTERVAL_SECONDS  = int(os.getenv("BATCH_INTERVAL_SECONDS", "900"))  # 15 min

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY", "")
MAIL_FROM        = os.getenv("SMTP_USER", "")
MAIL_TO          = os.getenv("MAIL_TO", "")


def extract_message(event):
    sequence = event.get("sequence", [])
    if isinstance(sequence, list) and sequence:
        last = sequence[-1]
        if isinstance(last, dict):
            return str(last.get("message", ""))
        return str(last)
    return str(event.get("message", ""))


def parse_incident(event):
    message = extract_message(event)
    ip_match   = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", message)
    user_match = re.search(r"user=([a-zA-Z0-9_.-]+)", message)
    return {
        "detected_at": event.get("detected_at", "—"),
        "source":      event.get("source", "unknown"),
        "host":        event.get("host", "unknown"),
        "score":       event.get("anomaly_score", 0),
        "ratio":       event.get("severity_ratio", 0),
        "threshold":   event.get("threshold", 0),
        "message":     message,
        "ip":          ip_match.group(0)   if ip_match   else "non détectée",
        "user":        user_match.group(1) if user_match else "non détecté",
        "model":       event.get("model_version", "unknown"),
    }


def recommendation(incident):
    msg = incident["message"].lower()
    if "authentication" in msg or "ssh" in msg or "kerberos" in msg:
        return "Vérifier les authentifications récentes, identifier l'IP source et bloquer l'accès si nécessaire."
    if "timeout" in msg or "connection" in msg:
        return "Vérifier la connectivité réseau, la disponibilité du service et les timeouts applicatifs."
    if "error" in msg or "failed" in msg:
        return "Inspecter le service concerné, corréler avec les logs voisins et redémarrer si l'erreur se répète."
    return "Analyser le service concerné, corréler avec les logs voisins et vérifier l'état de l'hôte."


def build_body(incidents, period_start, period_end):
    lines = [
        "Bonjour,",
        "",
        f"LogGuardian a détecté {len(incidents)} anomalie(s) entre {period_start} et {period_end}.",
        "=" * 60,
    ]
    for i, inc in enumerate(incidents, 1):
        lines += [
            "",
            f"[{i}/{len(incidents)}] {inc['source']} / {inc['host']}",
            f"  Date      : {inc['detected_at']}",
            f"  Score IA  : {inc['score']}",
            f"  Ratio     : {inc['ratio']}",
            f"  Seuil     : {inc['threshold']}",
            f"  IP        : {inc['ip']}",
            f"  User      : {inc['user']}",
            f"  Message   : {inc['message']}",
            f"  Action    : {recommendation(inc)}",
            "-" * 60,
        ]
    lines += ["", "LogGuardian — AIOps Command Center"]
    return "\n".join(lines)


def send_batch(incidents, period_start, period_end):
    if not SENDGRID_API_KEY or not MAIL_FROM or not MAIL_TO:
        print("SendGrid config incomplete. Email skipped.")
        return

    subject = f"[LogGuardian] {len(incidents)} anomalie(s) détectée(s) — {period_start} → {period_end}"
    body    = build_body(incidents, period_start, period_end)

    message = Mail(
        from_email=MAIL_FROM,
        to_emails=MAIL_TO,
        subject=subject,
        plain_text_content=body,
    )
    SendGridAPIClient(SENDGRID_API_KEY).send(message)
    print(f"Batch email sent: {len(incidents)} incidents | {period_start} → {period_end}")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id":          "logguardian-email-sender",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    print(f"Email sender started | broker={KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC} | batch={BATCH_INTERVAL_SECONDS}s")

    batch        = []
    batch_start  = time.time()
    period_start = datetime.utcnow().strftime("%H:%M UTC")

    while True:
        msg = consumer.poll(1.0)

        if msg is not None and not msg.error():
            try:
                event = json.loads(msg.value().decode("utf-8"))
                if float(event.get("severity_ratio", 0)) > ALERT_THRESHOLD:
                    batch.append(parse_incident(event))
            except Exception as e:
                print("Parse error:", e)

        # Envoie le batch toutes les 15 minutes s'il contient des incidents
        if time.time() - batch_start >= BATCH_INTERVAL_SECONDS:
            period_end = datetime.utcnow().strftime("%H:%M UTC")
            if batch:
                try:
                    send_batch(batch, period_start, period_end)
                except Exception as e:
                    print("Email sender error:", e)
            else:
                print(f"No anomalies in the last {BATCH_INTERVAL_SECONDS}s, no email sent.")
            batch        = []
            batch_start  = time.time()
            period_start = datetime.utcnow().strftime("%H:%M UTC")


if __name__ == "__main__":
    main()
