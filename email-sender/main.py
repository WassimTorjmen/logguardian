import json
import os
import re
import smtplib
import time
from email.mime.text import MIMEText

from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-anomalies-ml")
ALERT_THRESHOLD = float(os.getenv("ALERT_THRESHOLD", "1.3"))

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
MAIL_TO = os.getenv("MAIL_TO", "")

EMAIL_COOLDOWN_SECONDS = int(os.getenv("EMAIL_COOLDOWN_SECONDS", "60"))

sent_cache = {}


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

    ip_match = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", message)
    user_match = re.search(r"user=([a-zA-Z0-9_.-]+)", message)

    return {
        "detected_at": event.get("detected_at", "—"),
        "source": event.get("source", "unknown"),
        "host": event.get("host", "unknown"),
        "score": event.get("anomaly_score", 0),
        "ratio": event.get("severity_ratio", 0),
        "threshold": event.get("threshold", 0),
        "message": message,
        "ip": ip_match.group(0) if ip_match else "non détectée",
        "user": user_match.group(1) if user_match else "non détecté",
        "model": event.get("model_version", "unknown"),
    }


def recommendation(incident):
    msg = incident["message"].lower()

    if "authentication" in msg or "ssh" in msg or "kerberos" in msg:
        return "Vérifier les authentifications récentes, identifier l’IP source et bloquer l’accès si nécessaire."

    if "timeout" in msg or "connection" in msg:
        return "Vérifier la connectivité réseau, la disponibilité du service et les timeouts applicatifs."

    if "error" in msg or "failed" in msg:
        return "Inspecter le service concerné, corréler avec les logs voisins et redémarrer si l’erreur se répète."

    return "Analyser le service concerné, corréler avec les logs voisins et vérifier l’état de l’hôte."


def should_send(incident):
    key = f"{incident['host']}|{incident['source']}|{incident['message']}"
    now = time.time()

    last_sent = sent_cache.get(key)

    if last_sent and now - last_sent < EMAIL_COOLDOWN_SECONDS:
        return False

    sent_cache[key] = now
    return True


def send_email(incident):
    if not SMTP_USER or not SMTP_PASSWORD or not MAIL_TO:
        print("SMTP config incomplete. Email skipped.")
        return

    subject = f"[LogGuardian] Incident détecté - {incident['source']} / {incident['host']}"

    body = f"""Bonjour,

Un incident a été détecté par LogGuardian.

Date      : {incident['detected_at']}
Source    : {incident['source']}
Host      : {incident['host']}
Score IA  : {incident['score']}
Ratio     : {incident['ratio']}
Seuil     : {incident['threshold']}
Modèle    : {incident['model']}

Message :
{incident['message']}

Parsing automatique :
IP détectée          : {incident['ip']}
Utilisateur détecté : {incident['user']}

Recommandation :
{recommendation(incident)}

LogGuardian — AIOps Command Center
"""

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = MAIL_TO

    with smtplib.SMTP_SSL(SMTP_HOST, 465, timeout=20) as server:
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    print(f"Email sent: {incident['source']} ratio={incident['ratio']}")


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "logguardian-email-sender",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print(f"Email sender started | broker={KAFKA_BOOTSTRAP_SERVERS} | topic={KAFKA_TOPIC}")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            event = json.loads(msg.value().decode("utf-8"))
            ratio = float(event.get("severity_ratio", 0))

            if ratio <= ALERT_THRESHOLD:
                continue

            incident = parse_incident(event)

            if not should_send(incident):
                continue

            send_email(incident)

        except Exception as e:
            print("Email sender error:", e)


if __name__ == "__main__":
    main()