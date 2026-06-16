# Service — email-sender

Consomme `logs-anomalies-ml`, batche les anomalies critiques sur 15 minutes, envoie un récap par email via SendGrid.

## Rôle

1. Consumer Kafka sur le même topic que `monitoring-ui`
2. Filtrer : ne garder que `severity_ratio > ALERT_THRESHOLD` (1.3 par défaut)
3. Batcher pendant `BATCH_INTERVAL_SECONDS` (900 = 15 min)
4. À la fin de la fenêtre : envoyer un email avec tous les incidents
5. Reset et nouvelle fenêtre

## Code

```
email-sender/
├── Dockerfile
├── requirements.txt    # confluent-kafka + sendgrid
└── main.py             # Boucle poll + batch + send
```

## Pourquoi SendGrid et pas SMTP ?

GCP bloque les ports SMTP sortants (25, 465, 587) sur les nœuds GKE pour limiter le spam. SendGrid utilise HTTPS (port 443) qui n'est pas bloqué. C'est aussi plus robuste pour la délivrabilité.

## Configuration SendGrid

Avant de pouvoir envoyer :
1. Créer un compte SendGrid (trial = 100 emails/jour pendant 60j)
2. **Sender Authentication → Single Sender Verification** : ajouter l'adresse expéditeur (ex: `LogGuardian <ton.email@gmail.com>`), valider via le lien email reçu
3. **API Keys** → Create → Full Access → copier `SG.xxxxx...`
4. Mettre `SENDGRID_API_KEY` dans `.env` ou dans le secret K8s

Sans validation de l'expéditeur : SendGrid renvoie une erreur 403 et **aucun email n'est envoyé**.

## Format de l'email

Subject : `[LogGuardian] N anomalie(s) détectée(s) — HH:MM UTC → HH:MM UTC`

Body (plain text) :
```
Bonjour,

LogGuardian a détecté N anomalie(s) entre 14:00 UTC et 14:15 UTC.
============================================================

[1/3] linux / combo
  Date      : 2026-06-17T14:03:21
  Score IA  : 1.42
  Ratio     : 1.86x
  Seuil     : 0.76
  IP        : 192.168.1.42
  User      : root
  Message   : Failed password for invalid user from 192.168.1.42
  Action    : Vérifier les authentifications récentes...
------------------------------------------------------------
...
```

## Recommandation contextuelle

`recommendation()` choisit une action selon le contenu du message :
- `authentication|ssh|kerberos` → vérifier auth, bloquer IP
- `timeout|connection` → check réseau, dispo service
- `error|failed` → inspecter service, corréler

## Variables clés

| Variable | Effet |
|---|---|
| `ALERT_THRESHOLD=1.3` | Filtre : seuls les `severity_ratio > 1.3` sont notifiés |
| `BATCH_INTERVAL_SECONDS=900` | Fenêtre de batching (15 min) |
| `SENDGRID_API_KEY` | Clé API SendGrid |
| `SMTP_USER` | Email expéditeur (vérifié SendGrid) |
| `MAIL_TO` | Destinataire |

## En GKE

- Deployment 1 replica, `enableServiceLinks: false`
- Image : `europe-west1-docker.pkg.dev/.../email-sender:latest`
- `envFrom`: ConfigMap + Secret `monitoring-ui-secrets`
- Resources mini : 50m CPU / 64Mi RAM

## Tester rapidement

```bash
# Réduire la fenêtre à 60s pour vérifier
kubectl set env deployment/email-sender BATCH_INTERVAL_SECONDS=60 -n logguardian
kubectl rollout restart deployment/email-sender -n logguardian
kubectl logs deployment/email-sender -n logguardian -f

# Remettre à 15 min après le test
kubectl set env deployment/email-sender BATCH_INTERVAL_SECONDS=900 -n logguardian
```

## Arrêter temporairement

```bash
kubectl scale deployment/email-sender -n logguardian --replicas=0
# Relancer
kubectl scale deployment/email-sender -n logguardian --replicas=1
```

## Logs typiques

```
Email sender started | broker=kafka.logguardian.svc.cluster.local:29092 | topic=logs-anomalies-ml | batch=900s
Batch email sent: 3 incidents | 14:00 UTC → 14:15 UTC
No anomalies in the last 900s, no email sent.
```
