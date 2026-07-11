# log-producer

Scripts Python pour injecter des logs personnalisés dans le pipeline LogGuardian, utiles pour :
- Tester le pipeline end-to-end sans dépendre du log-generator (Loghub replay)
- Démontrer la détection d'anomalies en démo
- Streamer les logs de tes propres pods Kubernetes vers LogGuardian

## Prérequis

- Kafka accessible (via `kubectl port-forward` en GKE ou directement en local Docker Compose)
- Python 3.11+

## Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1     # Windows
# source .venv/bin/activate      # Linux/Mac
pip install -r requirements.txt
```

## Connexion à Kafka

### En local (Docker Compose)

Kafka est sur `localhost:9092` — la valeur par défaut fonctionne.

### En GKE via port-forward

Terminal 1 (à laisser tourner) :

```powershell
kubectl port-forward -n logguardian svc/kafka 29092:29092
```

Ajouter au fichier hosts (une seule fois, en admin) :

```powershell
Add-Content -Path C:\Windows\System32\drivers\etc\hosts -Value "127.0.0.1 kafka.logguardian.svc.cluster.local"
```

Puis dans le terminal du script :

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka.logguardian.svc.cluster.local:29092"
```

## send_my_logs.py — Injecter des logs personnalisés

Génère et envoie des batches de logs à Kafka.

### Modes disponibles

| Mode | Description |
|---|---|
| `synthetic` | 20 logs Linux réalistes (normaux + suspects) |
| `attack` | 20 logs "attaque" avec vocabulaire inventé |
| `ssh-brute` | 20 logs SSH brute-force sur 10 comptes utilisateurs |
| `unknown-source` | 15 logs avec `source=myapp` inconnue du modèle (anomalie garantie) |
| `file` | Tail d'un fichier `.log` local (comme `tail -f`) |
| `windows` | Windows Event Logs (Application, System, Security) |

### Exemples

```powershell
python send_my_logs.py --mode synthetic
python send_my_logs.py --mode unknown-source
python send_my_logs.py --mode file --path "C:\logs\myapp.log" --source myapp
python send_my_logs.py --mode windows --channel Application
```

## stream_pod_logs.py — Suivre des pods K8s en temps réel

Tail les logs de tous les pods d'un namespace et les pousse vers Kafka.

### Exemples

```powershell
# Suivre tous les pods du namespace 'default'
python stream_pod_logs.py --namespace default --source k8s

# Suivre uniquement certains pods (exclure les autres)
python stream_pod_logs.py --namespace demo-apps --source demo-apps --exclude kafka,zookeeper

# Ajuster la fenêtre de logs passés et la fréquence de scan
python stream_pod_logs.py --namespace default --source k8s --since 30 --refresh 60
```

⚠️ Ne pas utiliser sur le namespace `logguardian` sans exclure les pods du pipeline (log-generator, etl-processor, ml-model, monitoring-ui, email-sender, kafka, zookeeper) → boucle infinie.

## Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Adresse du broker Kafka |
| `KAFKA_TOPIC` | `logs-raw` | Topic où pousser les logs |

## Voir les résultats

Sur le dashboard LogGuardian (`https://<ton-domaine>`) :
- Onglet **Flux logs** → cherche par host (ex: `web-server-prod`) ou source
- Onglet **Incident board** → les anomalies détectées

Ou dans les logs du ml-model :

```powershell
kubectl logs deployment/ml-model -n logguardian --tail=200 | Select-String "<TON_HOST>"
```
