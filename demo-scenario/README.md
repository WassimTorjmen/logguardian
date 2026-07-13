# Scénario de démo LogGuardian

Setup complet pour démontrer la détection d'anomalies en temps réel sur une infrastructure simulée.

## Vue d'ensemble

```
┌───────────────────────────────────────────────────────┐
│  Namespace: demo-apps                                 │
│                                                       │
│  Phase 1 (steady state) :                             │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │ web-front  │  │ api-backend│  │ db-worker  │       │
│  │ (logs OK)  │  │ (logs OK)  │  │ (logs OK)  │       │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘       │
│        └────────┬─────┴─────┬──────────┘              │
│                 ▼                                     │
│         stream_pod_logs.py                            │
│                 ▼                                     │
│           Kafka logs-raw                              │
│                 ▼                                     │
│         ETL → ML → UI (état normal)                   │
│                                                       │
│  Phase 2 (attaque) : deploy chaos-injector            │
│  ┌────────────────┐                                   │
│  │ chaos-injector │  ← spam de logs suspects          │
│  │ (SSH brute,    │                                   │
│  │  escalation,   │                                   │
│  │  exfiltration) │                                   │
│  └───────┬────────┘                                   │
│          └─────────► stream_pod_logs.py               │
│                              ▼                        │
│                         UI + email alert 🚨           │
└───────────────────────────────────────────────────────┘
```

## Prérequis

- Cluster GKE `logguardian` opérationnel
- LogGuardian déployé et fonctionnel
- Port-forward Kafka actif : `kubectl port-forward -n logguardian svc/kafka 29092:29092`
- Le script `log-producer/stream_pod_logs.py` prêt (voir `log-producer/README.md`)
- Seuil `ml-model` baissé à `0.5` pour la démo (voir "Ajustements avant démo")

## Ajustements avant démo

### 1. Baisser le seuil du modèle ML (rendre la détection plus sensible)

```powershell
gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./threshold.json
# Editer : "threshold": 0.5
notepad threshold.json
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json
kubectl rollout restart deployment/ml-model -n logguardian
```

### 2. Réduire la fenêtre de batching email (voir les emails vite)

```powershell
kubectl set env deployment/email-sender -n logguardian BATCH_INTERVAL_SECONDS=60
```

## Déroulé de la démo (12-15 min)

### Étape 1 — Setup initial (T-2 min, avant le jury)

```powershell
# Déploie les 3 apps normales
kubectl apply -f demo-scenario/01-demo-apps.yaml

# Vérifie qu'elles tournent
kubectl get pods -n demo-apps
# STATUS doit être Running pour les 3
```

### Étape 2 — Lancer le streaming (T-1 min)

Dans un terminal dédié (à laisser tourner et à afficher au jury) :

```powershell
cd log-producer
.\.venv\Scripts\Activate.ps1
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka.logguardian.svc.cluster.local:29092"
python stream_pod_logs.py --namespace demo-apps --source demo-apps
```

Tu verras :
```
Monitoring namespace='demo-apps' | source='demo-apps' | exclude=[]
[START] Streaming demo-apps/web-frontend-...
[START] Streaming demo-apps/api-backend-...
[START] Streaming demo-apps/db-worker-...
```

### Étape 3 — Montrer l'état normal (T=0 min)

Ouvre https://logguardian.34-8-214-70.nip.io → onglet **Flux logs**.

Filtre `demo-apps` dans la barre de recherche → tu vois défiler les logs INFO/WARN des 3 services.

Narrative :
> "Voici mon infrastructure : trois microservices — un frontend web, un backend API, un worker DB. LogGuardian les surveille en temps réel. Actuellement, aucune anomalie détectée : tous les logs sont classés NORMAL."

### Étape 4 — Injecter l'attaque (T=5 min)

Dans un autre terminal :

```powershell
kubectl apply -f demo-scenario/02-chaos-injector.yaml
```

Le pod `chaos-injector` démarre et pompe des logs :
- 20 échecs de connexion SSH sur différents comptes
- Escalade de privilège
- Reverse shell
- Exfiltration de données
- Établissement de persistence

Le script `stream_pod_logs.py` détecte le nouveau pod automatiquement (scan toutes les 30s) et commence à streamer ses logs.

Narrative :
> "Je viens de déployer un injecteur de chaos qui simule un incident de sécurité : un attaquant tente une attaque brute-force SSH puis escalade les privilèges."

### Étape 5 — Voir la détection (T=6-7 min)

Sur l'UI :
- Filtre `chaos-injector` ou `ssh-attacker` dans la barre de recherche
- Les logs remontent avec statut **ANOMALIE**
- Onglet **Incident board** → le compteur cumulé augmente
- Clique sur un incident → panneau **Analyser avec l'IA** → Groq génère l'explication

Narrative :
> "En moins d'une minute, LogGuardian a détecté ces logs comme anormaux — le score de reconstruction dépasse le seuil. Regardez le panneau IA : Groq nous explique en langage naturel ce qui se passe et propose une action corrective."

### Étape 6 — Montrer l'email d'alerte (T=8 min)

Ouvre ta boîte email → l'email récapitulatif SendGrid est arrivé (batch de 60s pour la démo).

Narrative :
> "En parallèle, LogGuardian a envoyé un email à l'équipe d'astreinte avec le récapitulatif des incidents détectés dans la dernière minute."

## Cleanup après la démo

```powershell
# Supprimer le scénario
kubectl delete namespace demo-apps

# Restaurer le seuil normal
gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./threshold.json
# Editer : "threshold": 1.526948
notepad threshold.json
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json
kubectl rollout restart deployment/ml-model -n logguardian

# Restaurer le batching email
kubectl set env deployment/email-sender -n logguardian BATCH_INTERVAL_SECONDS=900

# Arrêter le port-forward (Ctrl+C dans son terminal)
# Arrêter stream_pod_logs.py (Ctrl+C)
```

## Aide-mémoire — commandes de secours

Si un pod ne démarre pas :
```powershell
kubectl describe pod -n demo-apps <pod-name>
kubectl logs -n demo-apps <pod-name>
```

Si le streaming ne remonte pas les logs :
```powershell
# Vérifier que le port-forward est actif
Test-NetConnection -ComputerName localhost -Port 29092

# Vérifier ce qui arrive dans Kafka
kubectl exec -n logguardian deployment/kafka -- kafka-console-consumer --bootstrap-server localhost:29092 --topic logs-raw --timeout-ms 10000 | Select-String "demo-apps"
```

Si les anomalies ne remontent pas :
```powershell
kubectl logs deployment/ml-model -n logguardian --tail=100 | Select-String "demo-apps"
```

Redémarrer proprement l'injecteur (relancer la séquence) :
```powershell
kubectl delete -f demo-scenario/02-chaos-injector.yaml
kubectl apply -f demo-scenario/02-chaos-injector.yaml
```
