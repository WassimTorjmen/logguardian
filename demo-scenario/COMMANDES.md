# Commandes démo — LogGuardian

Référence rapide, sans narration. Ouvrir 5 terminaux PowerShell.

## Terminal 1 — contrôle général

```powershell
kubectl get pods -n logguardian
kubectl apply -f k8s/
kubectl apply -f demo-scenario/01-demo-apps.yaml
kubectl get pods -n demo-apps -w
```

## Terminal 2 — tunnel Kafka (laisser tourner)

```powershell
kubectl port-forward -n logguardian svc/kafka 29092:29092
```

Vérifier l'entrée hosts (une fois, PowerShell admin si absente) :

```powershell
Get-Content C:\Windows\System32\drivers\etc\hosts | Select-String kafka.logguardian
Add-Content -Path C:\Windows\System32\drivers\etc\hosts -Value "127.0.0.1 kafka.logguardian.svc.cluster.local"
```

## Ajuster les seuils avant démo (Terminal 1)

```powershell
gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./threshold.json
notepad threshold.json
```

Mettre `"threshold": 0.3`, enregistrer, fermer.

```powershell
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json
kubectl rollout restart deployment/ml-model -n logguardian
kubectl set env deployment/auto-remediator -n logguardian CRITICAL_THRESHOLD=1.8 WARN_THRESHOLD=1.3
```

Vérifier le rechargement :

```powershell
kubectl logs deployment/ml-model -n logguardian --tail=20 | Select-String "seuil"
```

⚠️ Ne **pas** toucher à `email-sender` ici — le laisser à `0` replica (voir section dédiée ci-dessous). Avec le seuil à 0.3, le laisser tourner en continu épuise le quota SendGrid trial (100 emails/jour) en quelques minutes.

## Terminal 3 — streaming apps normales (laisser tourner)

```powershell
cd D:\ESGI\5ème année\PA\logguardian\log-producer
.\.venv\Scripts\Activate.ps1
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka.logguardian.svc.cluster.local:29092"
python stream_pod_logs.py --namespace demo-apps --source linux --exclude chaos-injector
```

## Terminal 4 — streaming chaos-injector (laisser tourner)

```powershell
cd D:\ESGI\5ème année\PA\logguardian\log-producer
.\.venv\Scripts\Activate.ps1
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka.logguardian.svc.cluster.local:29092"
python stream_pod_logs.py --namespace demo-apps --source malware --exclude web-frontend,api-backend,db-worker
```

## Terminal 5 — logs auto-remediator (laisser tourner)

```powershell
kubectl logs -n logguardian deployment/auto-remediator -f
```

## Déclencher l'attaque (Terminal 1 ou nouveau)

```powershell
kubectl apply -f demo-scenario/02-chaos-injector.yaml
```

## Montrer les emails (quota SendGrid partagé — procédure stricte)

Compte SendGrid trial = **100 emails/jour**, partagé entre **`email-sender`** (digest périodique des anomalies) et **`ticket-sender`** (ticket immédiat quand un opérateur rejette 5 réponses IA de suite dans le dashboard). Les deux services utilisent la même clé API — chaque envoi de l'un ou l'autre consomme le même quota.

Avec le seuil ML à 0.3, un lot `email-sender` se remplit en quelques secondes : si le service tourne en continu pendant la préparation ou la démo, le quota saute avant même d'arriver à cette étape.

**Par défaut, `email-sender` ET `ticket-sender` restent à `0` replica.** Ne les activer qu'au moment précis où tu veux montrer l'envoi, et les couper immédiatement après confirmation.

### email-sender (digest périodique)

```powershell
kubectl scale deployment/email-sender -n logguardian --replicas=1
kubectl logs deployment/email-sender -n logguardian -f
```

Dès que la ligne suivante apparaît (quelques secondes) :

```
Alert digest sent | anomalies=... | to=...
```

Couper immédiatement (Ctrl+C, puis) :

```powershell
kubectl scale deployment/email-sender -n logguardian --replicas=0
```

### ticket-sender (ticket manuel depuis le dashboard)

Scénario : dans l'onglet Flux logs, sélectionner une anomalie, cliquer "Analyser avec l'IA", cliquer "Pas utile" cinq fois de suite → un popup de ticket support apparaît → remplir un commentaire → envoyer.

Activer le service juste avant ce clic final :

```powershell
kubectl scale deployment/ticket-sender -n logguardian --replicas=1
kubectl logs deployment/ticket-sender -n logguardian -f
```

Dès que la ligne suivante apparaît :

```
Support ticket sent | ticket_id=... | recipient=...
```

Couper immédiatement :

```powershell
kubectl scale deployment/ticket-sender -n logguardian --replicas=0
```

**Chaque démonstration = 1 email du quota partagé.** Ne jamais laisser l'un ou l'autre tourner plus longtemps que le temps de capturer son log de confirmation.

Vérifier l'état à tout moment :

```powershell
kubectl get pods -n logguardian -l app=email-sender
kubectl get pods -n logguardian -l app=ticket-sender
```

Doit afficher aucun pod (replicas=0) en dehors des fenêtres de démonstration.

**Si le quota est déjà épuisé le jour J** (vérifiable sur https://app.sendgrid.com/email_activity) : sauter ces étapes en démo, mentionner que l'envoi a déjà été validé en amont (SendGrid API, HTTPS, contourne le blocage SMTP sortant de GCP), montrer le code/log plutôt que l'email reçu.

## Vérifications ponctuelles pendant la démo

```powershell
# Kafka reçoit bien les logs demo-apps
kubectl exec -n logguardian deployment/kafka -- kafka-console-consumer --bootstrap-server localhost:29092 --topic logs-raw --timeout-ms 10000 | Select-String "demo-apps"

# Scores ml-model pour un host donné
kubectl logs deployment/ml-model -n logguardian --tail=200 | Select-String "chaos-injector"

# État des pods demo-apps
kubectl get pods -n demo-apps

# Consumer groups Kafka (lag)
kubectl exec -n logguardian deployment/kafka -- kafka-consumer-groups --bootstrap-server localhost:29092 --list
```

## URL du dashboard

```
https://logguardian.34-8-214-70.nip.io
```

## Nettoyage après démo

```powershell
kubectl delete namespace demo-apps

kubectl set env deployment/auto-remediator -n logguardian CRITICAL_THRESHOLD=2.0 WARN_THRESHOLD=1.3
kubectl scale deployment/email-sender -n logguardian --replicas=0

gsutil cp gs://logguardian-models-logguardian-497218/threshold.json ./threshold.json
notepad threshold.json
```

Remettre `"threshold": 1.4713505506515503`, enregistrer.

```powershell
gsutil cp ./threshold.json gs://logguardian-models-logguardian-497218/threshold.json
kubectl rollout restart deployment/ml-model -n logguardian
```

Fermer les terminaux 2, 3, 4, 5 (Ctrl+C).

## Dépannage rapide

| Symptôme | Commande |
|---|---|
| Port-forward mort | `Test-NetConnection -ComputerName kafka.logguardian.svc.cluster.local -Port 29092` |
| Pod bloqué | `kubectl describe pod -n demo-apps <nom>` |
| Rien dans Kafka | vérifier T3/T4 affichent `[START] Streaming demo-apps/...` |
| Rien en CRITICAL | baisser `CRITICAL_THRESHOLD` : `kubectl set env deployment/auto-remediator -n logguardian CRITICAL_THRESHOLD=1.5` |
| Cooldown actif | attendre 180s ou changer de pod cible |
| Resize cluster bloqué | `gcloud container operations list --zone=europe-west1-b --project=logguardian-497218 --filter="status=RUNNING"` |
| Email/ticket jamais envoyé / erreur 403 SendGrid | quota trial 100/jour probablement épuisé — vérifier https://app.sendgrid.com/email_activity, sauter l'étape en démo si besoin |
| email-sender ou ticket-sender tourne encore après la démo | `kubectl get pods -n logguardian -l app=email-sender` / `-l app=ticket-sender` doivent être vides — sinon `kubectl scale deployment/<nom> -n logguardian --replicas=0` |
