# 18 — Troubleshooting

## Pod en CrashLoopBackOff

### Voir les logs

```bash
kubectl logs deployment/<service> -n logguardian --tail=50
kubectl logs pod/<pod-name> -n logguardian --previous     # logs du dernier crash
kubectl describe pod <pod-name> -n logguardian
```

### Cas connus

#### `ml-model` : `cannot import name 'NoBrokersAvailable'`

Cause : `kafka-python` 3.0.0 a retiré cette classe.
Fix : pin `kafka-python==2.0.2` dans `ml-model/requirements.txt` (déjà fait).

#### `ml-model` : `NumPy 2.x incompatible with torch`

Cause : `numpy` sans pin installe 2.x, incompatible avec `torch==2.2.2`.
Fix : `numpy<2` dans `requirements.txt`.

#### `ml-model` : `Missing key(s) in state_dict: 'attention.W'`

Cause : le modèle dans GCS est l'ancienne architecture (sans bidirectionnel + attention).
Fix : réentraîner avec la nouvelle architecture (voir [14 — Réentraînement](14-reentrainement.md)).

#### `email-sender` : `ModuleNotFoundError: No module named 'sendgrid'`

Cause : image Docker pas encore rebuildée avec la nouvelle `requirements.txt`.
Fix : attendre le Cloud Build OU déclencher manuellement :
```bash
gcloud builds submit . --config=cloudbuild.yaml --project=logguardian-497218
```

#### `email-sender` : `SMTP connection refused`

Cause : GCP bloque les ports SMTP sortants.
Fix : utiliser SendGrid API (HTTPS) — déjà migré.

## Pas de logs `stdout` qui apparaissent

Cause : Python bufferise par défaut.
Fix : `ENV PYTHONUNBUFFERED=1` dans le Dockerfile (ou env var dans le manifest).

## Pas d'emails reçus

### Checklist

1. **Pod tourne ?** `kubectl get pods -n logguardian`
2. **Anomalies sur Kafka ?** Voir `kubectl exec ... kafka-console-consumer.sh ...`
3. **Logs du pod ?** `kubectl logs deployment/email-sender -n logguardian -f` doit afficher `Batch email sent: N incidents`
4. **SendGrid sender vérifié ?** Dashboard SendGrid → Sender Authentication
5. **API Key correcte ?** `kubectl get secret monitoring-ui-secrets -o jsonpath='{.data.SENDGRID_API_KEY}' | base64 -d`
6. **Boîte spam vérifiée ?**

### Réduire la fenêtre pour tester

```bash
kubectl set env deployment/email-sender BATCH_INTERVAL_SECONDS=60 -n logguardian
kubectl rollout restart deployment/email-sender -n logguardian
```

## Dashboard inaccessible

### EXTERNAL-IP en `<pending>` longtemps

Cause : LoadBalancer GCP en cours de provisioning (~2 min).
Fix : attendre.

### Page blanche ou erreur 502

Cause : ReadinessProbe échoue. Vérifier les logs du pod.
```bash
kubectl describe pod <pod> -n logguardian
kubectl logs deployment/monitoring-ui -n logguardian --tail=30
```

### Login refusé

Cause : credentials env vars incorrects ou `DASH_SECRET_KEY` manquant.
Fix : vérifier le secret K8s :
```bash
kubectl get secret monitoring-ui-secrets -n logguardian -o jsonpath='{.data.LOGIN_USERNAME}' | base64 -d
kubectl get secret monitoring-ui-secrets -n logguardian -o jsonpath='{.data.DASH_SECRET_KEY}' | base64 -d
```

## Trop d'anomalies (faux positifs)

Voir [13 — Modèle](13-modele-ml.md#modifier-le-seuil-sans-réentraîner) — augmenter le seuil dans `threshold.json`.

## Logs Android n'apparaissent pas

Le parser `android_parser.py` ne descend pas dans les sous-dossiers `issue_N/`.
Fix : utiliser `os.walk()` à la place de `os.listdir()` (voir commit récent).

Et **`android` doit être dans `LOG_SOURCES`** (`k8s/log-generator.yaml`).

⚠️ Si Android est activé mais le modèle ne le connaît pas, ses logs ne contribuent pas correctement au scoring. Voir [14 — Réentraînement](14-reentrainement.md).

## Cloud Build échoue

```bash
gcloud builds list --project=logguardian-497218 --limit=5
gcloud builds log <BUILD_ID> --project=logguardian-497218
```

### Cas connus

#### `key in the template 'REGISTRY' is not a valid built-in substitution`

Cause : variable bash en MAJUSCULE = Cloud Build la traite comme substitution.
Fix : lowercase (`registry=...`) — déjà fait dans `cloudbuild.yaml`.

#### `denied: Permission "artifactregistry.repositories.uploadArtifacts" denied`

Cause : compte Cloud Build sans le rôle.
Fix :
```bash
PROJECT_NUMBER=$(gcloud projects describe logguardian-497218 --format='value(projectNumber)')
gcloud projects add-iam-policy-binding logguardian-497218 \
    --member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer"
```

## Kafka : consumer ne consomme pas

```bash
# Vérifier que le topic a des messages
kubectl exec -n logguardian deployment/kafka -- \
    kafka-console-consumer.sh --bootstrap-server localhost:29092 \
    --topic logs-anomalies-ml --max-messages 1 --timeout-ms 5000

# Vérifier le lag
kubectl exec -n logguardian deployment/kafka -- \
    kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
    --describe --group ml-model
```

Si `LAG` augmente : consumer trop lent ou crashé.

## Espace disque saturé sur les nœuds

Vérifier la rétention Kafka (`KAFKA_LOG_RETENTION_HOURS`, `KAFKA_LOG_RETENTION_BYTES`).
Le PVC `log-data-pvc` (10Gi) peut aussi remplir vite si tu changes les sources.

```bash
kubectl get nodes -o wide
kubectl describe node <node-name> | grep -A 5 "Allocated resources"
```

## Le pod log-generator boucle sans envoyer

Vérifier que `/data` est rempli :
```bash
kubectl exec -n logguardian deployment/log-generator -c log-generator -- ls /data/
```

Si vide → l'initContainer `sync-log-data` a échoué. Vérifier :
```bash
kubectl logs <pod> -c sync-log-data -n logguardian
```

Causes fréquentes : Workload Identity mal configuré, bucket vide, droits manquants sur le SA.
