# 16 — Monitoring & alertes

## Cloud Monitoring

Dashboard pré-configuré disponible dans `monitoring/dashboard.json`.

URL : https://console.cloud.google.com/monitoring/dashboards?project=logguardian-497218

### Métriques surveillées

| Métrique | Source | Alerte |
|---|---|---|
| Container CPU usage | GKE | — |
| Container memory usage | GKE | — |
| Pod restart count | GKE | > 3 / 5 min |
| Disk pressure | Nodes | > 85% |
| Kafka log retention bytes | Container metrics | — |

### Alertes actives

- **DiskPressure** : `monitoring/alert-disk-pressure.json` — nœud > 85% disque
- **PodRestarts** : `monitoring/alert-pod-restarts.json` — > 3 redémarrages en 5 min

Pour les déployer :

```bash
gcloud alpha monitoring policies create \
    --policy-from-file=monitoring/alert-disk-pressure.json \
    --project=logguardian-497218

gcloud alpha monitoring policies create \
    --policy-from-file=monitoring/alert-pod-restarts.json \
    --project=logguardian-497218
```

## Cloud Logging

URL : https://console.cloud.google.com/logs?project=logguardian-497218

Filtres utiles :

```sql
-- Tous les logs du namespace
resource.type="k8s_container"
resource.labels.namespace_name="logguardian"

-- Erreurs uniquement
severity>=ERROR
resource.labels.namespace_name="logguardian"

-- Pour un service précis
resource.labels.container_name="ml-model"
```

## kubectl en direct

```bash
# Stream logs d'un pod
kubectl logs deployment/ml-model -n logguardian -f

# Logs avec timestamps
kubectl logs deployment/ml-model -n logguardian --timestamps --tail=50

# Logs d'un container précis dans un pod multi-container
kubectl logs deployment/log-generator -c sync-log-data -n logguardian

# Events du namespace (debug pod stuck/pending)
kubectl get events -n logguardian --sort-by='.lastTimestamp' | tail -30

# Top resource usage
kubectl top pods -n logguardian
kubectl top nodes
```

## Healthchecks

### monitoring-ui

```yaml
readinessProbe:
  httpGet:
    path: /
    port: 8050
  initialDelaySeconds: 10
  periodSeconds: 5
```

### kafka

```yaml
readinessProbe:
  tcpSocket:
    port: 29092
  initialDelaySeconds: 30
  periodSeconds: 10
```

Les autres services n'ont pas de probes (pas de port HTTP).

## Métriques applicatives

Pas de Prometheus actuellement. Pour ajouter :
1. Sidecar `prometheus-statsd-exporter`
2. Ou directement `prometheus-client` dans chaque service Python
3. Scraper avec Managed Prometheus (GCP) ou un Prometheus auto-hébergé

## Logs structurés

Les services Python utilisent `logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s: %(message)s")`.

Pour des logs structurés JSON (mieux pour BigQuery / Cloud Logging) :
```python
import json
log.info(json.dumps({"event": "anomaly_published", "score": 1.24}))
```

## Suivi des emails SendGrid

Dashboard : https://app.sendgrid.com/email_activity

Affiche pour chaque email :
- Status (delivered, bounced, blocked)
- Open rate (si tracking activé)
- Spam reports

Quota du trial : 100 emails/jour.
