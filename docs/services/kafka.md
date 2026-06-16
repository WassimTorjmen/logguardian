# Service — Kafka

Bus de messages central — orchestré via Zookeeper.

## Configuration

| Paramètre | Valeur | Note |
|---|---|---|
| Image | `confluentinc/cp-kafka:7.5.0` | |
| Broker port | `29092` (interne K8s) | `9092` aussi exposé en local Docker |
| Zookeeper | `zookeeper:2181` | Service séparé |
| Replication factor | 1 | Single-broker, dev/démo |
| Log retention | 1h (GKE) / 24h (local) | Économie espace |
| Log retention bytes | 512 MB | |
| Auto create topics | `true` | Les topics sont créés à la volée |

## Topics

| Topic | Partitions | Retention | Producteur | Consommateur |
|---|---|---|---|---|
| `logs-raw` | 1 | 1h | log-generator | etl-processor |
| `logs-processed` | 1 | 1h | etl-processor | ml-model |
| `logs-anomalies` | 1 | 1h | etl-processor | (debug) |
| `logs-anomalies-ml` | 1 | 1h | ml-model | monitoring-ui, email-sender |

## En GKE

- Pas de PVC → données et offsets perdus à chaque restart de pod
- Consumers en `auto.offset.reset: latest` → reprennent au dernier message au démarrage
- Strategy `Recreate` (pas de rolling update — impossible avec un seul broker stateful)

## Inspecter les topics

### Lister les topics

```bash
kubectl exec -n logguardian deployment/kafka -- \
    kafka-topics.sh --bootstrap-server localhost:29092 --list
```

### Lire les messages d'un topic

```bash
kubectl exec -n logguardian deployment/kafka -- \
    kafka-console-consumer.sh \
    --bootstrap-server localhost:29092 \
    --topic logs-anomalies-ml \
    --from-beginning \
    --max-messages 5 \
    --timeout-ms 5000
```

### Détails d'un topic

```bash
kubectl exec -n logguardian deployment/kafka -- \
    kafka-topics.sh --bootstrap-server localhost:29092 \
    --describe --topic logs-processed
```

### Lag des consumers

```bash
kubectl exec -n logguardian deployment/kafka -- \
    kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
    --describe --group ml-model
```

## En local — Kafka UI

Docker Compose lance aussi `kafka-ui` sur http://localhost:8080 — interface graphique pour browser topics, consumer groups, messages.

## Persistance (pour la prod)

Ajouter un PVC dans `k8s/kafka.yaml` :

```yaml
volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      accessModes: [ReadWriteOnce]
      storageClassName: standard-rwo
      resources:
        requests:
          storage: 10Gi
```

Et convertir `Deployment` en `StatefulSet`.
