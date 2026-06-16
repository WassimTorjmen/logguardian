# Service — log-generator

Producteur de logs : lit des fichiers Loghub et publie sur Kafka.

## Rôle

Simule un flux de logs continu en relisant les datasets Loghub. Plusieurs parsers (un par source), interleaving round-robin, vitesse de replay configurable.

## Code

```
log-generator/
├── Dockerfile
├── requirements.txt
└── src/
    ├── main.py              # Boucle de publication Kafka
    ├── config.py            # Variables d'env
    ├── checkpoint.py        # Persistance offset (S3, legacy)
    ├── producer.py          # Wrapper confluent_kafka Producer
    └── parsers/
        ├── base.py          # BaseParser + LogEntry dataclass
        ├── linux_parser.py
        ├── ssh_parser.py
        ├── hadoop_parser.py
        ├── spark_parser.py
        ├── supercomputer_parser.py
        ├── hdfs_parser.py
        └── android_parser.py
```

## Données d'entrée

Dossier `/data/` (PVC en GKE, volume bind-mount en local) avec les fichiers Loghub :

```
/data/
├── Linux.log           # → LinuxParser
├── SSH.log             # → SSHParser
├── hadoop/             # → HadoopParser (récursif)
├── spark/              # → SparkParser
├── supercomputer/      # → SupercomputerParser (Thunderbird)
├── hdfs/               # → HDFSParser
└── Android/issue_N/    # → AndroidParser (récursif)
```

## Format de sortie (topic `logs-raw`)

```json
{
  "timestamp": "2026-06-17T14:23:01",
  "source": "linux",
  "host": "combo",
  "level": "INFO",
  "component": "sshd",
  "message": "Accepted password for user root from 192.168.1.10",
  "raw": "Jun 17 14:23:01 combo sshd[1234]: Accepted password..."
}
```

## Round-robin entre sources

Pour éviter de traiter une source entièrement avant la suivante, le générateur alterne :

```python
def _roundrobin(*iterables):
    pending = [iter(it) for it in iterables]
    while pending:
        next_pending = []
        for it in pending:
            try:
                yield next(it)
                next_pending.append(it)
            except StopIteration:
                pass
        pending = next_pending
```

## En GKE

- ConfigMap `log-generator-config` : sources actives, vitesse, bucket GCS
- InitContainer `sync-log-data` : `gsutil rsync` au premier démarrage, marqueur `.sync-done` pour skip après
- PVC `log-data-pvc` 10Gi : persistance des données entre redémarrages
- ServiceAccount `log-generator` lié à `terraform-sa` (Workload Identity)

## Logs typiques

```
INFO log-generator: Sent 1000 messages (cycle=0 offset=1000)
WARNING checkpoint: Failed to save checkpoint: Unable to locate credentials
```

Le warning `checkpoint` est normal et non bloquant (le checkpoint S3 est legacy).

## Personnaliser les sources

```yaml
# k8s/log-generator.yaml → ConfigMap
LOG_SOURCES: linux,ssh,hadoop,supercomputer,spark
```

Puis :
```bash
kubectl apply -f k8s/log-generator.yaml
kubectl rollout restart deployment/log-generator -n logguardian
```
