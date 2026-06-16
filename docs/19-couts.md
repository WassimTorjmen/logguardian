# 19 — Coûts & maintenance

## Coût mensuel estimé (GCP)

| Ressource | Conf | Coût ~ |
|---|---|---|
| GKE Standard (management) | 1 cluster zonal | gratuit |
| 2× `e2-standard-2` (24/7, non-préemptible) | 2 vCPU / 8 Go par nœud | ~50 €/mois |
| GCS (data + models) | ~10 Go | ~0,20 €/mois |
| Artifact Registry | ~5 Go d'images | ~0,50 €/mois |
| LoadBalancer (monitoring-ui) | forwarding rule + trafic | ~18 €/mois |
| Cloud Build | quota gratuit 120 min/jour | 0 € si < quota |
| Cloud Monitoring | quota gratuit | 0 € |
| **Total estimé** | | **~70 €/mois** |

## Réduire les coûts

### Mettre le cluster en veille

```bash
kubectl scale deployment --all --replicas=0 -n logguardian
gcloud container clusters resize logguardian \
    --node-pool=logguardian-nodes-standard \
    --num-nodes=0 \
    --zone=europe-west1-b --quiet
```

Économie : ~50 €/mois (les nœuds Compute Engine sont arrêtés).
Le management du cluster reste actif (gratuit) et les buckets persistent.

### Réveiller

```bash
gcloud container clusters resize logguardian \
    --node-pool=logguardian-nodes-standard \
    --num-nodes=2 \
    --zone=europe-west1-b --quiet
kubectl scale deployment --all --replicas=1 -n logguardian
```

Délai : ~2-3 min pour que les nœuds soient prêts + 1-2 min pour les pods.

### Supprimer le LoadBalancer

```bash
kubectl delete svc monitoring-ui -n logguardian
```

Économie : ~18 €/mois. Pour rétablir : `kubectl apply -f k8s/monitoring-ui.yaml`.
Alternative : utiliser `kubectl port-forward` pour les démos.

### Passer en préemptible

Modifier `terraform/gcp/gke.tf` : `preemptible = true`.
Économie : ~70% sur le coût compute. Inconvénient : les nœuds peuvent être interrompus à tout moment (24h max).

### Diminuer les replicas

Tous les déploiements sont à 1 replica → déjà au minimum.

## Maintenance régulière

### Mensuel

- Vérifier les coûts dans https://console.cloud.google.com/billing
- Nettoyer les vieux tags d'images :
  ```bash
  gcloud artifacts docker tags list \
      europe-west1-docker.pkg.dev/logguardian-497218/logguardian/ml-model \
      --project=logguardian-497218 --limit=20
  ```

### Trimestriel

- Mettre à jour les dépendances Python (`requirements.txt`)
- Mettre à jour les images de base (`python:3.11-slim` → dernier patch)
- Rebuilder + redéployer

### Sécurité

- Rotation des secrets (Groq, SendGrid, login) — voir [06 — Configuration](06-configuration.md#rotation-des-secrets)
- Audit des permissions IAM
- Vérifier les vulnérabilités des images dans Artifact Registry (scanner intégré)

## Suppression complète du projet

⚠️ Action irréversible. Détruit tout (cluster, données, modèles, IAM).

```bash
cd terraform/gcp
terraform destroy

# Si ça échoue (buckets non vides) :
gsutil -m rm -r gs://logguardian-datalake-logguardian-497218/
gsutil -m rm -r gs://logguardian-models-logguardian-497218/
terraform destroy
```

Ne pas supprimer le bucket `gs://logguardian-terraform-state-497218` si tu veux pouvoir réimporter le state plus tard.
