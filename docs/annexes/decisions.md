# Historique des décisions

Cette page documente les choix techniques importants et leurs raisons. Utile pour comprendre pourquoi le code est comme il est, et éviter de refaire les mêmes erreurs.

## Round-robin entre sources (log-generator)

**Problème** : `itertools.chain.from_iterable()` traitait les sources séquentiellement → toute la source `linux` épuisée avant de passer à `ssh`, etc. L'UI affichait des heures de logs Linux, puis rien d'autre.

**Solution** : générateur `_roundrobin()` qui alterne une entrée par source à chaque tour.

**Date** : 2026-06.

## Déduplication monitoring-ui

**Problème** : le thread Kafka ajoutait chaque message sans vérification → doublons dans le buffer si Kafka rejouait.

**Solution** : `set` `_seen` indexé par `(detected_at[:19], source, host, score formatté)`, purgé quand > 2 × MAX_ROWS.

**Date** : 2026-06.

## Migration AWS → GCP

**Contexte** : projet démarré sur AWS (S3, ECR, EKS), migré vers GCP (GCS, Artifact Registry, GKE).

**Raison** : crédits étudiants GCP plus généreux.

**Conséquences** :
- Le code legacy `checkpoint.py` parle encore à S3 (warning silencieux)
- La branche `develop` est la branche GCP, certaines anciennes branches contiennent du code AWS

## CPU-only PyTorch

**Problème** : `torch==2.2.2` installe les wheels CUDA par défaut → 2 Go par image → Cloud Build ~10 min.

**Solution** :
```
--extra-index-url https://download.pytorch.org/whl/cpu
torch==2.2.2+cpu
```

**Date** : 2026-06. Build divisé par ~2.

## kafka-python pinned à 2.0.2

**Problème** : `kafka-python` 3.0.0 a retiré `NoBrokersAvailable` → ImportError au démarrage de ml-model.

**Solution** : pin `kafka-python==2.0.2`.

**Date** : 2026-06.

## numpy < 2

**Problème** : `numpy` 2.x incompatible avec `torch` 2.2.x → erreurs au runtime.

**Solution** : pin `numpy<2`.

**Date** : 2026-06.

## Docker layer caching dans Cloud Build

**Problème** : Cloud Build rebuildait toutes les couches à chaque commit (10 min/build).

**Solution** : `docker pull :latest` + `--cache-from :latest` pour réutiliser les couches.

**Date** : 2026-06. Build divisé par ~3.

## SendGrid au lieu de SMTP

**Problème** : GCP bloque les ports SMTP sortants (25, 465, 587) → emails impossibles via Gmail SMTP.

**Solution** : SendGrid API HTTPS (port 443).

**Date** : 2026-06.

## Batching des emails (15 min)

**Problème** : un email par anomalie détectée → spam intolérable (potentiellement 1000+/h).

**Solution** : batch sur fenêtre glissante de 15 min, un seul email récap avec tous les incidents.

**Date** : 2026-06.

## Architecture LSTM : bidirectionnel + attention

**Problème** : modèle initial (LSTM unidirectionnel simple) avait du mal à distinguer signal vs bruit.

**Solution** : encoder bidirectionnel (2 couches) + attention sur la sortie encoder. `N_FEATURES` reste à 77 mais le state dict change (clés `attention.W`, `encoder.weight_ih_l0_reverse`, etc.).

**Date** : 2026-06.

## Seuil multiplié pour réduire les faux positifs

**Problème** : `threshold = p95` → par construction 5% des logs normaux sont des "anomalies". En pratique > 50% en prod.

**Solution temporaire** : éditer `threshold.json` (×2 ou ×3).

**Solution propre** : `THRESHOLD_PCT = 99` au prochain réentraînement.

**Date** : 2026-06.

## Authentification UI (Flask session)

**Contexte** : dashboard exposé publiquement via LoadBalancer.

**Solution** : login simple `LOGIN_USERNAME` / `LOGIN_PASSWORD`, session signée par `DASH_SECRET_KEY`, callbacks Dash pour login/logout.

**Limite** : un seul compte, pas de gestion multi-utilisateur. Pour la prod réelle, mettre IAP devant.

**Date** : 2026-06.

## Pas de PVC pour Kafka

**Choix** : conserver le manifeste sans persistance pour éviter la complexité d'un StatefulSet.

**Conséquence** : à chaque restart de Kafka, tous les messages et offsets sont perdus. Les consumers reprennent à `latest`.

**Si on veut persister** : convertir le `Deployment` en `StatefulSet` avec `volumeClaimTemplates`.

## Android désactivé temporairement

**Problème** : `android` ajouté dans `LOG_SOURCES` et parser créé, mais `SOURCES` dans `features.py` ne contient pas `"android"` → one-hot à zéro → mauvaise reconstruction → faux positifs en cascade.

**Solution temporaire** : retiré de `LOG_SOURCES`.

**Solution propre** : ajouter `"android"` à `SOURCES`, réentraîner.

**Date** : 2026-06.
