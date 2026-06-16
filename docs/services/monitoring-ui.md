# Service — monitoring-ui

Dashboard Dash interactif : visualisation des logs, anomalies, RAG explicatif, feedback.

## Rôle

1. Consommer `logs-anomalies-ml` en thread → buffer mémoire
2. Servir un dashboard Dash multi-pages (cockpit, flux logs, incident board)
3. Authentifier les utilisateurs (Flask session)
4. Permettre de demander une explication IA (Groq API) sur une anomalie
5. Tracer le feedback utilisateur en JSONL

## Code

```
monitoring-ui/
├── Dockerfile
├── requirements.txt
└── src/
    └── app.py              # ~2800 lignes — tout le dashboard
```

## Pages

| Page | URL (implicite) | Contenu |
|---|---|---|
| Login | `/` (si non authentifié) | Formulaire username/password |
| Vue cockpit | tab `dashboard` | KPIs, graphes par source, score moyen |
| Flux logs | tab `logs` | Table filtrée + panneau Analyste IA |
| Incident board | tab `alerts` | Liste cumulative des alertes critiques |

## Authentification

- Page login `_login_page()` avec `dcc.Input` username + password
- Callback `authenticate_user` : compare avec `LOGIN_USERNAME` / `LOGIN_PASSWORD` (env)
- Si OK : `session["authenticated"] = True`
- Callback `display_authenticated_content` : route entre login et layout selon session
- Bouton "Déconnexion" dans la sidebar → callback `logout_user` → `session.clear()`

**Note** : Flask session a besoin de `DASH_SECRET_KEY` (env) — sinon le cookie n'est pas signé.

## Consumer Kafka

Un thread démon démarré dans `_kafka_thread()` :
- Connecte un consumer avec un `group.id` unique (`monitoring-ui-<pid>-<ts>`) pour éviter le partage d'offsets entre instances
- Pour chaque message : déduplication par clé `(detected_at[:19], source, host, score formatté)`
- Si nouveau : ajout en tête du buffer (`deque(maxlen=2000)`)

## RAG (panneau Analyste IA)

- Clic sur "Analyser avec l'IA" → callback `_generate_rag_explanation`
- Construit un prompt contextualisé (source, hôte, score, séquence, exemples de feedbacks négatifs)
- Appelle Groq (`openai/gpt-oss-20b` par défaut)
- Affiche la réponse en streaming
- Si l'utilisateur clique "Pas utile" → régénère avec plus de tokens

## Feedback

Persisté dans `FEEDBACK_PATH` (par défaut `/app/feedback/rag_feedback.jsonl`) :

```jsonl
{"ts": "...", "log_id": "...", "verdict": "useful", "explanation": "..."}
{"ts": "...", "log_id": "...", "verdict": "not_useful", "user_comment": "..."}
```

En GKE : monté sur un `emptyDir` (perdu au pod restart). Pour persister, ajouter un PVC.

## En GKE

- ConfigMap `monitoring-ui-config` : `KAFKA_*`, `MAX_ROWS`, `REFRESH_INTERVAL_MS`
- Secret `monitoring-ui-secrets` : `GROQ_API_KEY`, `LOGIN_*`, `DASH_SECRET_KEY`
- Service type `LoadBalancer` (annotation `External`) → IP publique
- ReadinessProbe HTTP sur `/`
- Resources : 100m CPU / 128Mi RAM, limite 500m / 256Mi

## Sécurité

- Authentification simple : un seul compte (env vars)
- Pas de HTTPS natif — pour la prod, mettre un Ingress + cert-manager ou Cloudflare devant
- Pas de rate limiting sur l'API Groq côté UI — limité par la quota du compte
