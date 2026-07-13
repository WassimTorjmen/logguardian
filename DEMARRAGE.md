# Démarrage LogGuardian

1. Copier `.env.example` en `.env` et renseigner les variables SMTP.
2. Lancer :

```powershell
docker compose down --remove-orphans
docker compose build --no-cache ml-model
docker compose up -d
```

3. Vérifier :

```powershell
docker compose ps
docker compose logs ml-model --tail=100
docker compose logs monitoring-ui --tail=50
docker compose logs email-sender --tail=50
```

L'interface est sur http://localhost:8050 et Kafka UI sur http://localhost:8080.
