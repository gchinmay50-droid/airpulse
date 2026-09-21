# Deploying the public dashboard (free, no card)

Flink has no free tier anywhere, so the split is:

```
laptop  : Redpanda + Flink + producer + sink   -->  publisher (every 5 min)
                                                        |
cloud   : Neon Postgres (gold tables only)  <-----------+
          Render web service: FastAPI + dashboard, reads Neon
```

The public site is always up. While the laptop pipeline is running it is
current; when it is not, the header says "stale" with the real age instead of
pretending. That honesty is part of the design, not a limitation to hide.

## 1. Neon — the cloud Postgres (2 minutes)

1. https://neon.tech → sign up (GitHub login, no card) → **New project**, name `airpulse`, region Singapore (closest to India).
2. Copy the connection string from the dashboard. It looks like
   `postgresql://neondb_owner:...@ep-....ap-southeast-1.aws.neon.tech/neondb?sslmode=require`

## 2. Publish the gold tables from the laptop

```bash
# .env — add the line:
CLOUD_DATABASE_URL=postgresql://...neon.tech/neondb?sslmode=require

docker compose --profile cloud up -d      # starts airpulse-publisher
docker logs -f airpulse-publisher         # first run copies the last 7 days, then every 5 min
```

The publisher creates the tables itself (same DDL as `sql/`), so Neon needs
no manual setup.

## 3. Render — the web service (5 minutes)

1. https://render.com → sign up with GitHub (no card for the free plan).
2. **New → Blueprint** → select the `airpulse` repo. Render reads `render.yaml`.
3. When it asks for `DATABASE_URL`, paste the Neon string from step 1.
4. Deploy. First build ~3 min. The URL is `https://airpulse-<something>.onrender.com`.

Free-plan behaviour to know: the service sleeps after 15 min without traffic
and the first request afterwards takes ~30–50 s. For a resume link that is
acceptable; put the health page link second so the second click is instant.

## 4. Point the README at it

Replace the placeholder in README.md with the Render URL and push. Every push
to `main` redeploys automatically (`autoDeploy: true`).

## Alternatives, same Dockerfile

- **Google Cloud Run** (needs a card on file, bills $0 at this traffic):
  `gcloud run deploy airpulse --source . --region asia-south1 --allow-unauthenticated --set-env-vars DATABASE_URL=...`
- **Fly.io / Koyeb**: `fly launch` detects the Dockerfile; set `DATABASE_URL` as a secret.
