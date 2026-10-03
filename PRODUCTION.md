# Production Deployment Guide

Gold-to-Feast publication is now completion-driven. Rebuild both Spark and
features images, redeploy Prefect to remove the old feature clock schedule, and
apply the updated Feast source. See the [dataset publication runbook](docs/DATASET_PUBLICATION.md)
for paths, dataset versions, migration behavior, and pending-release recovery.

This guide covers how to deploy PolyHorizon using Docker Compose in production.

Environment files are excluded from Docker build contexts (including nested
`.env` and `.env.*` files). Supply credentials at runtime through the service's
`env_file`/secret configuration; never bake them into images. Recreating a
container is necessary to apply changed environment values. A credential-only
`.env.prod` is not a complete deployment configuration: populate the remaining
settings from `.env.prod.template` and run production validation first.

For local startup, the serving configuration also needs its model-registry,
Feast, forecast and inference settings from `.env.example`, plus a reachable
registry containing a compatible champion model and populated feature stores.
Starting an empty infrastructure stack does not create these model/data assets.

The repository builds a small local MLflow image from
`docker/mlflow/Dockerfile` because the upstream server image does not include the
PostgreSQL and S3 Python drivers required by this Compose configuration. The
image also pins MLflow 3.9.0 to match the repository client; an older server
does not implement the logged-model API used by that client.

## Strategy
- Local development uses `docker-compose.yaml`; every PolyHorizon service is
  built from the working tree and tagged `:local`.

- Production uses `docker-compose.prod.yaml`, Traefik, and release images built
  in CI and pushed to GHCR.

- **Deploy by pulling images** on the production host.

- Use a **separate env file** (e.g., `.env.prod`) for secrets and prod settings.

- Production accepts only the full immutable `sha-<40-hex-commit>` tag. Set
  `IMAGE_TAG` in `.env.prod` to that tag.

## Deployment Contract

Operator credentials are separate from forecast credentials. Before deploying
the redesigned frontend/serving release, move `SERVING_RELOAD_API_KEY` into
`SERVING_OPERATOR_API_KEYS` and remove it from `SERVING_API_KEYS`. Provision
distinct consumer keys as needed. Startup rejects overlapping key lists and
enabled privileged capabilities without operator keys; production validation
requires the reload key in the operator list. See
[`docs/FRONTEND_REDESIGN.md`](docs/FRONTEND_REDESIGN.md) for the `/ops` console.

A serving container is ready only when all of these are true:

1. The process has loaded a model successfully.
2. The loaded version is the version currently assigned to MLflow
   `@champion`.
3. `GET /v1/health/ready` returns HTTP 200 and reports both versions.

The readiness endpoint returns HTTP 503 when no model is loaded, the registry
cannot resolve `@champion`, or the loaded version is stale. Both Compose and the
rolling deployment script use this endpoint; `/v1/health` is informational and
is not a deployment gate.

Governance approval and traffic activation are separate. The activation task
loads the approved exact version on every serving replica using
`POST /v1/model/prepare`, checks the registry alias has not changed, then moves
`@champion` and calls `POST /v1/model/activate` on every replica. It verifies
`/v1/health/ready` on each replica. If activation fails, it restores the prior
alias and reloads replicas; operators must investigate any rollback failure.
The old model can remain ready for a bounded transition while the prepared
version matches the alias. Production sets `SERVING_ACTIVATION_MODE=reload`.
Operators may use `scripts/deploy/serving_rolling_update.sh` when an image
rollout is also required. Manual `POST /v1/model/reload` remains a recovery
operation after an alias move, not the normal promotion path.


## Prerequisites
- Docker + Docker Compose plugin installed on the server.

- `ml-platform` network created:
```bash
docker network create ml-platform
```

- A production environment file (not committed):
  - `.env.prod`

You can start from the template:
```bash
python3 scripts/deploy/prepare_prod_env.py
bash scripts/deploy/validate_prod_env.sh .env.prod
```
The preparation command merges new template keys into an existing ignored
`.env.prod` without replacing existing credentials and restricts the file to
owner access. Fill all reported placeholders, set a real 40-character image
SHA and deployment domains, then rerun validation. Deployment scripts load the
Compose-style env file as data; do not `source` it as shell code. No real
credentials are committed to this repository.

CI publishes an artifact with `IMAGE_TAG` prefilled for the latest main build:
- Artifact name: `env-prod-template`
- File: `.env.prod.template`

## Production CI/CD

The `PolyHorizon CI and Release` workflow is the release gate. Pull requests run
Python and frontend checks, validate shell scripts and the production Compose
model, and verify that CI, Compose, and Prefect use the same image contract.
The repository-wide mypy check is currently advisory because the existing code
base has an unresolved type-error baseline; lint and tests remain blocking.

After those checks pass on `main`, CI atomically publishes all release-owned
images with the same immutable `sha-<40-character-commit>` tag. The uploaded
release artifact contains `release.json` and a production environment template
prefilled with that tag. A tag is deployable only after every image succeeds.

Use the `Deploy Production` workflow to deploy:

1. Copy the immutable tag from the successful release artifact.
2. Trigger the workflow and provide the full tag.
3. Approve the protected `production` environment when prompted.

The workflow verifies that every image exists before contacting production,
serializes deployments, updates `.env.prod`, synchronizes Prefect work pools and
deployments, runs health checks, and automatically restores the previous image
tag if deployment or verification fails.

### Local Runner (Recommended for Local Stack)
If you do not have a VPS/EC2/bare‑metal server, run GitHub Actions locally using a self‑hosted runner.

1. Create the runner in GitHub:
   - Repo → Settings → Actions → Runners → New self‑hosted runner
2. Install it locally (follow GitHub’s instructions).
3. Keep the runner running:
   ```bash
   ./run.sh
   ```
4. Deploy workflows now run on your machine (we set `runs-on: self-hosted`).

Optional: install as a macOS launchd service using:
- `scripts/deploy/github-runner-launchd.plist`


## Recommended Deployment Flow
```bash
COMPOSE_FILE=docker-compose.prod.yaml \
ENV_FILE=.env.prod \
scripts/deploy/prod_deploy.sh
```

This script:
1. Pulls the selected immutable release images.
2. Starts services in explicit phases (infra → monitoring → orchestration → apps).
3. Creates/validates Prefect Docker work pools and deploys `prefect.yaml` with
   the same immutable release images.
4. Runs health checks (if domains are configured in `.env.prod`).

Prefect flow containers mount `PREFECT_JOB_ENV_FILE` at `/app/.env` read-only.
Set it to an absolute host path accessible to the Docker daemon; the deployment
script defaults it to `${DEPLOY_PATH}/.env.prod`. Work-pool concurrency limits
are synchronized on every release so stateful ingestion, feature, and training
runs cannot overlap.

## Health Check URLs
The deploy script checks these when configured:
- `https://${POLYHORIZON_DOMAIN}/health` (frontend)
- `https://${POLYHORIZON_DOMAIN}/v1/health/ready` (serving readiness)
- `https://${MLFLOW_DOMAIN}/api/2.0/mlflow/experiments/list`
- `https://${PREFECT_DOMAIN}/api/health`
- `https://${GRAFANA_DOMAIN}/api/health`
- `https://${PROMETHEUS_DOMAIN}/-/healthy`
- `https://${ALERTMANAGER_DOMAIN}/-/healthy`

Traefik is checked through its container-native `/ping` health check rather
than exposing an unauthenticated health endpoint on the dashboard router.


## Rolling Updates
Serving uses a readiness-gated surge rollout (three replicas during replacement,
two at steady state by default). Frontend has its own rolling helper:
```bash
scripts/deploy/serving_rolling_update.sh
scripts/deploy/frontend_rolling_update.sh
```

## Streaming Service
The long‑running Spark streaming container is disabled by default in prod and can be enabled on demand:
```bash
docker compose -f docker-compose.prod.yaml --profile manual-streaming up -d spark-streaming-service
```

If Prefect schedules are active, keep the streaming service disabled and let Prefect handle start/stop.


## Rollback

`scripts/deploy/deploy_release.sh` automatically rolls back application images
and Prefect deployments when a deployment fails. It records the last successful
tag in `.previous-production-image-tag`.

For an operator-initiated rollback, dispatch `Deploy Production` with a previous
release tag. Manual image pinning remains available:
```bash
docker compose -f docker-compose.prod.yaml --env-file .env.prod up -d
```

## GitHub repository setup

- Create a protected GitHub environment named `production` and configure its
  required reviewers.
- Add `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY`, and `DEPLOY_PATH` as
  production environment secrets.
- Set `POLYHORIZON_DOMAIN` as a production environment variable.
- Grant Actions read/write package access to GHCR.
- Ensure the deployment host is logged in to GHCR and has the external
  `ml-platform` Docker network.


## Security Notes
- Keep `.env.prod` outside of version control.

- Use strong values for `TRAEFIK_DASHBOARD_USERS` and Grafana admin creds.

- Restrict access to internal UIs via Traefik basic auth.


## Required Production Env Checklist

At minimum, ensure these are set correctly in `.env.prod`:
- `GITHUB_REPOSITORY`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `POSTGRES_SUPERUSER_PASSWORD`
- `POSTGRES_APP_DB`
- `POSTGRES_APP_USER`
- `POSTGRES_APP_PASSWORD`
- `POSTGRES_MLFLOW_DB`
- `POSTGRES_MLFLOW_USER`
- `POSTGRES_MLFLOW_PASSWORD`
- `POSTGRES_FEAST_DB`
- `POSTGRES_FEAST_USER`
- `POSTGRES_FEAST_PASSWORD`
- `POSTGRES_FEAST_REGISTRY_DB`
- `POSTGRES_FEAST_REGISTRY_USER`
- `POSTGRES_FEAST_REGISTRY_PASSWORD`
- `POSTGRES_MONITOR_USER`
- `POSTGRES_MONITOR_PASSWORD`
- `FEAST_REGISTRY_SCHEMA`
- `POSTGRES_PREFECT_DB`
- `POSTGRES_PREFECT_USER`
- `POSTGRES_PREFECT_PASSWORD`
- `MLFLOW_BUCKET_NAME`
- `FEAST_OFFLINE_BUCKET`
- `FEAST_REGISTRY_BUCKET`
- `SP500COMPANIES`
- `STREAMINGDATA_BUCKET`
- `POLYHORIZON_DOMAIN`
- `TRAEFIK_ACME_EMAIL`
- `TRAEFIK_DASHBOARD_DOMAIN`
- `TRAEFIK_DASHBOARD_USERS`
- `TRAEFIK_API_RATE_LIMIT_AVERAGE`
- `TRAEFIK_API_RATE_LIMIT_BURST`
- `TRAEFIK_API_MAX_INFLIGHT`
- `ALERTMANAGER_SLACK_WEBHOOK_FILE`
- `MLFLOW_DOMAIN`
- `PREFECT_DOMAIN`
- `GRAFANA_DOMAIN`
- `PROMETHEUS_DOMAIN`
- `ALERTMANAGER_DOMAIN`
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`
- `STREAMING_SPARK_IMAGE`
- `IMAGE_TAG`

Create the Alertmanager webhook secret before deployment:

```bash
mkdir -p monitoring/secrets
cp monitoring/secrets/alertmanager_slack_url.example \
  monitoring/secrets/alertmanager_slack_url
# Replace the placeholder in the copied file, then set:
# ALERTMANAGER_SLACK_WEBHOOK_FILE=./monitoring/secrets/alertmanager_slack_url
```

The deployment validator rejects a missing or placeholder webhook. Metrics and
Loki endpoints stay on the internal network; do not add public Traefik routers
for them. Operational UIs remain TLS-protected behind basic authentication.

## PostgreSQL backup and recovery

Run a checksummed logical backup with:

```bash
make postgres-backup ENV_FILE=.env.prod
```

Schedule that command from host cron/systemd (daily is the minimum recommended
cadence) and replicate the `postgres_backups` volume off-host. A Docker volume
on the same machine is not a disaster-recovery copy. Backups default to 14-day
retention via `POSTGRES_BACKUP_RETENTION_DAYS`.

Verify recovery regularly against a disposable database:

```bash
make postgres-restore-verify \
  ENV_FILE=.env.prod \
  BACKUP_PATH=/backups/20260722T020000Z
```

New clusters enable data checksums. Existing PostgreSQL volumes retain their
original checksum setting and require a planned offline migration to change it.
