#!/usr/bin/env bash
set -euo pipefail
# Explicit local-only deployment; --activate enables collection schedules only.
cd "$(dirname "$0")/../.."
test -f .env || { echo 'Missing local .env' >&2; exit 1; }
test -f artifacts/low-memory/kafka-verified.json || { echo 'Verify the two-broker migration first' >&2; exit 1; }
for image in polyhorizon-ingestion:reliability-fix spark-custom:low-memory-local polyhorizon-features:low-memory-local polyhorizon-prefect-server:latest; do
  docker image inspect "$image" >/dev/null
done
docker network inspect polyhorizon_ml-platform >/dev/null
docker compose exec -T prefect-server python - --env-file "$PWD/.env" --low-memory "$@" < scripts/deploy/register_local.py
docker compose -f docker-compose.yaml -f docker-compose.local-workers.yaml -f docker-compose.low-memory.yaml up -d \
  local-worker-ingestion local-worker-streaming
echo 'Local deployments registered. See schedules_active above; feature publication and training remain gated.'
