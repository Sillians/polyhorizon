#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yaml}"
ENV_FILE="${ENV_FILE:-.env.prod}"
SERVICE="${SERVICE:-frontend}"
SCALE_UP="${SCALE_UP:-2}"
SCALE_TARGET="${SCALE_TARGET:-1}"
HEALTH_URL="${HEALTH_URL:-http://localhost/}"
WAIT_SECONDS="${WAIT_SECONDS:-120}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-5}"


echo "Pulling latest image for ${SERVICE}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" pull "$SERVICE"


echo "Scaling ${SERVICE} up to ${SCALE_UP}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --no-deps --scale "${SERVICE}=${SCALE_UP}" "$SERVICE"


echo "Waiting for health check at ${HEALTH_URL}..."
elapsed=0
until curl -fsS "$HEALTH_URL" > /dev/null; do
  sleep "$INTERVAL_SECONDS"
  elapsed=$((elapsed + INTERVAL_SECONDS))
  if [ "$elapsed" -ge "$WAIT_SECONDS" ]; then
    echo "Health check did not pass within ${WAIT_SECONDS}s"
    exit 1
  fi
done


echo "Scaling ${SERVICE} down to ${SCALE_TARGET}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --no-deps --scale "${SERVICE}=${SCALE_TARGET}" "$SERVICE"


echo "Rolling update complete."
