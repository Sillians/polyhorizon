#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yaml}"
ENV_FILE="${ENV_FILE:-.env.prod}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-240}"
HEALTH_INTERVAL_SECONDS="${HEALTH_INTERVAL_SECONDS:-5}"
ALLOW_LATEST_TAG="${ALLOW_LATEST_TAG:-false}"

if [ ! -f "$ENV_FILE" ]; then
  echo "ENV file not found: $ENV_FILE"
  echo "Set ENV_FILE to a valid path (e.g., .env.prod)."
  exit 1
fi

echo "Using compose file: $COMPOSE_FILE"
echo "Using env file: $ENV_FILE"

while IFS= read -r -d '' entry; do
  export "$entry"
done < <(python3 "$(dirname "$0")/load_env.py" "$ENV_FILE")

bash "$(dirname "$0")/validate_prod_env.sh" "$ENV_FILE"

if [ -z "${IMAGE_TAG:-}" ]; then
  echo "IMAGE_TAG is not set. Production deployments must use immutable tags."
  exit 1
fi

if [ "${IMAGE_TAG}" = "latest" ] && [ "${ALLOW_LATEST_TAG}" != "true" ]; then
  echo "IMAGE_TAG is set to 'latest'. Set ALLOW_LATEST_TAG=true to override."
  exit 1
fi

echo "Pulling immutable release images for ${IMAGE_TAG}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" pull

echo "Starting core infrastructure..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  postgres zookeeper kafka-broker-1 kafka-broker-2 kafka-broker-3 kafka-init schema-registry \
  master volume filer seaweedfs-s3 seaweedfs-bucket-init redis

echo "Reconciling PostgreSQL roles, databases, and grants..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres \
  bash /docker-entrypoint-initdb.d/reconcile.sh

echo "Starting observability stack..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  docker-socket-proxy loki alloy postgres-exporter prometheus alertmanager prometheus-pushgateway grafana

echo "Starting orchestration + MLflow..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  prefect-server prefect-worker-training prefect-worker-features prefect-worker-ingestion prefect-worker-streaming \
  mlflow

if [ "${DEPLOY_PREFECT_FLOWS:-true}" = "true" ]; then
  echo "Synchronizing Prefect work pools and deployments..."
  COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" \
    bash "$(dirname "$0")/deploy_prefect_flows.sh"
fi

echo "Starting application services..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  spark-master spark-worker-1 spark-worker-2 frontend traefik

echo "Deploying serving replicas through readiness-gated rolling update..."
COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" \
  bash "$(dirname "$0")/serving_rolling_update.sh"

declare -A health_urls=()

if [ -n "${POLYHORIZON_DOMAIN:-}" ]; then
  health_urls["frontend"]="https://${POLYHORIZON_DOMAIN}/health"
  health_urls["serving"]="https://${POLYHORIZON_DOMAIN}/v1/health/ready"
fi
# Internal UIs are protected with basic auth at the edge. Their container state
# is checked below instead of sending unauthenticated public health requests.
if [ "${#health_urls[@]}" -eq 0 ]; then
  echo "No external health check URLs configured; using container health checks."
fi

echo "Running health checks..."
for name in "${!health_urls[@]}"; do
  url="${health_urls[$name]}"
  elapsed=0
  echo "Checking ${name} at ${url}"
  curl_args=(-fsS)
  if [[ "$name" == "serving" ]]; then
    curl_args+=(-H "${SERVING_API_KEY_HEADER:-X-API-Key}: ${SERVING_RELOAD_API_KEY}")
  fi
  until curl "${curl_args[@]}" "$url" > /dev/null; do
    sleep "$HEALTH_INTERVAL_SECONDS"
    elapsed=$((elapsed + HEALTH_INTERVAL_SECONDS))
    if [ "$elapsed" -ge "$HEALTH_TIMEOUT_SECONDS" ]; then
      echo "Health check failed for ${name} after ${HEALTH_TIMEOUT_SECONDS}s"
      exit 1
    fi
  done
  echo "OK: ${name}"
done

for service in \
  postgres-exporter prometheus alertmanager prometheus-pushgateway grafana loki alloy \
  mlflow prefect-server frontend traefik; do
  container_id="$(docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps -q "$service")"
  elapsed=0
  while true; do
    status="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || true)"
    if [ "$status" = "running" ] && { [ "$health" = "healthy" ] || [ "$health" = "none" ]; }; then
      break
    fi
    if [ "$status" = "exited" ] || [ "$health" = "unhealthy" ] || [ "$elapsed" -ge "$HEALTH_TIMEOUT_SECONDS" ]; then
      echo "Container check failed for ${service}: status=${status:-missing}, health=${health:-missing}" >&2
      exit 1
    fi
    sleep "$HEALTH_INTERVAL_SECONDS"
    elapsed=$((elapsed + HEALTH_INTERVAL_SECONDS))
  done
done

echo "Production deployment complete."
