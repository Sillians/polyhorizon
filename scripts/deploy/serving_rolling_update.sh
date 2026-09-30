#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yaml}"
ENV_FILE="${ENV_FILE:-.env.prod}"
SERVICE="${SERVICE:-model-serving}"
WAIT_SECONDS="${WAIT_SECONDS:-300}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-5}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Environment file not found: $ENV_FILE" >&2
  exit 1
fi

while IFS= read -r -d '' entry; do
  export "$entry"
done < <(python3 "$(dirname "$0")/load_env.py" "$ENV_FILE")

: "${IMAGE_TAG:?IMAGE_TAG is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${SERVING_RELOAD_API_KEY:?SERVING_RELOAD_API_KEY is required for readiness checks}"

if [[ ! "$IMAGE_TAG" =~ ^sha-[0-9a-f]{40}$ ]]; then
  echo "Production serving image must use an immutable sha-<40 hex> tag." >&2
  exit 1
fi

SCALE_TARGET="${SERVING_REPLICAS:-2}"
SCALE_UP="${SERVING_ROLLING_REPLICAS:-$((SCALE_TARGET + 1))}"
if (( SCALE_TARGET < 1 || SCALE_UP <= SCALE_TARGET )); then
  echo "SERVING_ROLLING_REPLICAS must be greater than SERVING_REPLICAS." >&2
  exit 1
fi

echo "Pulling immutable ${SERVICE} image ${IMAGE_TAG}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" pull "$SERVICE"

echo "Scaling ${SERVICE} to ${SCALE_UP} for readiness-gated replacement..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  --no-deps --remove-orphans --scale "${SERVICE}=${SCALE_UP}" "$SERVICE"

wait_for_replica() {
  local container_id=$1 elapsed=0
  while true; do
    state="$(docker inspect -f '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || true)"
    if [[ "$state" == "running" && "$health" == "healthy" ]]; then
      break
    fi
    if [[ "$state" == "exited" || "$health" == "unhealthy" || "$elapsed" -ge "$WAIT_SECONDS" ]]; then
      echo "Serving replica failed readiness: id=${container_id} state=${state:-missing} health=${health:-missing}" >&2
      return 1
    fi
    sleep "$INTERVAL_SECONDS"
    elapsed=$((elapsed + INTERVAL_SECONDS))
  done
}

mapfile -t replicas < <(
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps -q "$SERVICE"
)
if [[ "${#replicas[@]}" -ne "$SCALE_UP" ]]; then
  echo "Expected ${SCALE_UP} serving replicas, found ${#replicas[@]}." >&2
  exit 1
fi

expected_model_version="${EXPECTED_MODEL_VERSION:-}"
expected_image="ghcr.io/${GITHUB_REPOSITORY}/serving:${IMAGE_TAG}"
for container_id in "${replicas[@]}"; do
  wait_for_replica "$container_id"
  container_image="$(docker inspect -f '{{.Config.Image}}' "$container_id")"
  if [[ "$container_image" != "$expected_image" ]]; then
    echo "Replica ${container_id} runs ${container_image}; expected ${expected_image}." >&2
    exit 1
  fi
  model_version="$(
    docker exec "$container_id" python -c \
      "import json,urllib.request; print(json.load(urllib.request.urlopen('http://localhost:8000/v1/health/ready', timeout=5))['model_version'])"
  )"
  if [[ -z "$expected_model_version" ]]; then
    expected_model_version="$model_version"
  elif [[ "$model_version" != "$expected_model_version" ]]; then
    echo "Replica ${container_id} loaded model ${model_version}; expected ${expected_model_version}." >&2
    exit 1
  fi
done

echo "All replicas are ready with champion model version ${expected_model_version}."
echo "Scaling ${SERVICE} down to steady-state replica count ${SCALE_TARGET}..."
docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
  --no-deps --scale "${SERVICE}=${SCALE_TARGET}" "$SERVICE"

echo "Serving rolling update complete: image=${IMAGE_TAG} model=${expected_model_version} replicas=${SCALE_TARGET}."
