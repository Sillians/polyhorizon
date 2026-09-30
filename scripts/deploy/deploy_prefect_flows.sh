#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yaml}"
ENV_FILE="${ENV_FILE:-.env.prod}"
PREFECT_NETWORK="${PREFECT_NETWORK:-ml-platform}"
WAIT_SECONDS="${PREFECT_WAIT_SECONDS:-120}"
INTERVAL_SECONDS="${PREFECT_INTERVAL_SECONDS:-5}"

while IFS= read -r -d '' entry; do
  export "$entry"
done < <(python3 "$(dirname "$0")/load_env.py" "$ENV_FILE")

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${IMAGE_TAG:?IMAGE_TAG is required}"
: "${DEPLOY_PATH:?DEPLOY_PATH is required so flow jobs can mount the runtime environment}"

PREFECT_JOB_ENV_FILE="${PREFECT_JOB_ENV_FILE:-${DEPLOY_PATH}/.env.prod}"
export PREFECT_JOB_ENV_FILE
if [[ ! -f "$PREFECT_JOB_ENV_FILE" ]]; then
  echo "Prefect job environment file does not exist: ${PREFECT_JOB_ENV_FILE}" >&2
  exit 1
fi

elapsed=0
until docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T prefect-server \
  python -c "import urllib.request; urllib.request.urlopen('http://localhost:4200/api/health', timeout=2)"; do
  sleep "$INTERVAL_SECONDS"
  elapsed=$((elapsed + INTERVAL_SECONDS))
  if [[ "$elapsed" -ge "$WAIT_SECONDS" ]]; then
    echo "Prefect API did not become ready within ${WAIT_SECONDS}s." >&2
    exit 1
  fi
done

declare -A pool_limits=(
  [ingestion-docker-pool]=1
  [streaming-docker-pool]=2
  [features-docker-pool]=1
  [training-docker-pool]=1
)
for pool in "${!pool_limits[@]}"; do
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T prefect-server \
    prefect work-pool create --type docker "$pool" \
    || docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T prefect-server \
      prefect work-pool inspect "$pool" >/dev/null
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T prefect-server \
    prefect work-pool set-concurrency-limit "$pool" "${pool_limits[$pool]}"
done

docker run --rm \
  --network "$PREFECT_NETWORK" \
  --env-file "$ENV_FILE" \
  -e PREFECT_API_URL=http://prefect-server:4200/api \
  -e "GITHUB_REPOSITORY=$GITHUB_REPOSITORY" \
  -e "IMAGE_TAG=$IMAGE_TAG" \
  -e "PREFECT_JOB_ENV_FILE=$PREFECT_JOB_ENV_FILE" \
  "ghcr.io/${GITHUB_REPOSITORY}/orchestration:${IMAGE_TAG}" \
  uv run prefect --no-prompt deploy --all --prefect-file /app/prefect.yaml

echo "Prefect work pools and deployments are synchronized to ${IMAGE_TAG}."
