#!/usr/bin/env bash
set -euo pipefail

NEW_IMAGE_TAG="${1:-}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.prod.yaml}"
ENV_FILE="${ENV_FILE:-.env.prod}"
DEPLOY_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ ! "$NEW_IMAGE_TAG" =~ ^sha-[0-9a-f]{40}$ ]]; then
  echo "Release tag must be sha- followed by a full 40-character commit SHA." >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Production environment file not found: $ENV_FILE" >&2
  exit 1
fi

PREVIOUS_IMAGE_TAG="$(sed -n 's/^IMAGE_TAG=//p' "$ENV_FILE" | tail -n 1)"
if [[ -z "$PREVIOUS_IMAGE_TAG" ]]; then
  echo "IMAGE_TAG is missing from $ENV_FILE" >&2
  exit 1
fi

restore_previous_release() {
  exit_code=$?
  trap - ERR
  echo "Deployment failed; restoring ${PREVIOUS_IMAGE_TAG}." >&2
  sed -i.bak "s/^IMAGE_TAG=.*/IMAGE_TAG=${PREVIOUS_IMAGE_TAG}/" "$ENV_FILE"
  rm -f "${ENV_FILE}.bak"
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" pull \
    spark-master spark-worker-1 spark-worker-2 model-serving frontend \
    || echo "Warning: failed to pull one or more rollback images." >&2
  docker compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d \
    spark-master spark-worker-1 spark-worker-2 frontend \
    || echo "Warning: failed to restart one or more rollback services." >&2
  COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" \
    "$DEPLOY_SCRIPT_DIR/serving_rolling_update.sh" \
    || echo "Warning: failed to roll back serving replicas." >&2
  COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" \
    "$DEPLOY_SCRIPT_DIR/deploy_prefect_flows.sh" \
    || echo "Warning: failed to restore Prefect deployments." >&2
  echo "Rollback to ${PREVIOUS_IMAGE_TAG} completed." >&2
  exit "$exit_code"
}
trap restore_previous_release ERR

sed -i.bak "s/^IMAGE_TAG=.*/IMAGE_TAG=${NEW_IMAGE_TAG}/" "$ENV_FILE"
rm -f "${ENV_FILE}.bak"

COMPOSE_FILE="$COMPOSE_FILE" ENV_FILE="$ENV_FILE" \
  "$DEPLOY_SCRIPT_DIR/prod_deploy.sh"

trap - ERR
printf '%s\n' "$PREVIOUS_IMAGE_TAG" > .previous-production-image-tag
echo "Release ${NEW_IMAGE_TAG} deployed successfully (previous: ${PREVIOUS_IMAGE_TAG})."
