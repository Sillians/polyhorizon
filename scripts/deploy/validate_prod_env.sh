#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env.prod}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Production environment file not found: $ENV_FILE" >&2
  exit 1
fi

while IFS= read -r -d '' entry; do
  export "$entry"
done < <(python3 "$(dirname "$0")/load_env.py" "$ENV_FILE")

required=(
  IMAGE_TAG GITHUB_REPOSITORY
  POSTGRES_SUPERUSER_PASSWORD POSTGRES_APP_PASSWORD
  POSTGRES_MLFLOW_PASSWORD POSTGRES_FEAST_PASSWORD POSTGRES_PREFECT_PASSWORD
  POSTGRES_FEAST_REGISTRY_PASSWORD POSTGRES_MONITOR_PASSWORD
  AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
  POLYHORIZON_DOMAIN TRAEFIK_ACME_EMAIL TRAEFIK_DASHBOARD_DOMAIN
  MLFLOW_DOMAIN PREFECT_DOMAIN GRAFANA_DOMAIN PROMETHEUS_DOMAIN ALERTMANAGER_DOMAIN
  ALERTMANAGER_SLACK_WEBHOOK_FILE
  SERVING_RELOAD_API_KEY SERVING_ACTIVATION_MODE
  SERVING_API_KEYS SERVING_OPERATOR_API_KEYS FINNHUB_TOKEN
  GRAFANA_ADMIN_PASSWORD TRAEFIK_DASHBOARD_USERS
  DEPLOY_PATH PREFECT_JOB_ENV_FILE
  UNIVERSE_CURRENT_KEY UNIVERSE_MAX_AGE_HOURS
)

for name in "${required[@]}"; do
  value="${!name:-}"
  if [[ -z "$value" || "$value" == *REPLACE_ME* || "$value" == *REPLACE_WITH* || "$value" == "<>" ]]; then
    echo "Missing production value: $name" >&2
    exit 1
  fi
done

if [[ "$PREFECT_JOB_ENV_FILE" != /* || "$DEPLOY_PATH" != /* || ! -f "$PREFECT_JOB_ENV_FILE" ]]; then
  echo "DEPLOY_PATH and PREFECT_JOB_ENV_FILE must be absolute, and the job env file must exist." >&2
  exit 1
fi
if [[ "${KAFKA_ENABLE_AUTO_COMMIT:-}" != "false" ]]; then
  echo "Production consumer requires KAFKA_ENABLE_AUTO_COMMIT=false." >&2
  exit 1
fi
if [[ "$MLFLOW_BACKEND_STORE_URI" == *REPLACE_ME* ]]; then
  echo "MLFLOW_BACKEND_STORE_URI still contains a placeholder." >&2
  exit 1
fi

if [[ ! -f "$ALERTMANAGER_SLACK_WEBHOOK_FILE" ]]; then
  echo "Alertmanager Slack webhook file not found: $ALERTMANAGER_SLACK_WEBHOOK_FILE" >&2
  exit 1
fi
if grep -q "REPLACE_ME" "$ALERTMANAGER_SLACK_WEBHOOK_FILE"; then
  echo "Alertmanager Slack webhook file still contains a placeholder." >&2
  exit 1
fi

if [[ ! "$IMAGE_TAG" =~ ^sha-[0-9a-f]{40}$ ]]; then
  echo "IMAGE_TAG must be sha- followed by a full 40-character commit SHA, got: $IMAGE_TAG" >&2
  exit 1
fi

if [[ "${SERVING_CORS_ORIGINS:-}" == "*" || "${SERVING_REQUIRE_API_KEY:-false}" != "true" ]]; then
  echo "Production serving requires explicit CORS origins and API-key authentication." >&2
  exit 1
fi

if [[ "${SERVING_MODEL_RELOAD_ENABLED:-false}" != "true" || "${SERVING_ACTIVATION_MODE}" != "reload" ]]; then
  echo "Production requires authenticated champion reload activation." >&2
  exit 1
fi

case ",${SERVING_OPERATOR_API_KEYS:-}," in
  *",${SERVING_RELOAD_API_KEY},"*) ;;
  *)
    echo "SERVING_RELOAD_API_KEY must be present in SERVING_OPERATOR_API_KEYS." >&2
    exit 1
    ;;
esac

case ",${SERVING_API_KEYS:-}," in
  *",${SERVING_RELOAD_API_KEY},"*)
    echo "The operator reload credential must not also be a consumer API key." >&2
    exit 1
    ;;
esac

if [[ ! "${SERVING_REPLICAS:-}" =~ ^[1-9][0-9]*$ ]] \
  || [[ ! "${SERVING_ROLLING_REPLICAS:-}" =~ ^[1-9][0-9]*$ ]] \
  || (( SERVING_ROLLING_REPLICAS <= SERVING_REPLICAS )); then
  echo "SERVING_ROLLING_REPLICAS must be an integer greater than SERVING_REPLICAS." >&2
  exit 1
fi

if [[ ! "$UNIVERSE_MAX_AGE_HOURS" =~ ^[1-9][0-9]*$ ]]; then
  echo "UNIVERSE_MAX_AGE_HOURS must be a positive integer." >&2
  exit 1
fi

echo "Production environment validation passed: $ENV_FILE"
