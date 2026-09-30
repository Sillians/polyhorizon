#!/usr/bin/env bash
set -euo pipefail

echo "Reconciling PostgreSQL databases, roles, schemas, and grants..."

reconcile_database() {
  local database=$1 role=$2 password=$3 schema=$4

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    --set=role_name="$role" --set=role_password="$password" \
    --set=db_name="$database" <<'EOSQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name') \gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password') \gexec
SELECT format('CREATE DATABASE %I OWNER %I', :'db_name', :'role_name')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name') \gexec
SELECT format('ALTER DATABASE %I OWNER TO %I', :'db_name', :'role_name') \gexec
SELECT format('REVOKE CONNECT ON DATABASE %I FROM PUBLIC', :'db_name') \gexec
SELECT format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO %I', :'db_name', :'role_name') \gexec
EOSQL

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$database" \
    --set=role_name="$role" --set=schema_name="$schema" <<'EOSQL'
SELECT format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I', :'schema_name', :'role_name') \gexec
SELECT format('ALTER SCHEMA %I OWNER TO %I', :'schema_name', :'role_name') \gexec
SELECT format('ALTER ROLE %I IN DATABASE %I SET search_path = %I, public',
              :'role_name', current_database(), :'schema_name') \gexec
SELECT format('GRANT USAGE, CREATE ON SCHEMA %I TO %I', :'schema_name', :'role_name') \gexec
SELECT format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO %I', :'schema_name', :'role_name') \gexec
SELECT format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I TO %I', :'schema_name', :'role_name') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT ALL ON TABLES TO %I',
              :'role_name', :'schema_name', :'role_name') \gexec
SELECT format('ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT ALL ON SEQUENCES TO %I',
              :'role_name', :'schema_name', :'role_name') \gexec
EOSQL
}

reconcile_database "$APP_DB" "$APP_USER" "$APP_PASSWORD" "app_schema"
reconcile_database "$MLFLOW_DB" "$MLFLOW_USER" "$MLFLOW_PASSWORD" "mlflow_schema"
reconcile_database "$FEAST_DB" "$FEAST_USER" "$FEAST_PASSWORD" "$FEAST_SCHEMA"
reconcile_database "$FEAST_REGISTRY_DB" "$FEAST_REGISTRY_USER" "$FEAST_REGISTRY_PASSWORD" "$FEAST_REGISTRY_SCHEMA"
reconcile_database "$PREFECT_DB" "$PREFECT_USER" "$PREFECT_PASSWORD" "prefect_schema"

# The exporter gets PostgreSQL's built-in read-only monitoring role and no
# ownership privileges over application data.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=role_name="$POSTGRES_MONITOR_USER" --set=role_password="$POSTGRES_MONITOR_PASSWORD" <<'EOSQL'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role_name') \gexec
SELECT format('ALTER ROLE %I LOGIN PASSWORD %L', :'role_name', :'role_password') \gexec
SELECT format('GRANT pg_monitor TO %I', :'role_name') \gexec
EOSQL

for database in "$APP_DB" "$MLFLOW_DB" "$FEAST_DB" "$FEAST_REGISTRY_DB" "$PREFECT_DB"; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    --set=db_name="$database" --set=role_name="$POSTGRES_MONITOR_USER" <<'EOSQL'
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', :'db_name', :'role_name') \gexec
EOSQL
done

echo "PostgreSQL reconciliation completed."
