#!/usr/bin/env bash
set -euo pipefail

: "${BACKUP_PATH:?Set BACKUP_PATH to a timestamped backup directory}"
VERIFY_DB="${POSTGRES_RESTORE_VERIFY_DB:-polyhorizon_restore_verify}"

(cd "$BACKUP_PATH" && sha256sum --check SHA256SUMS)
dropdb --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" --if-exists "$VERIFY_DB"
createdb --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" "$VERIFY_DB"

dump_file="$(find "$BACKUP_PATH" -maxdepth 1 -name '*.dump' -print -quit)"
test -n "$dump_file"
pg_restore --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" \
  --dbname "$VERIFY_DB" --no-owner --no-privileges "$dump_file"
psql --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" \
  --dbname "$VERIFY_DB" --command "SELECT count(*) AS restored_relations FROM pg_class WHERE relkind IN ('r','p');"
dropdb --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" "$VERIFY_DB"
echo "PostgreSQL restore verification passed for ${dump_file}"
