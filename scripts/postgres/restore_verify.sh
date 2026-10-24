#!/usr/bin/env bash
set -euo pipefail

: "${BACKUP_PATH:?Set BACKUP_PATH to a timestamped backup directory}"
VERIFY_DB="polyhorizon_verify_$(date +%s)_${RANDOM}"

(cd "$BACKUP_PATH" && sha256sum --check SHA256SUMS)
createdb --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" "$VERIFY_DB"
cleanup() {
  dropdb --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" "$VERIFY_DB"
}
trap cleanup EXIT

dump_file="$(find "$BACKUP_PATH" -maxdepth 1 -name '*.dump' -print -quit)"
test -n "$dump_file"
pg_restore --exit-on-error --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" \
  --dbname "$VERIFY_DB" --no-owner --no-privileges "$dump_file"
psql --host "$POSTGRES_HOST" --port "$POSTGRES_PORT" --username "$POSTGRES_USER" \
  --dbname "$VERIFY_DB" --command "SELECT count(*) AS restored_relations FROM pg_class WHERE relkind IN ('r','p');"
echo "PostgreSQL restore verified for ${dump_file}; other database dumps and object-store/model artifacts require separate verification"
