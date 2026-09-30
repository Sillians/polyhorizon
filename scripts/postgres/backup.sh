#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${POSTGRES_BACKUP_DIR:-/backups}"
RETENTION_DAYS="${POSTGRES_BACKUP_RETENTION_DAYS:-14}"
if [[ -z "$BACKUP_ROOT" || "$BACKUP_ROOT" == "/" ]]; then
  echo "Refusing unsafe POSTGRES_BACKUP_DIR: ${BACKUP_ROOT:-<empty>}" >&2
  exit 1
fi
if [[ ! "$RETENTION_DAYS" =~ ^[0-9]+$ ]]; then
  echo "POSTGRES_BACKUP_RETENTION_DAYS must be a non-negative integer" >&2
  exit 1
fi
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
destination="${BACKUP_ROOT}/${timestamp}"
mkdir -p "$destination"

pg_dumpall --host "${POSTGRES_HOST:-postgres}" --port "${POSTGRES_PORT:-5432}" \
  --username "$POSTGRES_USER" --globals-only --no-role-passwords \
  > "${destination}/globals.sql"

IFS=',' read -r -a databases <<< "$POSTGRES_BACKUP_DATABASES"
for database in "${databases[@]}"; do
  pg_dump --host "${POSTGRES_HOST:-postgres}" --port "${POSTGRES_PORT:-5432}" \
    --username "$POSTGRES_USER" --dbname "$database" --format=custom \
    --compress=9 --file "${destination}/${database}.dump"
  pg_restore --list "${destination}/${database}.dump" >/dev/null
done

(cd "$destination" && sha256sum ./* > SHA256SUMS)
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d \
  -name '[0-9]*T[0-9]*Z' -mtime "+${RETENTION_DAYS}" -exec rm -rf -- {} +
echo "Verified PostgreSQL backup created at ${destination}"
