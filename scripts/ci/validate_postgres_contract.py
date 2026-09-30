"""Validate the PostgreSQL production and ML data contracts."""

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.prod.yaml").read_text())
    services = compose["services"]
    postgres = services["postgres"]
    environment = postgres["environment"]

    required = {
        "POSTGRES_INITDB_ARGS",
        "FEAST_REGISTRY_DB",
        "FEAST_REGISTRY_USER",
        "FEAST_REGISTRY_PASSWORD",
        "POSTGRES_MONITOR_USER",
        "POSTGRES_MONITOR_PASSWORD",
    }
    missing = required - set(environment)
    if missing:
        raise SystemExit(f"PostgreSQL environment contract missing: {sorted(missing)}")

    if "--data-checksums" not in environment["POSTGRES_INITDB_ARGS"]:
        raise SystemExit("New PostgreSQL clusters must enable data checksums")
    if "postgres-exporter" not in services:
        raise SystemExit("PostgreSQL metrics exporter is missing")
    if "postgres-backup" not in services:
        raise SystemExit("PostgreSQL backup job is missing")

    feast = (ROOT / "polyhorizon/features/feature_repo/feature_store.yaml").read_text()
    if "POSTGRES_SUPERUSER_PASSWORD" in feast:
        raise SystemExit("Feast must never use the PostgreSQL superuser")
    for variable in (
        "POSTGRES_FEAST_REGISTRY_DB",
        "POSTGRES_FEAST_REGISTRY_USER",
        "POSTGRES_FEAST_REGISTRY_PASSWORD",
    ):
        if variable not in feast:
            raise SystemExit(f"Feast registry connection is missing {variable}")

    init = (ROOT / "scripts/postgres/01-create-databases-and-users.sh").read_text()
    if "GRANT pg_monitor" not in init or "REVOKE CONNECT" not in init:
        raise SystemExit("PostgreSQL least-privilege grants are incomplete")

    ingestion = (ROOT / "polyhorizon/features/src/parquet_to_postgres.py").read_text()
    if "hashtext(%s)" not in ingestion or "USING BRIN" not in ingestion:
        raise SystemExit("Offline-store locking or time-range indexing is missing")


if __name__ == "__main__":
    main()
