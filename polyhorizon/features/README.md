# Features Service (Feast)

See [`docs/SYSTEM_SERVICES.md`](../../docs/SYSTEM_SERVICES.md) for this service's
role in the complete platform and its upstream/downstream contracts.

This service manages feature definitions, offline store ingestion, online materialization, data quality, and feature lineage for the training and serving stack.

**Core stack**
Feast, Postgres (offline + registry), Redis (online), SeaweedFS S3 Gateway (Delta source), Great Expectations, Prefect, MLflow integration.

**Primary goals**
- Maintain a reliable offline feature store (Postgres).
- Materialize to an online feature store (Redis).
- Enforce data contracts and quality gates before materialization.
- Track feature schema evolution and materialization lineage.
- Provide metrics and drift signals.

**Architecture overview**
- **Source**: SeaweedFS S3 (Delta Lake) contains the latest engineered OHLCV features.
- **Offline store**: Postgres table in `FEAST_SCHEMA`.
- **Snapshot table**: Rolling snapshot for training.
- **Registry**: Feast registry metadata stored in a separate Postgres schema.
- **Online store**: Redis materialization for low-latency inference.
- **Observability**: Pushgateway metrics for runtime, freshness, and drift.
- **Training integration**: MLflow tags include feature schema + materialization lineage.

**Database and schema design**
Feast uses two Postgres connections:
- **Offline store (feature data)**
  - DB: `POSTGRES_FEAST_DB`
  - User: `POSTGRES_FEAST_USER`
  - Password: `POSTGRES_FEAST_PASSWORD`
  - Schema: `FEAST_SCHEMA`
- **Registry (Feast metadata)**
  - DB: `POSTGRES_FEAST_REGISTRY_DB`
  - User: `POSTGRES_FEAST_REGISTRY_USER`
  - Password: `POSTGRES_FEAST_REGISTRY_PASSWORD`
  - Schema: `FEAST_REGISTRY_SCHEMA`

The registry schema is created by the Postgres init script in `scripts/postgres/01-create-databases-and-users.sh`.

**Recommended defaults**
You can use the default `postgres` DB/user for registry only.
```
POSTGRES_FEAST_REGISTRY_DB=postgres
POSTGRES_FEAST_REGISTRY_USER=postgres
POSTGRES_FEAST_REGISTRY_PASSWORD=postgres
FEAST_REGISTRY_SCHEMA=feast_registry
```

## Configuration

**Primary config**
- `configs/feast_features.yaml` is the single source of truth.
- `feature_repo/feature_store.yaml` configures Feast backend connections.

**Key configuration sections**
- `data`: offline table, snapshot table, partition settings.
- `data_contract`: required columns and types.
- `feast_parameters`: lookback and backfill safety.
- `monitoring`: Pushgateway metrics settings.

**Data contract**
- Enforced at ingestion time.
- Configurable in `data_contract`.
- Example required columns: `symbol`, `event_timestamp`, `window_start`, `open`, `high`, `low`, `close`, `total_volume`.

**Partitioning**
- Optional, enabled by `data.enable_partitioning`.
- Supported granularities: `day`, `month`.
- Applies to newly created offline tables only. Existing non‑partitioned tables are left unchanged.

## Feature flows and scheduling

**Feature store flow**
- Entry point: `polyhorizon/features/flow/feature_store_flow.py`.
- Steps:
  1. Receive an explicit completed dataset manifest from the Spark flow.
  2. Read the pinned feature Delta version and validate before database writes.
  3. Atomically upsert offline rows, rebuild the training snapshot, and record dataset lineage.
  4. Apply the version-filtered Feast source definition (enabled by default).
  5. Materialize the manifest-bounded feature view to Redis; mark publication complete.
  6. Freshness + drift metrics.

**Quality flow**
- Entry point: `polyhorizon/features/flow/feature_store_quality_flow.py`.
- Runs GE validations + drift checks and pushes metrics.

**Prefect schedules** (see `prefect.yaml`)
- `feature-store-after-close` has no schedule; Gold completion triggers it with a versioned manifest.
- `feature-store-quality-nightly` runs at 01:00 ET daily.

## Feast apply policy

- The completion-driven deployment applies definitions before materialization,
  including the source query that filters by dataset version.
- You can still run manually:
```
uv run feast apply
```

## Idempotency guarantees

- Publication uses advisory locks and upserts corrections by `(symbol, window_start)`.
- Retries reuse the exact dataset manifest; a pending publication blocks later datasets.
- See [dataset publication and recovery](../../docs/DATASET_PUBLICATION.md) for
  paths, version semantics, validation, and the PostgreSQL/Redis failure boundary.
- Materialization is incremental in production by default.
- Full materialization requires explicit `allow_backfill=True` and is capped by `max_backfill_days`.

## Observability

**Pushgateway metrics**
- `features_materialization_duration_seconds`
- `features_materialization_success`
- `features_snapshot_staleness_seconds`
- `features_drifted_features`

Enable metrics via:
```
FEATURES_ENABLE_METRICS=true
FEATURES_PUSHGATEWAY_URL=prometheus-pushgateway:9091
FEATURES_METRICS_PREFIX=features
```

**Drift detection**
- Uses KS test between prior and recent snapshot windows.
- Threshold configured via `feast_parameters.drift_threshold`.

## Lineage and auditing

**Schema evolution**
- Stored in `feature_schema_versions` with a hash of the column set.

**Materialization lineage**
- Stored in `feature_materialization_runs` (duration, window, schema version, status).

**Training integration**
- Training and retraining flows attach lineage to MLflow tags.

## Folder map

- `configs/` – Pydantic + YAML configuration
- `feature_repo/` – Feast FeatureViews and data sources
- `feast_ops/` – FeatureManager and Feast interactions
- `src/` – Ingestion, snapshots, validation, utilities
- `tasks/` – Prefect tasks
- `flow/` – Prefect flows
- `data_quality/` – Great Expectations assets and validators
- `utils/` – logger and metrics helpers

## Running locally

**Install dependencies**
```
uv sync --extra features
```

**Start infrastructure**
```
docker compose up -d postgres redis prometheus-pushgateway
```

**Run feature store flow manually**
```
uv run python -m polyhorizon.features.flow.feature_store_flow
```

**Run quality flow manually**
```
uv run python -m polyhorizon.features.flow.feature_store_quality_flow
```

## CI/CD

Workflows: `.github/workflows/ci-cd.yml` and `.github/workflows/deploy-prod.yml`.
- CI tests the Python code and validates the Prefect deployment contract.
- The shared image matrix builds `docker/features/Dockerfile`.
- The completion-driven feature deployment applies Feast definitions during
  publication; the isolated live test below is opt-in, not part of default CI.

## Tests

**Unit tests**
```
uv run pytest tests/unit/features
```

**Integration test (opt‑in)**
```
FEATURES_E2E=1 uv run pytest tests/integration/features/test_feature_store_e2e.py
```

For a separate Docker/OrbStack rehearsal of Gold completion, versioned Delta
publication, PostgreSQL commit, and Feast/Redis retry and readback, follow the
[isolated publication test instructions](../../docs/DATASET_PUBLICATION.md#isolated-live-publication-test-docker--orbstack).
This test does not use the application stack or its data.

## Troubleshooting

**feast apply fails**
- Ensure registry DB/schema exist (created by `scripts/postgres`).
- Verify env vars in `.env` match `feast_features.yaml` and `feature_store.yaml`.

**Materialization returns NULLs**
- Check Redis connectivity and materialization status.
- Inspect `feature_materialization_runs` for failures.

**Data quality failures**
- Review GE expectations in `data_quality/expectations.py`.
- Confirm offline table column names and types match `data_contract`.

**Partitioning not applied**
- Partitioning only applies when creating a new table.
- Existing tables are left unchanged to avoid destructive migrations.
