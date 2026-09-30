# Local backend and synthetic model bootstrap

This workflow is for integration testing when no historical OHLCV entitlement
is available. The configured Finnhub key can access live quotes but returned
HTTP 403 for 30-minute historical candles. The bootstrap therefore creates a
deterministic, clearly labeled synthetic release for `NVDA`, `AAPL`, and `MSFT`.

It is **not a production model**. The MLflow run and version carry
`data_provenance=synthetic-development`, `production_eligible=false`, and
`governance_qualification=not-evaluated-synthetic`. The script assigns the local
champion alias explicitly only to make the serving integration executable; the
normal governance bootstrap will not qualify this model.

```bash
docker compose up -d postgres redis master volume filer seaweedfs-s3 \
  seaweedfs-bucket-init mlflow
PYTHONPATH=. .venv/bin/python scripts/dev/bootstrap_synthetic_model.py \
  --allow-synthetic --epochs 2
docker compose build model-serving
docker compose up -d model-serving
```

The script refuses non-local PostgreSQL and MLflow targets. It publishes one
versioned dataset into the local Feast PostgreSQL schema, trains a compact TFT,
stores fitted dataset parameters and the target/conversion contract in the
artifact, registers it in MLflow, and assigns local champion/challenger aliases.
It uses `FEAST_SCHEMA`, `OFFLINE_TABLE_NAME`, and `SNAPSHOT_TABLE_NAME` from the
environment; keep those values identical across publication, training, Feast,
and serving. Serving selects only the latest `dataset_publications` release, so
it cannot mix rows from multiple dataset versions in one inference window.
Serving must use `SERVING_TFT_MAX_ENCODER_LENGTH=64` for this development model.
The serving dependency set includes `boto3`, which MLflow needs to load model
artifacts from the configured S3-compatible store.

If OrbStack's published PostgreSQL port conflicts with another local instance,
set `LOCAL_POSTGRES_HOST` to the project container's private address and
`LOCAL_POSTGRES_PORT=5432`. The script accepts only loopback/private IP addresses.

API keys are never printed. Verify using the values in ignored `.env`:

```bash
curl http://127.0.0.1:8000/v1/health/ready
curl -H "X-API-Key: <forecast key>" http://127.0.0.1:8000/v1/model
```

Replace this model with a governed model trained on licensed, validated market
history before interpreting predictions or deploying beyond local development.
