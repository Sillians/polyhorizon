# Ingestion Pipeline

Subscriptions use the shared [product allowlist](../../docs/PRODUCT_SYMBOLS.md),
not the first rows of the governed universe. All product symbols must be present
and both subscription limits must accommodate the complete list.

See [`docs/SYSTEM_SERVICES.md`](../../docs/SYSTEM_SERVICES.md) for this service's
role in the complete platform and its upstream/downstream contracts.

This service streams real‑time stock trade data from Finnhub WebSocket and publishes it to Kafka. A consumer reads from Kafka and processes trades with optional batch handling and health monitoring.

## What runs
- **Producer**: connects to Finnhub, subscribes to symbols, and sends trades to Kafka.
- **Consumer**: reads trades from Kafka, runs handlers, and logs health metrics.

## Configuration
Config lives in `polyhorizon/ingestion/configs/ingestion_features.yaml` and supports env var expansion via `.env`.

Key sections:
- `finnhub_connection`
- `kafka_connection`
- `bucket_details`
- `load_consumer_performance`
- `load_processing_configuration`
- `monitoring_and_metrics`
- `error_handling`
- `logging`

## Local run (dev)
From repo root:

```bash
python -m polyhorizon.ingestion.main
```

Or run the Prefect flow:

```bash
python -m polyhorizon.ingestion.flow.producer_consumer_streaming_flow
```

## Docker + Prefect (prod)
1. Build image:
```bash
docker build -f docker/ingestion/Dockerfile -t polyhorizon-ingestion:latest .
```

2. Create work pool (once):
```bash
prefect work-pool create --type docker ingestion-docker-pool
```

3. Start worker:
```bash
docker compose up -d prefect-worker-ingestion
```

4. Deploy schedules:
```bash
prefect deploy --all --prefect-file prefect.yaml
```

The Prefect deployment `ingestion-market-hours` runs at **market open** (9:30am ET) and shuts down automatically at market close.

## Health checks (Prometheus)
Health metrics are pushed to Pushgateway at `prometheus-pushgateway:9091` and scraped by Prometheus.

## Live integration rehearsal

The [Kafka and Prefect rehearsal](../../docs/KAFKA_PREFECT_REHEARSAL.md) runs the
real producer against a local Finnhub-format WebSocket fixture and Kafka, then
checks Spark checkpoints, dead-letter routing, Gold completion, and scheduled
Prefect publication through PostgreSQL and Feast/Redis. It uses no Finnhub token
or application data. The test is opt-in and its production-coverage limits are
documented in the guide.
