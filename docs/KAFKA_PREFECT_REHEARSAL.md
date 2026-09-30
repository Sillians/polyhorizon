# Live Kafka and Prefect rehearsal

This opt-in test uses a real Kafka broker, Prefect API/scheduler, process worker,
Spark/Delta, SeaweedFS, PostgreSQL, and Feast/Redis. It does not contact Finnhub,
read application credentials, or register deployments on the application's
Prefect server.

## Run locally (Docker or OrbStack)

Install the locked development, ingestion, streaming, and features dependencies,
and make Java 17 available. From the repository root:

```bash
docker compose -f tests/integration/features/compose.yaml -f tests/integration/orchestration/compose.yaml up -d --wait
ORCHESTRATION_E2E=1 .venv/bin/python -m pytest -q -s tests/integration/orchestration/test_live_orchestration.py --tb=short
docker compose -f tests/integration/features/compose.yaml -f tests/integration/orchestration/compose.yaml down --volumes
```

Use these exact Compose files for cleanup, not the application Compose files.
The project is `polyhorizon-orchestration-test`. It uses loopback ports 19092
(Kafka), 15432 (PostgreSQL), 16379 (Redis), and 18333 (S3). Do not run it at the
same time as the standalone publication test, which uses the storage ports.
Spark may download Maven dependencies on first run.

Prefect starts locally from the installed, locked Python environment on a free
loopback port, with a temporary SQLite database and separate `PREFECT_HOME`.
The process worker and its child jobs use an explicit environment with dotenv
loading disabled. The test prints the temporary directory containing server and
worker logs. All credentials in the fixtures are public test-only credentials.

## Assertions

1. Prefect's scheduler creates a run from a one-occurrence UTC schedule; the test
   does not manually create that flow run. The worker executes it at/after its
   scheduled time, and the run is recorded as automatically scheduled.
2. A local WebSocket fixture verifies subscriptions for the shared product
   allowlist and sends Finnhub-format trades. The real producer resolves a
   checksummed universe artifact in S3, normalizes the messages, and sends them
   to a unique Kafka topic with three partitions.
3. The production Spark Kafka reader, quality filter, and Delta sinks accept
   nine trades and reject one negative-price trade into the dead-letter table.
   Accepted trade timestamps retain their original millisecond precision.
   Restarting the Bronze query from its S3 checkpoint adds no duplicate rows.
4. Gold completion and feature computation create a version-pinned six-row
   release. The production `publish_completed_dataset` task dispatches the
   manifest to an unscheduled `feature-store-flow/feature-store-after-close`
   deployment and waits for its terminal state.
5. A test-only configuration adapter runs the real publisher and `feast apply`
   against a disposable schema/repository. PostgreSQL's ledger reaches
   `complete`, and all three symbols return the expected close and volume from
   Redis. The child run carries the same dataset ID and is not auto-scheduled.

The test has bounded startup/execution timeouts and terminates its Prefect
process groups on success or failure. Normal completion/failure cleans the
Kafka topic, S3 bucket, and PostgreSQL schema; forced termination may interrupt
that cleanup. The Compose teardown removes remaining disposable
data, including Redis keys. Temporary logs remain under pytest's temporary
directory for diagnosis.

## Boundaries

This is not a production deployment. It uses a single broker (replication factor
one), a local process worker rather than Docker work pools, synthetic trades,
and a one-shot schedule rather than waiting for the next market-open cron.
The test explicitly finalizes the synthetic session instead of waiting for
the market calendar to close. It therefore does not certify live Finnhub
credentials/rate limits, multi-broker failover, production image/network/env-file
mounts, or the full-session shutdown timer. The feature deployment's test adapter
omits optional drift/metrics reporting but executes the real data publisher.

Production retains the market-open schedules in `prefect.yaml`; feature
publication remains completion-triggered with no independent clock schedule.
See [dataset publication](DATASET_PUBLICATION.md) for the release contract and
pending-publication recovery procedure.

## Verification record

Verified on OrbStack on 2026-09-21: the scheduled end-to-end test passed in
142 seconds, including the real downstream deployment and `feast apply`.
It produced 9 Bronze rows, 1 dead-letter row, and 6 published feature rows;
checkpoint restart produced no duplicates. All 26 feature/scheduling unit tests,
6 Spark regression tests, repository Ruff checks, and the Prefect contract
validator passed.

The rehearsal exposed and fixed a production Kafka reader bug: converting epoch
milliseconds through `from_unixtime` discarded the fractional second. The reader
now uses `timestamp_millis`; regression tests check exact epoch round trips in
UTC and America/New_York. Previously truncated Bronze data is not automatically
repaired. No production schedules were activated during verification.
