# Recovery and operational qualification

## Full-session release gate (2026-09-29)

Verification: 67 targeted unit tests passed across the session, publication,
orchestration, lineage, activation, bootstrap and local-operations suites. The
isolated full-session Spark/Delta → PostgreSQL → Feast test also passed, including
pending-publication recovery and online parity. These were synthetic tests, not
a live-session qualification. The isolated test services were removed afterward.
Local Spark qualification and features images were rebuilt and registered with
all schedules paused. No production publication, training or promotion ran.

Streaming and recovery flows default to `qualification_only=True`. They finish
the pinned Delta release but do not dispatch feature publication. Leave this
default in place for the first live session. No schedules have been enabled by
this code change.

The gate requires the exact NYSE calendar close (including early closes), all
13 half-hour bars on a normal day (7 on an early close) for every product symbol,
no duplicate/off-grid bars, valid positive OHLCV, and no trade gap above 120
seconds per symbol, including the opening and closing boundaries. Trades are
read from the pinned Bronze version. The local audit is bounded at two million
session trades and fails closed above that limit. Coverage is not proof that
every provider trade was delivered; reconcile provider/broker telemetry too.

Publication independently recomputes the evidence from pinned Gold/Bronze before
connecting to PostgreSQL. Old releases without this evidence cannot publish.
Training reads only completed publications with qualification evidence; model
approval and activation require qualified training lineage. Existing minimum
training history, evaluation sample size, baseline and calibration gates still
apply: one session is not enough by itself to train a production TFT.

### Live-session procedure

1. Rebuild Spark, features and training images before using these changes; do not
   assume previously running image tags contain the new gates. Keep feature,
   training and promotion schedules paused.
   For this local code-only Spark update, build with
   `docker build -f docker/spark/Dockerfile.session -t spark-custom:session-qualified-local .`.
   This requires the previously dependency-tested `spark-custom:3.5.7` base;
   changes to dependency versions require rebuilding that base first.
   Re-register local jobs with `bash scripts/deploy/deploy_local.sh`; this keeps
   schedules paused and selects the qualification driver image. Training and
   serving images must also be rebuilt before any later activation.
2. Start object storage and Spark master/workers, verify Kafka health and governed
   universe freshness. Use only one driver and producer for the session. Preserve
   existing Kafka offsets and Spark checkpoints.
3. Start the streaming qualification deployment before the open; start ingestion
   close enough to the open to stay within its seven-hour timeout. Confirm Bronze
   query readiness before the first trade. Do not start hours early or mid-session
   and claim a complete session.
4. During the session, verify broker acknowledgement counts, per-symbol liveness,
   Bronze/checkpoint progress, dead letters and producer/driver failures. Record
   Prefect run IDs and operational incidents. A coverage pass is not permission
   to ignore a failed producer or unexplained broker/Delta discrepancy.
5. After the close/grace-period drain, export the returned `dataset_release`
   object as JSON. Recheck it without publishing:

   ```bash
   PYTHONPATH=. .venv/bin/python scripts/ops/qualify_session.py /absolute/path/release.json
   ```

6. Review the pinned report and operational evidence before explicitly dispatching
   that same release to `feature-store-flow/feature-store-after-close`. This gate
   does not automatically start training, change an alias or arm future sessions.

An interrupted/partial session must fail and be repeated on another trading day;
do not fill missing bars with synthetic trades or relax the checks to pass it.

## Recorded verification (2026-09-26)

- Recovered 12,351 retained Kafka records into isolated local Delta, read back all
  rows, and rejected zero records. Millisecond event timestamps and Kafka
  topic/partition/offset provenance are retained. Source consumer offsets and
  application Spark checkpoints were not modified.
- Captured immutable upper bounds: partition 0 = 9538, 1 = 0, 2 = 2813. Recovery
  refuses changing retention bounds, missing records and timeout; each run uses
  a new UUID directory and emits a success report only after validation.
- Found 3,482 identical symbol/time/price/volume payloads. They are flagged, not
  dropped: distinct legitimate trades can share these fields. There is no
  provider trade ID proving duplicate identity.
- Coverage: NVDA has 3 of 13 session bars, AAPL/MSFT 2 of 13. The first bar is
  only partially observed. This is NOT a production-eligible feature release.
- Actual producer fault test: real local WebSocket and Kafka, acknowledged three
  symbol records, forced disconnect, resubscription, provider error and explicit
  retry-exhaustion failure. The disposable test topic is deleted afterward.
- Actual feature test: Spark/Delta → PostgreSQL → Feast/Redis, deliberate
  materialization failure, durable pending publication, retry, same-dataset
  idempotence, and online price/volume/dataset-version parity. Synthetic fixtures
  run in isolated test services, not the production feature store.
- Docker workers for local ingestion, streaming and features are registered.
  A manual Saturday ingestion run completed via Docker and skipped the closed
  market. This proves startup/configuration, NOT full-session trading reliability.
- The publication watchdog completed in Docker and reported the missing release
  as overdue. Promtool validated both changed alert files (nine rules total).

Recovery report: `artifacts/recovery/8044e09b-9a4a-476e-a5e6-2308081ae011/report.json`.
Artifacts are excluded from Git. Retain/back up this folder if recovery must
survive workstation cleanup; `/tmp/polyhorizon-recovery` is only a scratch copy.

## Reproduce safely

```bash
PYTHONPATH=. .venv/bin/python scripts/ops/recover_kafka.py --output artifacts/recovery
bash scripts/deploy/deploy_local.sh
docker compose -f tests/integration/features/compose.yaml up -d --wait
PUBLICATION_E2E=1 PYTHONPATH=. .venv/bin/python -m pytest -q tests/integration/features/test_publication_pipeline.py
INGESTION_FAULT_E2E=1 PYTHONPATH=. .venv/bin/python -m pytest -q tests/integration/ingestion/test_fault_recovery.py
```

Local deployments use the actual `polyhorizon_ml-platform` Docker network,
host `.env` mounted read-only, internal Kafka/S3/Postgres/Redis/Prefect addresses,
and local images with pull policy Never. All scheduled deployments are paused
and schedule entries inactive by default. The feature flow has no cron; the
completed Spark manifest triggers it. `register_local.py --activate` deliberately
enables schedules; do not use that option until the checklist below is satisfied.
The Docker socket is mounted only in local workers and grants host-level Docker
control; this convenience profile is not a hardened production deployment.
The ingestion pool allows two jobs so deadline checks can run alongside the
market-session producer; each deployment still permits only one concurrent run.

The separate local registration script does not deploy training or promote a
model. It resets these managed local deployment definitions on each invocation.
Use release-pinned registry images and the existing production deployment path
on remote hosts; local tags are not portable registry releases.

## Publication and ingestion monitoring

`publication-deadline-hourly` reads only the publication ledger and exports
whether the NYSE closing dataset was completed by close + 60 minutes. It handles
holidays/early closes and never treats a `postgres_complete` pending row as a
completed handoff. Missing ledger/release is overdue; DB/metrics failures fail
the flow. The watchdog-unavailable alert covers missing/stale check results.
The per-symbol ingestion alert covers stale symbols while producer telemetry
is still reporting. Alert rules must be loaded by a running Prometheus and routed
through configured Alertmanager before notifications can be claimed operational.

## Still required before production activation

1. Verify all job images, object-store volumes, Spark master/workers, Redis and
   metrics endpoints are running; local registration does not start the full stack.
2. Refresh the governed universe and run through one complete NYSE session.
   Confirm all three symbols, broker acknowledgements, reconnect recovery,
   advancing Spark checkpoints and nonzero Bronze rows throughout the session.
3. Require the closing bar and adequate encoder/history coverage for each symbol;
   verify a completed release, ledger status `complete`, and serving freshness.
4. Exercise the configured notification route and confirm a missing-release
   alert reaches an operator. Do not suppress the current overdue-data condition.
5. Keep recovered morning data isolated until an explicit replay/deduplication
   policy is approved. Do not delete live checkpoints or bypass publication gates.

No full-session reliability claim, production materialization, model training,
or champion promotion follows from these integration tests.
