# Gold completion and versioned feature publication

The production dependency chain is:

`market close + grace → drain Bronze → finalize Gold → compute features → validate → PostgreSQL → Feast`

The streaming Prefect flow owns the Spark driver through market close, feature
computation, and the downstream feature deployment. The recovery streaming flow
uses the same handoff. `feature-store-after-close` has **no clock schedule**;
it is dispatched only after Spark returns a completed dataset manifest. Running
the feature flow without `dataset_release` fails closed. Republish Prefect
deployments to remove the old 16:10 ET schedule.

## Completion semantics

At the calendar's close plus configured shutdown grace, the driver drains the
Bronze query, stops streaming queries, and pins the committed Bronze Delta
version. It batch-aggregates that snapshot into an independent Gold table.
This flushes final windows that streaming watermarks cannot finalize without
later events. All product symbols must have a bar ending at market close;
missing closing bars fail the release. Early closes use the calendar close.

Completion means all accepted trades in the drained Bronze snapshot, not a
guarantee that the upstream provider delivered every trade. Rejected/late events
remain in the dead-letter path. A process interruption, stream failure, missing
closing bar, or validation failure never dispatches feature publication.

## Paths and identities

| Artifact | Path/version |
|---|---|
| Live Bronze | `BRONZE_PATH`, explicit Delta version recorded in manifest |
| Live Gold | `GOLD_PATH`; `source_table` must resolve to the same path |
| Completed Gold | `${GOLD_PATH}-completed/<dataset UUID>`, Delta version 0 |
| Computed features | `${FEATURE_OUTPUT_PATH}/datasets/<dataset UUID>`, Delta version 0 |
| Durable completion manifest | `${FEATURE_OUTPUT_PATH}/manifests/<dataset UUID>/part-*.txt` |

Gold and feature output tables are written with `errorifexists`; retries cannot
overwrite them. The JSON manifest records the UUID, all three input/output paths
and Delta versions, feature definition version, frequency, rolling lookback,
image tag, row count, market close, completion time, and event-time bounds.
The manifest is persisted only after the feature Delta commit succeeds.

`FEATURE_OUTPUT_PATH` is the shared feature root. The feature config's legacy
`feature_parquet_path` aliases this root; the production publisher never reads
an unversioned `stock_ohlcv.parquet` path or implicitly reads latest. `s3a://`
and `s3://` are normalized for delta-rs reads and path comparisons.

## Validation and PostgreSQL

Feature computation buckets existing Gold bars by `window_start`, preserving
their original interval. `event_timestamp` is always `window_end`, including in
the training snapshot. Initial per-symbol rows with undefined sample volatility
are excluded as warmup; no backward-fill from future observations is performed.

Before any PostgreSQL connection is opened, the publisher reads the exact
feature Delta version and checks required columns/types, manifest counts/time
bounds, complete product membership, unique keys, finite positive prices,
OHLC bounds, integer nonnegative volume, and nonnegative volatility.
Publication uses the shared product allowlist directly; it does not depend on
fetching a separate S&P symbol-universe object to initialize Feast.

A session advisory lock serializes publication against the legacy offline
loader. One PostgreSQL transaction stages validated rows, upserts corrections
by `(symbol, window_start)`, rebuilds the configured rolling training snapshot,
records schema lineage, and stores the manifest with `postgres_complete` status
in `FEAST_SCHEMA.dataset_publications`. Both data tables carry `dataset_version`.
The `latest_training_snapshot` pointer records that same dataset version in the
same transaction.
Snapshot replacement uses transactional DELETE/INSERT: readers see the prior
or new committed snapshot, never a partially rebuilt snapshot.

Feast's PostgreSQL source selects rows belonging to the newest publication ID,
so rows retained from older releases cannot leak into the published feature
view. `feast apply` is enabled for the completion-driven deployment to install
this source query. Existing deployments must apply the updated definitions.
Full bounded materialization of the configured feature view replays corrections
and uses the manifest's maximum event time, not wall-clock `now()`. The end
bound includes the closing bar; lookback/backfill limits still apply.

## Failure and recovery

PostgreSQL and Redis are separate systems; this is not a distributed transaction.
After PostgreSQL commits, Feast may fail or partially update Redis. The ledger
remains `postgres_complete`, and another dataset is blocked until that dataset
is successfully retried. Successful materialization marks the record `complete`.
The same completed ID is a no-op; reuse of an ID with a different manifest fails.
Older event-time releases cannot replace newer publications.

Inspect publication state (substitute the configured schema):

```sql
SELECT dataset_id, status, max_event_time, updated_at
FROM feast_schema.dataset_publications ORDER BY updated_at DESC;
```

To retry, copy the exact JSON object from the pending row's `manifest` or its
durable object-store manifest and run `feature-store-flow/feature-store-after-close`
with that object as `dataset_release`. Do not rerun the streaming flow to recover
an online-store failure: that creates a different dataset ID. Fix the underlying
Feast/Redis problem first. Do not mark the ledger complete by hand.

PostgreSQL data becomes visible before Redis materialization completes; consumers
requiring a cross-store atomic cutover need an additional serving activation
barrier. Manual legacy loaders/materializers must not run during publication or
while a record is pending. They do not carry this version contract.

## Deployment and verification

Rebuild the Spark and features images from the same release, deploy the updated
Prefect definitions, and preserve manifests and Delta files for replay (avoid
vacuuming referenced versions). New feature dependencies are included in the
lockfile; Spark executors and driver use the image's locked Python environment.

Automated checks exercise pre-load validation, explicit version reads,
PostgreSQL/Feast retry behavior, successful-completion-only dispatch, and Gold
window alignment. A full integration rehearsal additionally needs running
Kafka, Spark/Delta, SeaweedFS, PostgreSQL, Prefect, and Redis. Batch Gold
recomputation currently covers retained Bronze history, and feature validation
collects the resulting product dataset on the driver; size the driver for that
history or add bounded history processing before expanding the product universe.

### Isolated live publication test (Docker / OrbStack)

With the locked streaming, features, and test dependencies installed and Java 17
available, run from the repository root:

```bash
docker compose -f tests/integration/features/compose.yaml up -d --wait
PUBLICATION_E2E=1 .venv/bin/python -m pytest -q tests/integration/features/test_publication_pipeline.py
docker compose -f tests/integration/features/compose.yaml down --volumes
```

The separate `polyhorizon-publication-test` project exposes test-only PostgreSQL,
Redis, and SeaweedFS on loopback ports 15432, 16379, and 18333. Its credentials
are public test fixtures, never production credentials. Do not reuse them for
application services. Spark runs locally and may download Maven dependencies.
SeaweedFS is pinned by digest for repeatability. The test first checks conditional
S3 copies (required for Delta metadata renames); older incompatible images can
otherwise spend minutes retrying each commit.

The test writes synthetic Bronze trades, finalizes immutable Gold and feature
Delta datasets, validates and publishes them into real PostgreSQL, injects an
online-publication failure, retries the same manifest through real Feast/Redis,
and verifies online values and completed-replay idempotency. Each run uses a
unique bucket, database schema, and Feast project. Teardown removes the ephemeral
test services and their data; application volumes are not touched. This does not
exercise Kafka ingestion, market-calendar scheduling, or Prefect dispatch.

Verified on OrbStack on 2026-09-21: the live publication/retry test passed,
alongside 22 feature unit tests, repository Ruff checks, and the Prefect contract
validator. This verifies the isolated data-publication path, not a production
deployment or a full Kafka-to-Prefect scheduling rehearsal.

The separate [Kafka/Prefect rehearsal](KAFKA_PREFECT_REHEARSAL.md) extends this
coverage with real scheduled execution, Kafka ingestion, Spark checkpoint
restart, and the downstream Prefect deployment handoff.
