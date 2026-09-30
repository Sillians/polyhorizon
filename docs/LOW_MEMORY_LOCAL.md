# Low-memory local collection

This is an opt-in local Compose overlay, not a change to production defaults.
Requires Compose 2.24.4+ for `!override`. Do not use plain `compose up` for the
reduced stack: it would reintroduce optional services and broker 3.

```bash
docker compose -f docker-compose.yaml -f docker-compose.local-workers.yaml \
  -f docker-compose.low-memory.yaml up -d --no-build --scale spark-worker=1 \
  postgres zookeeper kafka-broker-1 kafka-broker-2 master volume filer \
  seaweedfs-s3 prometheus-pushgateway spark-master spark-worker prefect-server
```

The supervisor uses this overlay and explicitly scales the Spark worker to one.
It does not enable itself: installing/re-enabling the LaunchAgent remains a
separate action. Collection-only Prefect registration uses `--low-memory`;
without `--activate` schedules stay paused. No training/materialization is enabled.

## Resource envelope

| Component | Container memory cap | Main process budget |
|---|---:|---|
| Kafka, each of two brokers | 768 MiB | 256–384 MiB JVM heap |
| ZooKeeper | 256 MiB | 64–128 MiB JVM heap |
| Spark master | 384 MiB | 128 MiB daemon heap |
| Single Spark worker | 1536 MiB | 128 MiB daemon + 768 MiB executor heap, one core |
| Spark/Prefect driver job | 1536 MiB | 768 MiB JVM heap, 128 MiB result limit |
| Prefect server | 512 MiB | Python service |
| Each of two Prefect workers | 256 MiB | Python worker |
| Ingestion/universe job | 512 MiB each | Python process |
| PostgreSQL | 384 MiB | 96 MiB shared buffers, 60 connections |
| Seaweed master/volume/filer/S3 | 192/256/384/192 MiB | Filer Go memory target 256 MiB |
| Pushgateway | 64 MiB | Metrics endpoint required by flows |

Caps are ceilings, not measured steady-state usage. The active job processes,
container engine, filesystem caches and host apps also need RAM. Start by measuring
this profile on the 16 GiB Mac; do not force the engine down to 8 GiB before a full
session (including post-close processing) succeeds without OOM or accumulating lag.
The current engine is OrbStack; its global memory setting has not been modified.
Optional dashboards, Redis, serving, feature worker and training are excluded.

## Existing Kafka data

Never remove the third broker before migrating existing topic assignments.
`scripts.ops.migrate_kafka_two_brokers` inspects all topics, including internal
consumer offsets, refuses out-of-sync replicas, saves the prior assignments,
and verifies replicas/ISR and end offsets. It defaults to plan-only; `--execute`
is the explicit mutation. Run offline with all three brokers temporarily available.
The supervisor requires the resulting `artifacts/low-memory/kafka-verified.json`.

Replication factor is 2 with `min.insync.replicas=2`; producer acknowledgements
remain `all`. Both brokers must be healthy for writes. Partition counts, topic
names and consumer groups are unchanged. Broker 3's volume is retained. Restoring
three-way replication later requires deliberate reassignment using the saved plan,
not merely starting the old broker. A profile change does not rewrite replicas.

Host-side clients should use `localhost:9092,localhost:9093`. Prefect and Compose
jobs use internal `kafka-broker-1:29092,kafka-broker-2:29093`; no private `.env`
secrets or production three-broker examples are overwritten.

## Spark checkpoint transition

`POLYHORIZON_LOW_MEMORY=1` configures eight shuffle partitions, one executor core,
bounded RocksDB memory and Kafka batches of at most 10,000 offsets. Bronze-to-Gold
Delta replay is limited to 32 files per microbatch.

Only Gold's output and checkpoint gain the suffix `-p8-v1`. The first run replays
retained Bronze into this separate Gold projection; subsequent runs resume its
checkpoint. **Never delete or edit the old checkpoints.** Bronze's checkpoint,
dead-letter checkpoint, Kafka offsets and Bronze data are unchanged, so changing
Gold state partitioning does not re-ingest Kafka into Bronze. Initial replay can
take longer; watermarked live Gold is not used as proof of complete historical
coverage. Post-close qualification independently recomputes from pinned Bronze.

The old Gold path remains available for comparison/rollback. Do not alternate
profiles during a session or run both drivers against shared Bronze checkpoints.
Before resuming automated collection, ensure no manual driver is running.

## Bounded completion and historical features

Post-close OHLC aggregation runs in Spark. Only the current session's bars are
collected to pandas, with an explicit calendar-derived row cap. Trade counts,
invalid prices/volumes and maximum gaps are checked in distributed Spark and
only one summary per symbol reaches the driver. No full tick-frame `toPandas`.

Rolling features use at most the configured number of preceding bars per symbol
plus the current session. This preserves the existing inclusive rolling-window
semantics. Finding historical bars still scans retained Bronze in Spark; it does
not load that history into the driver. Releases contain current-session features
only. The publisher rebuilds the training snapshot from the qualified offline
history rather than erasing earlier days when publishing a session-sized release.

Full-session qualification, finite metrics, minimum sample counts, calibration,
baseline comparison and champion gates are unchanged. This profile is not proof
that a complete session or model is production-ready.

## Verification and shutdown

```bash
docker stats --no-stream
docker compose -f docker-compose.yaml -f docker-compose.local-workers.yaml \
  -f docker-compose.low-memory.yaml ps
```

Monitor memory through the post-close peak, Kafka progress, batch duration and
lag, `OOMKilled` state, restart counts and session report. Stopping this profile
uses `compose stop`, never `down -v`. The automatic supervisor remains disabled
until explicitly re-enabled.
