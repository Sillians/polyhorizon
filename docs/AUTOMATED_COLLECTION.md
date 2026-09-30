# Automatic market collection

Local Prefect owns these recurring schedules in `America/New_York`:

| Deployment | Weekday preparation time | Behavior |
|---|---|---|
| sp500-universe-weekdays | 09:15 | Refresh governed universe; ingestion rejects stale/missing universe |
| streaming-market-hours | 09:25 | Start one Spark driver before trades arrive, resume checkpoints |
| ingestion-market-hours | 09:29 | Load configuration, wait for 09:30 market open, collect until calendar close |

The NYSE calendar excludes holidays and handles early closes. Cron timezone
handles daylight saving time. Queued runs from a previous NY trading date are
skipped rather than replayed. Ingestion tasks receive an absolute close deadline,
so retries cannot extend collection past the close. Spark drains at close plus
five minutes, then performs qualification. An incomplete dataset fails closed.

Collection schedules are enabled with:

```bash
bash scripts/deploy/deploy_local.sh --activate
```

Running the script without `--activate` pauses these schedules. Activation is
restricted to the three deployments above. Feature publication, training,
promotion and the separate publication watchdog remain unchanged/paused by this
local registration path. The Spark deployment explicitly sets
`qualification_only=True`; collection does not imply production data approval.

Concurrency is limited to one run per deployment. Spark's duplicate-app check
uses its actual application name, `Polyhorizon_streaming_job`. Local job containers
use the internal Spark master URL, not a developer `local[*]` setting. Workers
restart unless explicitly stopped. Kafka, storage, database, Spark master/workers
and Prefect infrastructure are stopped after the session when the host supervisor
below is installed. Without it, only the collection jobs stop at close.
No nightly volume deletion or checkpoint reset occurs.

## Local host supervisor

The supervisor now uses the [low-memory local overlay](LOW_MEMORY_LOCAL.md): two
Kafka brokers, one Spark worker and bounded container/job budgets. Existing Kafka
replicas must be migrated and verified before startup. Optional services remain off.

Install once from the repository, using the existing virtual environment:

```bash
.venv/bin/python -m scripts.ops.market_supervisor --plan
.venv/bin/python -m scripts.ops.install_market_supervisor
launchctl print gui/$(id -u)/com.polyhorizon.market-supervisor
```

The per-user LaunchAgent checks every minute while logged in. It uses the NYSE
calendar (including holidays, daylight saving changes and early closes):

- Open minus 20 minutes: request Docker Desktop startup if unavailable; start
  the explicit collection infrastructure, register collection-only schedules,
  start the ingestion/streaming workers. Existing topics, buckets and images must
  already be bootstrapped; daily startup does not download images or reset storage.
- Universe refresh at 09:15, Spark at 09:25, ingestion at 09:29; ingestion waits
  until the actual open. Startup has a three-attempt budget per session. It never
  replays a failed collection flow automatically.
- During collection: check required container health, expected flow states,
  Spark app presence, per-partition Kafka offsets, Bronze version progress,
  committed event-time freshness, host disk and Docker data disk capacity.
  These are progress/freshness checks, not exact consumer-group lag accounting.
- Close plus 20 minutes: wait for active jobs and qualification, then generate
  a read-only report before stopping the collection service allowlist. If jobs
  remain at close plus 60 minutes, request cancellation and stop only containers
  bearing this session's exact Prefect flow-run IDs. Never kill by process name.
- Unrelated active Prefect runs, a remaining Spark app, or failed control-plane
  probes block infrastructure shutdown. Operator intervention is preferable to
  stopping unrelated work. Completed job containers remain for inspection.

Private state and daily reports: `artifacts/market-supervisor/`. Each daily JSON
report is `qualified`, `incomplete`, or `operational_failure`, with measured gaps
and version-pinned qualification evidence when available. Qualification requires
a saved completed manifest and independently rechecks Bronze and completed Gold;
missing evidence never becomes a passing report. A failed report does not block
safe shutdown after jobs have stopped. No data is materialized or model promoted.

macOS notifications are best-effort and deduplicated by current condition; local
logs/reports remain authoritative. Allow notifications when prompted. This does
not provide remote alerts when the Mac is offline. `caffeinate` prevents idle
sleep during the managed window on AC power, not forced sleep, closing the lid,
power loss, or shutdown. **The Mac must already be awake and the user logged in
before pre-open.** No privileged wake schedule or system power settings are changed.

Optional dashboards, Redis, feature workers, serving and training are not started
or stopped by this allowlist. Docker Desktop itself stays open overnight. Volumes
and checkpoints are preserved. Partial startup and prior-day sessions are tracked
across supervisor restarts. A missed entire session does not trigger after-hours
collection. Reset a retry budget only after investigating its state/error report.

Disable future supervision without deleting data or stopping containers:

```bash
.venv/bin/python -m scripts.ops.install_market_supervisor --uninstall
```

Uninstalling does not pause Prefect schedules. If infrastructure remains running,
use the existing deployment script without `--activate` to pause collection too.

## Operational prerequisites and production boundary

The schedules are on the existing local Docker/Prefect installation, not on a
new cloud server. This machine must remain awake, powered, online, with Docker
and all infrastructure running. Schedules cannot wake a sleeping Mac or start a
stopped Docker engine. Check the next scheduled runs in Prefect after maintenance.

A production rollout requires an identified always-on host, registry-pinned
images, secrets provisioning, storage backups and verified alert delivery.
The Docker-socket worker profile is a trusted local convenience, not a hardened
multi-tenant production configuration. Move this same calendar/deadline behavior
to the chosen production environment before calling the deployment production.

No continuous full-session qualification has yet passed. Before enabling model
operations, require a full qualified release and the separate data/history and
model-evaluation gates.
