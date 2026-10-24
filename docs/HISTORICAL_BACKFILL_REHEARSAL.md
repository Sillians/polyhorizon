# Historical backfill and readiness rehearsal

## Contract

Historical candles are **training data**, not evidence of live tick continuity.
`nyse-historical-bars-v1` requires all three product symbols (NVDA, AAPL, MSFT),
every expected 30-minute bar of every completed NYSE session in the requested
inclusive date range, early-close/holiday handling, unique keys, finite positive
OHLCV, consistent price bounds, integer BIGINT volume, and a content checksum.
There is no interpolation, forward-fill, or silent exclusion of missing sessions.
Qualification rejects unfinished sessions and future retrieval timestamps (with
a five-minute clock-skew allowance); training also excludes future event times.

The source manifest records provider, explicitly declared split-adjustment policy,
request/content hashes, retrieval time, and per-session coverage. The adjustment
declaration must be confirmed with the provider/export; coverage validation cannot
prove vendor adjustment semantics or authenticity. Do not relabel adjusted data
as unadjusted. The default training query accepts only unadjusted historical bars,
to avoid mixing adjusted archives with the unadjusted live feed. Split-adjusted
archives can be qualified/stored, but are not consumed by this training contract.

Features use the live `ohlcv-rolling-v1` definition: a trailing `lookback_bars + 1`
row window, sample standard deviation, and exclusion of the first warm-up row per
symbol. Raw and feature Delta tables are immutable version 0 releases under a
deterministic dataset UUID. `manifest.json` is stored beside the feature table;
retry checks existing content and never overwrites a different manifest. Finnhub
responses are cached per symbol/session with atomic, exclusive writes. Bounded
connection/429/5xx retries and sequential throttling prevent request bursts.
No-data, access-denied, malformed candles, or incomplete coverage fail closed.
Requests carry the provider token in a header, never in logged URLs.

Publication rechecks pinned raw coverage, feature values, and manifest evidence
before connecting to PostgreSQL. Under the existing publication advisory lock,
it writes `<snapshot_table>_historical` and a `complete` ledger entry in one
transaction. It does **not** write the live offline table, move the live snapshot
pointer, call Feast, change Redis, or activate a model. Matching historical overlaps
are retained with original lineage; differing overlaps are rejected and require an
explicit migration or non-overlapping range. Live publication and operations queries
exclude historical releases from their latest-live selection.

Training unions completed, qualified live/history rows with live precedence for
overlapping keys, logs exact dataset versions, and requires at least 60 observed
qualified sessions per product symbol by default (`data.min_history_sessions`).
`min_days_required: 180` is the calendar lookback, not proof of 180 days of data.
Both source policies are accepted by training/governance/activation; source runs
assembled by the loader use `nyse-qualified-data-v1`. Data qualification never
waives holdout evaluation, calibration, baseline comparison, governance approval,
or replica preparation/activation checks.

## Run historical collection and publication

From the repository root, with the local `.env`, locked Python environment, and
the normal local SeaweedFS/PostgreSQL services available:

```bash
.venv/bin/python -m scripts.ops.backfill_history \
  --start 2026-04-01 --end 2026-10-02 \
  --adjustment unadjusted \
  --manifest .cache/historical/release.json
```

Dates are examples; choose the actual source-supported range and confirm adjustment
semantics first. Ranges are bounded to 730 calendar days. This command collects,
qualifies, computes features, and saves evidence; it does not publish. Add
`--publish` to perform the explicit training-only publication. Repeating the same
command resumes cached provider responses and the same immutable release.

For a provider export, add `--csv /absolute/path/bars.csv`. Required columns:

```text
symbol,window_start,window_end,open,high,low,close,total_volume
```

Use timezone-qualified timestamps, with `window_start` at the regular-session
bar opening and `window_end` 30 minutes later. CSV file modification time is
recorded as retrieval time and must be after the final completed session.
Preserve the export unchanged for reproducible retries. The export must contain
exactly the requested regular-session range, not pre/post-market bars.

Prefect also has an unscheduled `historical-backfill-manual` deployment in the
features Docker pool. Supply `start_date`, `end_date`, `adjustment`, `feature_root`,
and `raw_root`; `cache_dir` defaults to the durable `/app/backfill-cache` mount.
Create a host directory and set `PREFECT_BACKFILL_CACHE_DIR` to its absolute path
before registering production deployments. No provider credentials are flow
parameters. Runtime credentials come from the read-only mounted `.env`.

The returned manifest can be submitted to `feature-store-after-close` with
`dataset_release=<manifest>`, **`apply_changes=false`, `materialize=false`**.
Historical publication skips online freshness/drift reporting. Publishing the
first historical release (or a new live release) creates the additional history
table required by the upgraded training query. Then run the existing
`training-quarterly` deployment manually with `run_tuning=false` for the initial
rehearsal. That flow retains governance and activation checks; a trained model is
not automatically a qualified champion. Keep recurring training schedules paused
until the real rehearsal has passed.

## Browser rehearsal

```bash
cd polyhorizon/frontend
npm ci
npx playwright install chromium
npm run test:e2e
```

Default mode starts isolated loopback servers on ports 38180/38173, trains a tiny
return model, registers/loads its artifact, calls real serving HTTP routes, and
checks chart/quantile/history/provenance rendering, selection invalidation, and
the locked `/ops` workspace, actual operator authentication, evidence inspection,
and relocking. Missing operational evidence remains visibly blocked. It does not
intercept/mock browser API requests.
Bars are synthetic, freshness is intentionally bypassed, and the fixture champion
is **not** a production governance-qualified model. This is frontend integration
evidence only. It never mounts the developer `.env` or uses production credentials.

For the real running frontend/API, set `REHEARSAL_BASE_URL` to its frontend URL,
and `REHEARSAL_FORECAST_KEY` if required; supply `REHEARSAL_OPERATOR_KEY` for the
admin rehearsal, then run the same command. This starts no
fixture servers and does not bypass freshness. Playwright traces are disabled for
real-stack runs to avoid retaining credentials. Reports/screenshots are local
ignored artifacts under `polyhorizon/frontend/test-results/`; inspect them before
sharing. The browser harness follows Playwright's documented
[web-server lifecycle](https://playwright.dev/docs/test-webserver).

## Recovery and serving proof

```bash
docker compose -f tests/integration/features/compose.yaml up -d --wait
PUBLICATION_E2E=1 .venv/bin/python -m pytest -q \
  tests/integration/features/test_historical_publication.py \
  tests/integration/features/test_operational_drills.py
docker compose -f tests/integration/features/compose.yaml down
```

The isolated stack uses ports 15432 (Postgres), 16379 (Redis), 18333 (S3), and
19093 (Alertmanager). The alert test temporarily listens on 18081 for Docker's
host-gateway webhook. It sends notifications only to that local test receiver.
Restore tests create/drop only UUID-named disposable databases; publication tests
create/remove only their own UUID-named schema and bucket. No application volume
is deleted. The restore comparison verifies feature-to-publication lineage, not
all application database/model/object-store recovery. The existing restore helper
now creates a fresh verifier database instead of dropping a fixed existing name,
and uses `pg_restore --exit-on-error`.

With a qualified champion and fresh live features deployed, export
`READINESS_FORECAST_KEY` and `READINESS_OPERATOR_KEY` through your secret tooling,
then run:

```bash
.venv/bin/python -m scripts.ops.readiness_report \
  --replica https://replica-1.example.com \
  --replica https://replica-2.example.com
```

List every serving replica, not only Traefik. The read-only command requires a
stably loaded champion (no activation grace), eligible operations preflight,
governance evidence, latest-close online/offline parity, successful uncached
forecasts for all three symbols, ordered quantiles, matching model versions, and
an unchanged champion after inference. It emits JSON and exits nonzero on gaps.
It reports **serving readiness**, not blanket production operational readiness.

Production sign-off additionally needs: immutable release images and the normal
`docker-compose.prod.yaml`/Traefik readiness route; real historical and latest-live
qualification; held-out model approval; the real-stack browser report; actual
backup restoration including model/object-store/Feast artifacts; an actual
production alert receiver receipt; and restart/rollback evidence on the deployed
fleet. A local drill or a healthy HTTP endpoint cannot substitute for these.

## Verification recorded on 2026-10-03

| Check | Result | Scope |
| --- | --- | --- |
| Historical Delta/S3/PostgreSQL publication | Passed | Immutable retry, durable manifest, forged-evidence rejection, no online mutation, minimum-history gate |
| Chromium HTTP forecast rehearsal | Passed | Trained tiny-model artifact, synthetic data, actual serving API, frontend and locked operations |
| PostgreSQL dump/restore drill | Passed | Disposable test databases, recovered feature/publication lineage |
| Alertmanager firing/resolved notifications | Passed | Actual local receiver receipts, not production delivery |
| Frontend unit tests/build | Passed | 16 unit tests, Vite 8.3.2 production build |
| Frontend dependency installation audit | Zero known vulnerabilities | Updated build/test dependencies; not an image or Python vulnerability scan |
| Full Python regression suite | 267 passed, 8 skipped | Optional external-stack tests were then run separately where listed; final clock-boundary checks also passed in a targeted 15-test run |
| Live Spark/Delta/PostgreSQL/Feast gate | Passed | Pending-online retry, Redis parity, preservation of previous live sessions |
| Finnhub historical access | Blocked: HTTP 403 | One bounded candle request using the local token |
| Main application Compose stack | Not running during inspection | No real-stack model/frontend/fleet readiness claim |

The authenticated browser run passes three checks: client-key forecast rendering,
rejection of missing/client-only credentials for operations, and operator
inspection/relocking with disabled actions staying disabled. Screenshots in fixture
mode carry a prominent synthetic-rehearsal label. Isolated rehearsal containers
and their network were removed after the drills; no application volumes were deleted.

GitHub CI now runs the browser fixture and historical publication/recovery drills
and uploads browser evidence. These additions have been verified locally; the
remote GitHub run and rebuilt release images are not yet proven. Real-data execution needs working provider
access or a suitable CSV export, a confirmed adjustment policy, and the chosen
historical range. Neither production schedules nor the real champion were changed.
