# S&P 500 Universe Service

This service owns the canonical, point-in-time equity universe used by market
ingestion and feature generation. It is a governed data product, not a
best-effort web scraper.

## Publication contract

Each refresh:

1. Fetches the configured source with bounded retries and timeouts.
2. Requires the expected constituents table and schema.
3. Normalizes symbols and rejects conflicts, invalid symbols, implausible
   constituent counts, or excessive membership churn.
4. Writes immutable CSV, JSON, and symbol-list artifacts under
   `universe/snapshots/YYYY/MM/DD/<run-id>/`.
5. Writes a versioned manifest containing source lineage, schema version,
   timestamp, membership changes, byte sizes, and SHA-256 checksums.
6. Updates compatibility objects and moves `universe/current.json` last.

The current pointer is the activation boundary. A failed fetch, validation, or
write leaves the previously activated universe intact.

Consumers resolve the immutable CSV through `universe/current.json` and verify
schema version, freshness, checksum, byte size, count, order, and uniqueness.
They fail closed rather than silently running ingestion or feature jobs with an
empty or untrusted universe.

## Orchestration

Prefect runs `sp500-universe-weekdays` at 07:00 America/New_York, ahead of the
09:30 ingestion deployment. Publication tasks have bounded retries, timeouts,
non-overlapping runs, and immutable production images. Prefect run state and the
activated manifest provide the operational audit trail.

### Image and entrypoint verification

The universe deployment shares `docker/ingestion/Dockerfile` with ingestion.
The image installs both locked extras, `ingestion` and `sp500_data` (including
BeautifulSoup), and puts `/app/.venv/bin` on `PATH` so Prefect and `python` use
those dependencies. Console logging works without a mounted `.env`; live
publication still needs the configured source and object-store credentials.

Every image build checks the flow referenced by `sp500-universe-weekdays` in
`prefect.yaml` and runs `python -m polyhorizon.sp500_data --help`. CI also runs
that check in a container with networking disabled before publishing images.

```bash
docker build -f docker/ingestion/Dockerfile -t polyhorizon-ingestion:universe-smoke .
docker run --rm --network none polyhorizon-ingestion:universe-smoke \
  python scripts/ci/smoke_universe_entrypoint.py
```

This smoke test checks imports and CLI startup, without fetching constituents,
starting a Prefect flow run, or activating a universe snapshot.

## Configuration

The default configuration is
`polyhorizon/sp500_data/configs/sp500_variables.yaml`.

- `minimum_constituents`: lower safety bound, default 450.
- `maximum_constituents`: upper safety bound, default 550.
- `maximum_change_fraction`: maximum additions plus removals relative to the
  previous universe, default 10%.
- `request_timeout_seconds`: source request timeout, default 30.
- `UNIVERSE_CURRENT_KEY`: governed pointer key used by consumers.
- `UNIVERSE_MAX_AGE_HOURS`: maximum consumer freshness, default 30.

Run manually with:

```bash
uv run python -m polyhorizon.sp500_data
```

Run tests with:

```bash
pytest -q tests/sp500_data
```
