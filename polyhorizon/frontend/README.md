# PolyHorizon Frontend (Vite)

The symbol selector uses `supported_symbols` from `/v1/metadata`, backed by the
shared [product allowlist](../../docs/PRODUCT_SYMBOLS.md). Do not add a separate
frontend ticker list; change product membership at its shared source.

See [`docs/SYSTEM_SERVICES.md`](../../docs/SYSTEM_SERVICES.md) for this service's
role in the complete platform and its API dependency boundaries.

Two focused workspaces in one Vite application: the forecasting product at `/`
and a restricted operations console at `/ops`. Both use the same backend contract.
The visual system is warm ivory, charcoal, and deep teal, with amber warnings,
high-contrast focus indicators, and responsive layouts (no external fonts).

## Features
- Forecast requests against `/v1/forecast`
- Model metadata from `/v1/model`
- Readiness status from `/v1/health/ready`
- Feature debug viewer from `/v1/features/debug`
- Horizon selector in **days** (1–3 days), converted to **bars** for the API (13 bars/day)
- Toggle to show **days** vs **bars** in the forecast table
- Interactive chart with tooltip (p10/p50/p90)
- Forecast table view under the chart
- Last observed close, end-of-horizon median, and P10–P90 prices; band width is
  explicitly **not** presented as accuracy or calibrated confidence
- Runtime controls populated from `GET /v1/metadata` instead of hard-coded client values
- Timestamp-aware market-day aggregation and currency/percentage formatting
- Cancellable requests with API timeouts and explicit authentication, rate-limit,
  readiness, and offline error states
- Safe DOM-based rendering for feature data
- Server-gated feature debugging and confirmed champion activation controls
- Keyboard and pointer chart inspection with grid, price, and market-time axes
- Historical closes from `/v1/features`, clipped at the forecast data cutoff,
  with a visible forecast boundary and synchronized table selection
- Explicit data age, target timestamps, model version, cached/fresh source, and
  response time (a cached response timestamp is not the original inference time)
- Selection changes discard old results; requests are cancelled on reconnect
- Opt-in, clearly labeled synthetic `DEMO` instrument; no automatic mock fallback
- System font stack with no third-party font request

## Local Dev (Vite)
```bash
cd polyhorizon/frontend
npm install
npm run dev
```
Open `http://localhost:5173`.

### API Base URL
The app defaults to `window.location.origin` and uses the Vite proxy to forward `/v1` and `/metrics` to `http://localhost:8000` in dev.

To override directly, set:
```
VITE_API_BASE_URL=http://localhost:8000
```

For single‑domain production, set:
```
FRONTEND_API_BASE_URL=https://app.example.com
```

## Build
```bash
npm run build
npm run preview
```

## Docker
Build image:
```bash
docker build -f docker/frontend/Dockerfile -t polyhorizon-frontend:latest .
```

Run locally:
```bash
docker compose up -d frontend
```

## Production (Single Domain via Traefik)
Frontend and API can share a single domain (no CORS required). Traefik routes:
- `/` to the frontend
- `/v1` and `/metrics` to the serving API

Set these in `.env`:
```
POLYHORIZON_DOMAIN=app.example.com
TRAEFIK_ACME_EMAIL=you@example.com
TRAEFIK_DASHBOARD_DOMAIN=traefik.app.example.com
TRAEFIK_DASHBOARD_USERS=admin:$2y$10$replace_with_htpasswd_hash
```

The frontend is served at:
```
https://app.example.com
```

## CI/CD
The frontend pipeline builds and pushes an image to GHCR. If `FRONTEND_API_BASE_URL`
is set as a GitHub secret, it is baked into the build.

Required secrets for deploy:
```
DEPLOY_HOST
DEPLOY_USER
DEPLOY_SSH_KEY
DEPLOY_PATH
FRONTEND_API_BASE_URL
```

## Runtime Metadata

The frontend loads its runtime contract from `GET /v1/metadata`. Configure
symbols, default symbol, market bars per day, horizon choices, currency, and
timezone under `client_metadata` in
`polyhorizon/serving/configs/serving_config.yaml`.

The metadata endpoint also supplies the configured API-key header and request
timeout, keeping browser behavior aligned with serving configuration.

Forecast credentials live in Connection settings, not in the primary workflow.
Credentials are in memory only, never persisted in browser storage. Remote API
URLs require HTTPS; localhost HTTP is supported. Redirects are rejected to avoid
forwarding credentials to a different destination.

`/ops` requires a separate operator key checked by `GET /v1/ops/session`.
Configure `SERVING_OPERATOR_API_KEYS` separately from `SERVING_API_KEYS`.
Debugging and champion reload additionally require their server-side capability
flags and `SERVING_REQUIRE_API_KEY=true`. Reload requires an explicit confirmation.
The server enforces this boundary even if a privileged path is mistakenly placed
in the authentication exemption list. This is an API-key role boundary, not a
full user-account/SSO system. Production should also restrict operations ingress.

The UI does not invent market-open status or evaluation metrics. Data age is
wall-clock age and does not itself classify holidays or market closures; the
serving API enforces post-close freshness against the NYSE calendar. Trading bars
are evenly spaced on the chart; gaps between sessions are compressed. Daily table
rows use the last **forecast** bar on each market date, not a promised full-day close.
If historical prices fail to load, the forecast remains available with an explicit
warning and its observed base price.

## Source layout

- `src/main.js`: workspace controller, routing, state and safe DOM rendering
- `src/api.js`: transport, timeout/cancellation, auth and error mapping
- `src/forecast.js`: timestamp/quantile validation, cutoff filtering, demo fixtures
- `src/chart.js`: SVG history/forecast chart and accessible inspection
- `src/lib.js`: formatting, metadata-driven horizons and market-date grouping
- `src/style.css`: responsive visual system

Both Vite and production Nginx support direct navigation to `/ops`. The console
shell is not secret; privileged data/actions are protected on the API.

The production Nginx configuration adds CSP, HSTS, anti-framing, MIME-sniffing,
referrer, permissions-policy, and static-asset cache headers.

## Tests

```bash
npm test
npm run build
```

Tests cover 1-, 13-, and 39-bar alignment, quantile validation, history cutoff,
daily aggregation, missing values, URL safety, API headers, cancellation,
timeouts, and actionable errors. See `docs/FRONTEND_REDESIGN.md` at repository root
for the seven-part implementation and browser verification checklist.
