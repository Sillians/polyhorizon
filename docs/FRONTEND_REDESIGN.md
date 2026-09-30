# Forecast workspace redesign

## Seven changes

1. **Separate audiences.** `/` is the forecast product; `/ops` is an operator
   console in the same frontend. Settings are out of the main forecasting flow.
   API authorization, not navigation visibility, protects operator actions.
2. **Information hierarchy.** Instrument and horizon controls lead to a compact
   observed/median/interval summary, the main chart, and expandable values and
   provenance panels. The layout stacks on narrow screens.
3. **Chart context.** Observed closes are clipped to the forecast feature cutoff.
   A dashed boundary distinguishes observations from the median and P10–P90 band.
   Pointer/keyboard inspection and row selection stay synchronized. Bars are
   equally spaced and session gaps compressed, explicitly disclosed.
4. **Trust and semantics.** Show observed close (not “live price”), target range,
   data cutoff and age, currency/timezone, model version, cached/fresh source, and
   response time. Quantiles are not calibrated accuracy guarantees. Market-open
   status and unavailable evaluation metrics are not fabricated.
5. **Honest states.** Connecting, unavailable, unauthenticated, forbidden,
   rate-limited, invalid/insufficient-data, timeout, and loading states have
   actionable messages. Invalid timestamps/quantiles are rejected. Changing
   selection clears results; reconnecting cancels requests. Demo is explicit,
   opt-in, deterministic and fictional, with no production allowlist duplication.
6. **Visual overhaul.** Warm ivory surfaces, charcoal text, deep teal actions,
   sage quantile bands, and amber warnings replace pastel gradients/coral.
   Visible focus rings, keyboard chart/table interaction, a skip link, semantic
   forms and live status messaging support accessibility.
7. **Maintainable modules and tests.** Transport, forecast validation/fixtures,
   chart rendering, formatting and workspace control are separated. Metadata
   remains the source of production symbols, horizons, timezone and capabilities.

## Operator migration

- Keep forecast credentials in `SERVING_API_KEYS`.
- Put distinct operator credentials in `SERVING_OPERATOR_API_KEYS`.
- The existing `SERVING_RELOAD_API_KEY` used by deployment/training must belong
  to the operator list, **not** the consumer list.
- Feature debug/reload flags require API-key auth and a configured operator key.
- Empty operator keys keep the console locked. Overlapping credential lists are
  rejected. Privileged checks precede authentication exemptions.
- `GET /v1/ops/session` returns the authenticated operator role and capabilities.
- Backend enforcement also runs in app instances without initialized models.

This is not SSO, per-user RBAC, or an audit-log system. Keep operator keys secret,
use HTTPS and restrict production console/API ingress where appropriate.

### Provisioning and using credentials

These are application-owned secrets, not Finnhub/provider credentials. Generate
independent cryptographically random 32-byte keys for each role and environment
(for example, run `openssl rand -hex 32` separately for every new key).

Store local credentials in the ignored `.env` and production credentials in the
ignored `.env.prod`, with file permissions `0600`. Set `SERVING_REQUIRE_API_KEY=true`.
Use a distinct forecast key in `SERVING_API_KEYS`; put the operator key in
`SERVING_OPERATOR_API_KEYS` and that same operator value in `SERVING_RELOAD_API_KEY`.
Do not reuse local keys in production or put real keys in templates/documentation.

In the frontend, use the forecast key under **Connection settings** and the
operator key only on the **Operations** login. Updating environment files does
not update an already running process: recreate/restart the relevant serving
and deployment/training caller services through the normal deployment workflow.
Production secrets must also reach the production host or secret manager; a local
`.env.prod` edit is not a deployment. This provisioning does not itself enable
debugging, champion reload capability flags, or automatic activation schedules.

## Verification

```bash
cd polyhorizon/frontend
npm test
npm run build
cd ../..
uv run --project . --extra dev python -m pytest -q tests/unit/serving
python scripts/ci/validate_deployment_contract.py
```

Browser checklist:

- With the API offline, no fabricated prices appear and Generate is disabled.
- Explicit demo opt-in shows permanent synthetic labels and a fictional symbol.
- 13-/39-bar demo requests show matching target ranges; changing horizon clears
  the previous results before a new request.
- Arrow/Home/End chart keys and table row Enter/Space select matching values.
- Daily grouping retains the final forecast bar for each market date.
- Direct `/ops` navigation shows a locked console, not privileged data.
- Connection settings and confirmation dialogs are keyboard accessible.
- Check desktop and narrow/mobile layouts for clipping and horizontal overflow.

Live model training, production deployment and calibration evaluation are outside
this frontend change. Validate the connected experience against the deployed
serving service after provisioning the separate operator credential.

### Verification recorded for this change

- 16 frontend unit tests passed; production Vite build passed.
- 74 serving/model-activation regression tests passed, including operator-only
  reload, denied consumer access, disabled capabilities, CORS preflight/auth
  errors, and stale-feature error codes.
- Deployment contract validation and shell syntax checks passed.
- Browser checks passed for offline controls, explicit demo, selection clearing,
  13-/39-bar chart inspection, daily grouping, mobile overflow (390px viewport),
  desktop layout (1440px), connection URL validation, and the locked `/ops` route.
- Live API-backed forecasting and an unlocked console were not browser-tested:
  the local serving API was unavailable. Operator API behavior was verified with
  isolated application instances and stubs, not a production reload.
