import test from "node:test";
import assert from "node:assert/strict";
import { ApiClient, errorMessage, validateBaseUrl } from "../src/api.js";
import { demoForecast, historyAtCutoff, validateForecast, freshness } from "../src/forecast.js";
import { formatCurrency, formatPercent, aggregateDaily } from "../src/lib.js";

for (const horizon of [1, 13, 39]) {
  test(horizon + "-bar forecast keeps predictions and target timestamps aligned", () => {
    const full = demoForecast(39).result;
    const short = validateForecast(demoForecast(horizon).result, horizon);
    assert.deepEqual(short.predictions, full.predictions.slice(0, horizon));
    assert.equal(short.predictions.at(-1).timestamp, full.predictions[horizon - 1].timestamp);
  });
}
test("history excludes observations after the forecast cutoff", () => {
  const history = historyAtCutoff([
    { event_timestamp: "2026-07-20T16:00:00Z", close: 102 },
    { event_timestamp: "2026-07-20T14:00:00Z", close: 100 },
    { event_timestamp: "2026-07-20T15:00:00Z", close: null },
  ], "2026-07-20T15:00:00Z");
  assert.deepEqual(history, [{ timestamp: "2026-07-20T14:00:00Z", price: 100 }]);
});
test("invalid or misaligned forecast is rejected instead of plotted", () => {
  const result = demoForecast(13).result;
  assert.throws(() => validateForecast(result, 39), /length/);
  result.predictions[0].timestamp = result.features_timestamp;
  assert.throws(() => validateForecast(result, 13), /timestamps/);
  const crossing = demoForecast(1).result;
  crossing.predictions[0].p10 = crossing.predictions[0].p90 + 1;
  assert.throws(() => validateForecast(crossing, 1), /quantiles/);
});
test("missing values are not fabricated zeros and old data is explicit", () => {
  assert.equal(formatCurrency(null), "—"); assert.equal(formatPercent(""), "—");
  assert.equal(freshness("2026-07-20T14:00:00Z", Date.parse("2026-07-21T14:00:00Z")), "1d old");
  assert.equal(freshness(null), "Data age unavailable");
});
test("daily rows preserve the last bar's quantiles and target time", () => {
  const result = demoForecast(39).result;
  assert.deepEqual(aggregateDaily(result.predictions).map(r => r.timestamp), [12, 25, 38].map(i => result.predictions[i].timestamp));
});
test("connection URLs reject credential leakage and remote plaintext", () => {
  assert.equal(validateBaseUrl("http://127.0.0.1:8000/"), "http://127.0.0.1:8000");
  assert.equal(validateBaseUrl("https://example.com"), "https://example.com");
  for (const url of ["http://example.com", "https://secret@example.com", "https://example.com?key=secret", "javascript:alert(1)"]) assert.throws(() => validateBaseUrl(url));
});
test("API uses configured auth header, rejects redirects and encodes JSON", async () => {
  let recorded;
  const api = new ApiClient("http://localhost:8000", async (...args) => { recorded = args; return { ok: true, json: async () => ({ ready: true }) }; });
  api.header = "X-Test"; api.key = "consumer";
  await api.request("test", "/v1/forecast", { method: "POST", body: { horizon: 13 } });
  assert.equal(recorded[1].headers["X-Test"], "consumer");
  assert.equal(recorded[1].redirect, "error");
  assert.equal(recorded[1].body, '{"horizon":13}');
  assert.equal(api.pending.size, 0);
});
test("new same-key requests cancel the old request", async () => {
  const api = new ApiClient("http://localhost:8000", async (_url, options) => {
    return await new Promise((resolve, reject) => {
      options.signal.addEventListener("abort", () => reject(new DOMException("Cancelled", "AbortError")));
      if (_url.endsWith("new")) resolve({ ok: true, json: async () => "new" });
    });
  });
  const old = api.request("forecast", "/old");
  const rejected = assert.rejects(old, { name: "AbortError" });
  assert.equal(await api.request("forecast", "/new"), "new");
  await rejected; assert.equal(api.pending.size, 0);
});
test("timeouts stop requests and clean pending state", async () => {
  const api = new ApiClient("http://localhost:8000", async (_url, options) => new Promise((_resolve, reject) => options.signal.addEventListener("abort", () => reject(new DOMException("Cancelled", "AbortError")))));
  api.timeout = 5;
  await assert.rejects(api.request("slow", "/slow"), { name: "TimeoutError" });
  assert.equal(api.pending.size, 0);
});
test("actionable auth, readiness, rate-limit and insufficient-data errors", () => {
  for (const status of [400, 401, 403, 404, 422, 429, 503]) assert.ok(errorMessage({ status }).length > 30);
  assert.match(errorMessage({ status: 403 }), /operator/);
  assert.match(errorMessage({ status: 503, code: "stale_features" }), /Forecast withheld/);
});
