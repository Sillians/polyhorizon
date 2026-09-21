export function historyAtCutoff(data, cutoff) {
  const end = Date.parse(cutoff);
  if (!Number.isFinite(end)) return [];
  return (data || []).filter(row => row.close != null && Number.isFinite(Number(row.close)) && Date.parse(row.event_timestamp) <= end)
    .sort((a, b) => Date.parse(a.event_timestamp) - Date.parse(b.event_timestamp))
    .slice(-65).map(row => ({ timestamp: row.event_timestamp, price: Number(row.close) }));
}

export function validateForecast(result, horizon) {
  if (!Array.isArray(result.predictions) || result.predictions.length !== horizon) throw new Error("The API returned an unexpected forecast length.");
  let previous = Date.parse(result.features_timestamp);
  if (!Number.isFinite(previous) || !Number.isFinite(result.base_price)) throw new Error("The forecast is missing a valid data cutoff or observed price.");
  for (const row of result.predictions) {
    const time = Date.parse(row.timestamp);
    if (!Number.isFinite(time) || time <= previous || ![row.p10, row.p50, row.p90].every(Number.isFinite) || row.p10 > row.p50 || row.p50 > row.p90) {
      throw new Error("The API returned invalid forecast timestamps or quantiles.");
    }
    previous = time;
  }
  return result;
}

export function freshness(timestamp, now = Date.now()) {
  const elapsed = now - Date.parse(timestamp);
  if (!Number.isFinite(elapsed)) return "Data age unavailable";
  if (elapsed < -60000) return "Data timestamp is in the future";
  const minutes = Math.max(0, Math.floor(elapsed / 60000));
  const age = minutes < 60 ? minutes + "m" : minutes < 1440 ? Math.floor(minutes / 60) + "h" : Math.floor(minutes / 1440) + "d";
  return age + " old";
}

// Deliberately fictional instrument: this is not another production symbol allowlist.
export const demoMetadata = {
  supported_symbols: ["DEMO"], default_symbol: "DEMO", bars_per_day: 13,
  horizon_days: [1, 2, 3], max_prediction_length: 39, currency: "USD",
  market_timezone: "America/New_York", frequency: "30min", target_type: "return",
};

export function demoForecast(horizon) {
  const history = Array.from({ length: 39 }, (_, i) => {
    const day = Math.floor(i / 13);
    const timestamp = new Date(Date.UTC(2026, 6, 15 + day, 14, (i % 13) * 30)).toISOString();
    return { timestamp, price: 120 + i * .12 + Math.sin(i * .5) * .65 };
  });
  const base = history.at(-1).price;
  const predictions = Array.from({ length: horizon }, (_, i) => {
    const p50 = base + .09 * (i + 1) + Math.sin(i * .25) * .3;
    return { step: i + 1, timestamp: new Date(Date.UTC(2026, 6, 20 + Math.floor(i / 13), 14, (i % 13) * 30)).toISOString(), p10: p50 - .6 - i * .05, p50, p90: p50 + .6 + i * .05 };
  });
  return { history, result: { symbol: "DEMO", horizon, predictions, base_price: base, absolute_change: predictions.at(-1).p50 - base, percent_change: predictions.at(-1).p50 / base - 1, features_timestamp: history.at(-1).timestamp, model_version: "synthetic", model_uri: "Not a trained model", cached: false, generated_at: "2026-07-20T13:00:00Z" } };
}
