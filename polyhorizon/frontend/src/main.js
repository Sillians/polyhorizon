import "./style.css";
import { ApiClient, errorMessage, validateBaseUrl } from "./api.js";
import { daysToBars, formatCurrency, formatPercent, formatTimestamp, aggregateDaily } from "./lib.js";
import { historyAtCutoff, validateForecast, freshness, demoMetadata, demoForecast } from "./forecast.js";
import { drawChart } from "./chart.js";

const $ = id => document.getElementById(id);
const api = new ApiClient(import.meta.env.VITE_API_BASE_URL || location.origin);
const state = { metadata: null, ready: false, demo: false, result: null, history: [], generation: 0, busy: false, operator: "", capabilities: null, select: null };
const isOps = location.pathname.replace(/\/$/, "") === "/ops";
const money = value => formatCurrency(value, state.metadata?.currency || "USD");
const time = value => formatTimestamp(value, state.metadata?.market_timezone);
function notice(message, tone = "") { $("notice").textContent = message; $("notice").dataset.tone = tone; }
function controls() {
  $("symbol").disabled = !state.metadata || state.busy;
  $("horizon").disabled = !state.metadata || state.busy;
  $("run").disabled = !state.ready || state.busy;
  $("run").textContent = state.busy ? "Generating…" : state.demo ? "Generate demo ↗" : "Generate forecast ↗";
  $("demo").textContent = state.demo ? "Exit demo" : "Explore a demo";
}
function selectOptions(id, options, selected) {
  $(id).replaceChildren(...options.map(([value, text]) => {
    const option = document.createElement("option");
    option.value = value; option.textContent = text; option.selected = String(value) === String(selected);
    return option;
  }));
}
function configure(metadata) {
  state.metadata = metadata;
  selectOptions("symbol", metadata.supported_symbols.map(symbol => [symbol, symbol]), metadata.default_symbol);
  selectOptions("ops-symbol", metadata.supported_symbols.map(symbol => [symbol, symbol]), metadata.default_symbol);
  selectOptions("horizon", metadata.horizon_days.map(days => [days, days + " trading day" + (days === 1 ? "" : "s") + " · " + daysToBars(days, metadata.bars_per_day, metadata.max_prediction_length) + " bars"]), 1);
  $("timezone").textContent = metadata.market_timezone;
  controls();
}
function clearForecast() {
  state.result = null; state.history = []; state.select = null;
  ["base", "median", "range"].forEach(id => $(id).textContent = "—");
  $("asof").textContent = "Not a live quote";
  $("change").textContent = "Relative to observed close";
  $("rows").replaceChildren(); $("provenance").replaceChildren();
  $("chart").setAttribute("hidden", ""); $("chart").replaceChildren(); $("empty").hidden = false;
  $("inspect").textContent = "No forecast selected.";
  $("chart-context").textContent = "Generate a forecast to explore the path ahead.";
  $("chart-title").textContent = "History meets possibility.";
}
function invalidate() { state.generation++; api.cancelAll(); state.busy = false; clearForecast(); controls(); }
function lockOps() {
  state.operator = ""; state.capabilities = null;
  $("operator-key").value = ""; $("ops-lock").hidden = false; $("ops-content").hidden = true;
  $("model-data").textContent = "Not loaded"; $("feature-data").textContent = "No feature data requested.";
  $("reload").disabled = true; $("features").disabled = true;
}

async function connect() {
  invalidate(); lockOps(); state.demo = false; state.ready = false; state.metadata = null; controls();
  const generation = state.generation;
  selectOptions("symbol", [["", "Unavailable"]]); selectOptions("horizon", [["", "Unavailable"]]);
  $("health").textContent = "Connecting…"; notice("Connecting to the forecasting service…");
  try {
    const metadata = await api.request("metadata", "/v1/metadata", { key: "" });
    if (generation !== state.generation) return;
    configure(metadata);
    api.header = metadata.api_key_header || "X-API-Key";
    api.timeout = (metadata.request_timeout_seconds || 30) * 1000;
    await api.request("health", "/v1/health/ready", { key: "" });
    if (generation !== state.generation) return;
    state.ready = true; $("health").textContent = "Service ready";
    notice(isOps ? "Service connected. Operator authentication is required to unlock this workspace." : "Connected. Select an instrument and horizon to generate a forecast.", "success");
  } catch (error) {
    if (generation !== state.generation || error.name === "AbortError") return;
    $("health").textContent = "Service unavailable";
    notice(errorMessage(error) + " No live forecast is being displayed.", "error");
  } finally { if (generation === state.generation) controls(); }
}

function inspect(row) {
  $("inspect").textContent = (state.demo ? "SYNTHETIC DEMO · " : "") + "Bar " + row.step + " · " + time(row.timestamp) + " · P10 " + money(row.p10) + " / P50 " + money(row.p50) + " / P90 " + money(row.p90);
  [...$("rows").children].forEach(tr => {
    const date = new Intl.DateTimeFormat("en-CA", { timeZone: state.metadata.market_timezone, dateStyle: "short" });
    const selected = $("aggregation").value === "daily"
      ? date.format(new Date(tr.dataset.timestamp)) === date.format(new Date(row.timestamp))
      : tr.dataset.timestamp === row.timestamp;
    tr.classList.toggle("selected", selected);
    tr.setAttribute("aria-selected", String(selected));
  });
}
function renderRows() {
  const result = state.result;
  if (!result) return;
  const rows = $("aggregation").value === "daily" ? aggregateDaily(result.predictions, state.metadata.market_timezone) : result.predictions;
  $("rows").replaceChildren(...rows.map(row => {
    const tr = document.createElement("tr"); tr.tabIndex = 0; tr.dataset.timestamp = row.timestamp;
    [row.step, time(row.timestamp), money(row.p10), money(row.p50), money(row.p90)].forEach(value => { const td = document.createElement("td"); td.textContent = value; tr.append(td); });
    const choose = () => state.select?.(result.predictions.findIndex(p => p.timestamp === row.timestamp));
    tr.onclick = choose; tr.onkeydown = event => { if (event.key === "Enter" || event.key === " ") { event.preventDefault(); choose(); } };
    return tr;
  }));
}
function renderForecast() {
  const r = state.result, final = r.predictions.at(-1), m = state.metadata;
  $("base").textContent = money(r.base_price);
  $("median").textContent = money(final.p50);
  $("range").textContent = money(final.p10) + " – " + money(final.p90);
  $("asof").textContent = (state.demo ? "Synthetic observation · " : "Observed · ") + time(r.features_timestamp);
  $("change").textContent = formatPercent(final.p50 / r.base_price - 1) + " vs. observed close · " + r.horizon + " bars";
  $("chart-title").textContent = r.symbol + " / " + (state.demo ? "Synthetic price scenario" : "Price forecast");
  $("chart-context").textContent = time(r.predictions[0].timestamp) + " → " + time(final.timestamp) + " · " + (state.history.length ? state.history.length + " observed bars" : "Historical series unavailable; showing the observed base only");
  const fields = [
    ["Source", state.demo ? "SYNTHETIC DEMO — not live market data" : r.cached ? "Cached forecast" : "Fresh inference"],
    ["Data cutoff", time(r.features_timestamp)],
    ["Data age", state.demo ? "Fixed example dates" : freshness(r.features_timestamp) + ". Post-close freshness is validated by the API against the NYSE calendar."],
    ["Response time", time(r.generated_at) + (r.cached ? " (cache retrieval, not inference time)" : "")],
    ["Model version", r.model_version],
    ["Price conversion", state.demo ? "Synthetic price path; no model conversion" : m.target_type === "price" ? "Direct price target" : (m.return_to_price_method ? m.return_to_price_method + " return-to-price conversion" : "Conversion method not reported by this API")],
    ["Output", "Price quantiles in " + m.currency + "; displayed to 2 decimal places; model target: " + m.target_type],
    ["Cadence", m.frequency + " bars · post-close publication once per NYSE session · " + (m.publication_delay_minutes ?? 60) + " minutes after close · " + m.market_timezone],
    ["Interpretation", "P10 / P50 / P90 are model quantiles, not calibrated coverage guarantees."],
  ];
  $("provenance").replaceChildren(...fields.flatMap(([label, value]) => { const dt = document.createElement("dt"), dd = document.createElement("dd"); dt.textContent = label; dd.textContent = value; return [dt, dd]; }));
  renderRows(); $("empty").hidden = true; $("chart").removeAttribute("hidden");
  state.select = drawChart($("chart"), r, state.history, m, inspect);
}
async function runForecast() {
  if (!state.ready || state.busy) return;
  invalidate();
  const generation = state.generation, m = state.metadata;
  const horizon = daysToBars($("horizon").value, m.bars_per_day, m.max_prediction_length);
  const symbol = $("symbol").value;
  state.busy = true; controls(); notice(state.demo ? "Building a synthetic example…" : "Generating forecast and retrieving observed history…");
  try {
    let historyWarning = "";
    if (state.demo) {
      const sample = demoForecast(horizon); state.result = validateForecast(sample.result, horizon); state.history = sample.history;
    } else {
      const [forecast, history] = await Promise.allSettled([
        api.request("forecast", "/v1/forecast", { method: "POST", body: { symbol, horizon, use_cache: true } }),
        api.request("history", "/v1/features?symbol=" + encodeURIComponent(symbol) + "&limit=100"),
      ]);
      if (generation !== state.generation) return;
      if (forecast.status === "rejected") throw forecast.reason;
      state.result = validateForecast(forecast.value, horizon);
      if (forecast.value.symbol !== symbol) throw new Error("The API returned a different instrument. Forecast discarded.");
      state.history = history.status === "fulfilled" ? historyAtCutoff(history.value.data, forecast.value.features_timestamp) : [];
      if (!state.history.length) historyWarning = " Historical prices could not be loaded; only the observed base price is shown.";
    }
    renderForecast();
    notice(state.demo ? "DEMO MODE — synthetic prices for a fictional instrument. No live data or trained-model predictions." : "Forecast ready. " + freshness(state.result.features_timestamp) + "." + historyWarning, state.demo ? "" : "success");
  } catch (error) {
    if (generation !== state.generation || error.name === "AbortError") return;
    clearForecast(); notice(errorMessage(error), "error");
  } finally { if (generation === state.generation) { state.busy = false; controls(); } }
}

async function opsAction(id, path, options = {}) {
  const generation = state.generation;
  try {
    const result = await api.request(id, path, { ...options, key: state.operator });
    if (generation !== state.generation) return null;
    return result;
  } catch (error) {
    if (generation !== state.generation || error.name === "AbortError") return null;
    if ([401, 403].includes(error.status)) lockOps();
    notice(errorMessage(error), "error"); return null;
  }
}
async function refreshModel() {
  const model = await opsAction("model", "/v1/model");
  if (model) $("model-data").textContent = JSON.stringify(model, null, 2);
}
$("ops-login").onsubmit = async event => {
  event.preventDefault();
  if (!state.metadata) { notice("Connect to the API before unlocking operations.", "error"); return; }
  state.operator = $("operator-key").value; $("operator-key").value = "";
  const session = await opsAction("session", "/v1/ops/session");
  if (!session || session.role !== "operator") { lockOps(); return; }
  state.capabilities = session; $("ops-lock").hidden = true; $("ops-content").hidden = false;
  $("reload").disabled = !session.model_reload_enabled;
  $("features").disabled = !session.feature_debug_enabled;
  notice("Operator session unlocked. Disabled actions are not enabled by the serving configuration.", "success");
  await refreshModel();
};
$("lock").onclick = () => { state.generation++; api.cancelAll(); lockOps(); notice("Console locked. Operator credential cleared."); };
$("model-refresh").onclick = refreshModel;
$("features").onclick = async () => {
  const data = await opsAction("debug", "/v1/features/debug?symbol=" + encodeURIComponent($("ops-symbol").value) + "&limit=10");
  if (data) $("feature-data").textContent = JSON.stringify(data, null, 2);
};
$("reload").onclick = () => $("reload-dialog").showModal();
$("cancel-reload").onclick = () => $("reload-dialog").close();
$("confirm-reload").onclick = async () => {
  $("reload-dialog").close(); $("reload").disabled = true;
  notice("Reloading the registry champion…");
  const result = await opsAction("reload", "/v1/model/reload", { method: "POST" });
  if (result) { notice("Champion reloaded. Active version: " + result.model_version, "success"); await refreshModel(); }
  $("reload").disabled = !state.capabilities?.model_reload_enabled;
};
$("settings-open").onclick = () => { $("api-base").setCustomValidity(""); $("api-base").value = api.base; $("api-key").value = api.key; $("settings").showModal(); };
$("settings").onclose = () => { $("api-key").value = ""; };
$("settings-close").onclick = () => { $("api-key").value = ""; $("settings").close(); };
$("connection-form").onsubmit = event => {
  event.preventDefault();
  try { const base = validateBaseUrl($("api-base").value); api.base = base; api.key = $("api-key").value; api.header = "X-API-Key"; $("api-key").value = ""; $("settings").close(); connect(); }
  catch (error) { $("api-base").setCustomValidity(error.message); $("api-base").reportValidity(); }
};
$("api-base").oninput = () => $("api-base").setCustomValidity("");
$("retry").onclick = connect;
$("run").onclick = runForecast;
$("aggregation").onchange = () => { renderRows(); state.select?.(0); };
["symbol", "horizon"].forEach(id => $(id).onchange = () => { invalidate(); notice("Selection changed. Generate a new forecast to view matching results."); });
$("demo").onclick = () => {
  if (state.demo) { connect(); return; }
  invalidate(); lockOps(); state.demo = true; state.ready = true;
  configure(demoMetadata); $("health").textContent = "Synthetic demo"; runForecast();
};
if (isOps) {
  $("product").hidden = true; $("ops").hidden = false;
  $("eyebrow").textContent = "INTERNAL OPERATIONS";
  $("page-title").textContent = "Inspect. Diagnose. Operate.";
  $("page-description").textContent = "A restricted console for the serving platform.";
  $("ops-link").setAttribute("aria-current", "page");
  document.title = "PolyHorizon — Operations";
} else $("product-link").setAttribute("aria-current", "page");
window.addEventListener("resize", () => {
  if (state.result) state.select = drawChart($("chart"), state.result, state.history, state.metadata, inspect);
});
connect();
