export function validateBaseUrl(value) {
  const url = new URL(value);
  if (url.username || url.password || url.search || url.hash) throw new Error("Use a base URL without credentials, query parameters, or fragments.");
  const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && local)) throw new Error("Use HTTPS for remote APIs; HTTP is allowed only on localhost.");
  return url.href.replace(/\/$/, "");
}

export function errorMessage(error) {
  if (error.code === "stale_features") return "Forecast withheld: feature data is stale, missing, or ahead of the expected market cutoff. Refresh the ingestion/feature pipeline before retrying.";
  if (error.name === "TypeError") return "Unable to reach the API. Check the connection URL, network, and CORS configuration.";
  if (error.name === "SyntaxError") return "The API returned an unreadable response. Check the serving URL and proxy configuration.";
  if (error.name === "AbortError") return "Request cancelled.";
  if (error.name === "TimeoutError") return "The request timed out. Check the service and retry.";
  const messages = {
    400: "No forecast could be generated for this request. Check the horizon and whether enough valid feature history is available.",
    401: "Authentication required. Add a valid forecast API key in Connection settings.",
    403: "Access denied. Operations require a separate operator credential.",
    404: "Requested data or capability is unavailable. Check data ingestion and server configuration.",
    422: "The request could not be fulfilled. Check the instrument, horizon, and available feature history.",
    429: "Too many requests. Please wait before trying again.",
    503: "The serving API is not ready. Check the model and feature-store services, then reconnect.",
  };
  return messages[error.status] || (error.status >= 500 ? "The API is unavailable or encountered an error. Check the serving service and reconnect." : error.message || "Unable to reach the API. Check your connection.");
}

export class ApiClient {
  constructor(base, fetcher = globalThis.fetch) {
    this.base = validateBaseUrl(base);
    this.fetcher = (...args) => fetcher(...args);
    this.key = "";
    this.header = "X-API-Key";
    this.timeout = 30000;
    this.pending = new Map();
  }
  cancelAll() {
    for (const controller of this.pending.values()) controller.abort();
    this.pending.clear();
  }
  async request(id, path, { method = "GET", body, key = this.key } = {}) {
    this.pending.get(id)?.abort();
    const controller = new AbortController();
    this.pending.set(id, controller);
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, this.timeout);
    try {
      const headers = { Accept: "application/json" };
      if (key) headers[this.header] = key;
      if (body) headers["Content-Type"] = "application/json";
      const response = await this.fetcher(this.base + path, { method, headers, body: body ? JSON.stringify(body) : undefined, signal: controller.signal, redirect: "error" });
      if (!response.ok) {
        const error = new Error("API request failed");
        error.status = response.status;
        try { error.code = (await response.json()).code; } catch { /* Proxies may return non-JSON errors. */ }
        throw error;
      }
      return await response.json();
    } catch (error) {
      if (timedOut) throw new DOMException("Request timed out", "TimeoutError");
      throw error;
    } finally {
      clearTimeout(timer);
      if (this.pending.get(id) === controller) this.pending.delete(id);
    }
  }
}
