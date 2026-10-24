"""Read-only fleet/forecast readiness proof; exits nonzero on incomplete evidence."""
import argparse
import json
import os
import math
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def inspect_replica(base, forecast_key, operator_key, session=None):
    parsed = urlsplit(base)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Replica URL must not contain credentials or query parameters")
    if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}):
        raise ValueError("Use HTTPS for non-loopback serving endpoints")
    session = session or requests.Session()
    result = {"url": base, "passed": False, "checks": {}}

    def get(path, key="", header="X-API-Key"):
        response = session.get(base.rstrip("/") + path, headers={header: key} if key else {}, timeout=15, allow_redirects=False)
        if response.status_code != 200:
            raise RuntimeError(f"{path}: HTTP {response.status_code}")
        return response.json()

    try:
        metadata = get("/v1/metadata")
        header = metadata.get("api_key_header", "X-API-Key")
        ready = get("/v1/health/ready")
        if (ready.get("status") != "ready" or ready.get("transitioning") or
                not ready.get("model_version") or ready.get("model_version") != ready.get("champion_version")):
            raise RuntimeError("Champion is not stably active")
        result["checks"]["readiness"] = ready
        if not operator_key:
            raise RuntimeError("READINESS_OPERATOR_KEY is required for evidence inspection")
        overview = get("/v1/ops/overview", operator_key, header)
        result["checks"]["operations"] = overview
        if overview["forecast_gate"]["status"] != "eligible":
            raise RuntimeError("Operations preflight is blocked")
        forecasts = []
        for symbol in PRODUCT_SYMBOLS:
            horizon = min(13, metadata["max_prediction_length"])
            response = session.post(base.rstrip("/") + "/v1/forecast",
                headers={header: forecast_key} if forecast_key else {},
                json={"symbol": symbol, "horizon": horizon, "use_cache": False},
                timeout=max(30, metadata.get("request_timeout_seconds", 30)), allow_redirects=False)
            if response.status_code != 200:
                raise RuntimeError(f"{symbol} forecast: HTTP {response.status_code}")
            payload = response.json()
            if payload["model_version"] != ready["champion_version"] or payload["symbol"] != symbol:
                raise RuntimeError("Forecast provenance differs from loaded champion")
            if (payload.get("cached", False) or len(payload.get("predictions", [])) != horizon or
                    not all(all(math.isfinite(p[key]) for key in ("p10", "p50", "p90")) and
                            p["p10"] <= p["p50"] <= p["p90"] for p in payload["predictions"])):
                raise RuntimeError("Forecast quantiles are empty or unordered")
            forecasts.append({"symbol": symbol, "model_version": payload["model_version"],
                              "features_timestamp": payload["features_timestamp"], "rows": len(payload["predictions"])})
        result["checks"]["forecasts"] = forecasts
        # Recheck after inference to catch an alias move during the probes.
        after = get("/v1/health/ready")
        if after != ready:
            raise RuntimeError("Champion changed during verification")
        result["passed"] = True
    except (requests.RequestException, RuntimeError, KeyError, ValueError, TypeError):
        # Never print request objects, tokens, or credential-bearing transport exceptions.
        result["failure"] = "Serving readiness evidence incomplete; inspect recorded checks and service logs"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replica", action="append", required=True, help="Repeat for EVERY serving replica, not just the load balancer")
    args = parser.parse_args()
    results = [inspect_replica(base, os.getenv("READINESS_FORECAST_KEY", ""), os.getenv("READINESS_OPERATOR_KEY", "")) for base in args.replica]
    versions = {r["checks"].get("readiness", {}).get("model_version") for r in results}
    passed = all(r["passed"] for r in results) and len(versions) == 1
    print(json.dumps({"checked_at": datetime.now(timezone.utc).isoformat(), "serving_ready": passed,
        "scope": "listed replicas, latest-close parity, governance, and uncached HTTP forecasts only",
        "production_operational_readiness": "requires separate real backup restore, alert receiver receipt, immutable release and frontend evidence",
        "replicas": results}, indent=2))
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
