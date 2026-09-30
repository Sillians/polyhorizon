"""Read-only, operator-scoped evidence for the operations workspace."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from urllib.parse import urlsplit
from urllib.request import urlopen

import pandas as pd
import pandas_market_calendars as mcal
import psycopg2
from fastapi import APIRouter, Depends
from psycopg2 import sql

from polyhorizon.serving.app.dependencies import ServingContainer, get_container
from polyhorizon.serving.services.freshness import StaleFeaturesError, require_post_close_features
from polyhorizon.serving.services.metrics import REQUEST_COUNT


router = APIRouter()


def _latest_due_session(now: pd.Timestamp, delay_minutes: int):
    schedule = mcal.get_calendar("NYSE").schedule(
        start_date=(now - pd.Timedelta(days=14)).date(), end_date=now.date()
    )
    due = schedule[schedule.market_close + pd.Timedelta(minutes=delay_minutes) <= now]
    if due.empty:
        return None
    return due.iloc[-1]


def _publication(container: ServingContainer) -> dict:
    config = container.config
    params = config.connection_parameters.model_dump(exclude_none=True)
    schema = config.offline_store.db_schema
    try:
        connection = psycopg2.connect(**params)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT dataset_id, status, max_event_time, updated_at "
                            "FROM {}.dataset_publications ORDER BY updated_at DESC LIMIT 5")
                    .format(sql.Identifier(schema))
                )
                rows = cursor.fetchall()
        finally:
            connection.close()
    except (psycopg2.Error, ValueError):
        return {"status": "unavailable", "reason": "Publication ledger could not be read", "recent": []}
    recent = [{"dataset_id": row[0], "status": row[1],
               "max_event_time": row[2].isoformat(), "updated_at": row[3].isoformat()}
              for row in rows]
    if not recent:
        return {"status": "missing", "reason": "No published dataset", "recent": []}
    return {"status": recent[0]["status"], "recent": recent,
            "pending_count": sum(row["status"] != "complete" for row in recent)}


def _symbol_evidence(container: ServingContainer, symbol: str, session) -> dict:
    try:
        history = container.feature_store.get_offline_history(
            symbol, container.config.offline_store.history_rows
        )
        if history.empty:
            return {"symbol": symbol, "status": "missing", "reason": "No published offline history"}
        field = container.config.inference.time_field
        timestamps = pd.to_datetime(history[field], utc=True)
        latest = timestamps.max()
        try:
            require_post_close_features(latest, container.config.publication.publication_delay_minutes)
            freshness = "current"
        except StaleFeaturesError:
            freshness = "stale"
        evidence = {"symbol": symbol, "status": freshness, "latest_bar": latest.isoformat(),
                    "history_rows": len(history), "required_encoder_rows": container.config.inference.max_encoder_length}
        if session is not None:
            expected = pd.date_range(session.market_open + pd.Timedelta(minutes=30),
                                     session.market_close, freq="30min")
            observed = set(timestamps)
            missing = [stamp.isoformat() for stamp in expected if stamp not in observed]
            evidence.update({"session_close": session.market_close.isoformat(),
                             "expected_bars": len(expected), "observed_bars": len(expected) - len(missing),
                             "missing_bars": missing})
            if missing:
                evidence["status"] = "incomplete"
        if len(history) < container.config.inference.max_encoder_length:
            evidence["status"] = "insufficient_history"
        return evidence
    except Exception:
        return {"symbol": symbol, "status": "unavailable", "reason": "Offline history could not be read"}


def _online_evidence(container: ServingContainer, symbol: str) -> dict:
    try:
        online = container.feature_store.get_online_features([symbol])
        if online.empty:
            return {"symbol": symbol, "status": "missing"}
        offline = container.feature_store.get_offline_history(symbol, 1)
        if offline.empty or "close" not in online.columns or "close" not in offline.columns:
            return {"symbol": symbol, "status": "not_comparable"}
        online_close = float(online.iloc[-1]["close"])
        offline_close = float(offline.iloc[-1]["close"])
        if not pd.notna(online_close) or not pd.notna(offline_close):
            return {"symbol": symbol, "status": "not_comparable"}
        # This compares the latest close only. It is not full feature parity.
        matches = abs(online_close - offline_close) <= max(1e-4, abs(offline_close) * 1e-5)
        return {"symbol": symbol, "status": "matching_latest_close" if matches else "close_mismatch",
                "online_close": online_close, "offline_close": offline_close}
    except Exception:
        return {"symbol": symbol, "status": "unavailable"}


def _serving_requests() -> dict:
    counts: dict[str, int] = {}
    for family in REQUEST_COUNT.collect():
        for sample in family.samples:
            if sample.name == "serving_requests_total" and sample.labels.get("endpoint") == "/v1/forecast":
                code = sample.labels.get("status", "unknown")
                counts[code] = counts.get(code, 0) + int(sample.value)
    return {"scope": "this serving process since startup", "forecast_status_counts": counts}


def _active_alerts() -> dict:
    base = os.getenv("SERVING_ALERTMANAGER_URL", "http://alertmanager:9093")
    parsed = urlsplit(base)
    if parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password:
        return {"status": "unavailable", "reason": "Alertmanager URL is invalid"}
    try:
        with urlopen(base.rstrip("/") + "/api/v2/alerts?active=true&silenced=false&inhibited=false",
                     timeout=2) as response:
            alerts = json.load(response)
        if not isinstance(alerts, list):
            raise ValueError("Unexpected alert response")
        return {"status": "available", "count": len(alerts),
                "items": [{"name": str(item.get("labels", {}).get("alertname", "Unnamed")),
                           "severity": str(item.get("labels", {}).get("severity", "unknown"))}
                          for item in alerts[:5]]}
    except Exception:
        return {"status": "unavailable", "reason": "Alertmanager could not be reached"}


@router.get("/ops/overview")
def operations_overview(container: ServingContainer = Depends(get_container)) -> dict:
    """Summarize recorded evidence; never infer quality or fleet-wide state."""
    now = pd.Timestamp.now(tz="UTC")
    session = _latest_due_session(now, container.config.publication.publication_delay_minutes)
    publication = _publication(container)
    symbols = [_symbol_evidence(container, symbol, session)
               for symbol in container.config.client_metadata.supported_symbols]
    online = [_online_evidence(container, symbol)
              for symbol in container.config.client_metadata.supported_symbols]
    loaded = container.model_handle
    try:
        champion = container.model_registry.get_champion_version()
        version = container.model_registry.client.get_model_version(
            container.config.model_registry.name, champion
        )
        qualification = (version.tags or {}).get("governance_qualification", "unknown")
    except Exception:
        champion, qualification = None, "unavailable"
    model = {"loaded_version": str(loaded.model_version), "champion_version": champion,
             "qualification": qualification, "loaded_at": loaded.loaded_at.isoformat(),
             "prepared_version": getattr(getattr(container, "prepared_handle", None), "model_version", None),
             "scope": "this serving process"}
    blockers = []
    if publication["status"] != "complete":
        blockers.append("Dataset publication is not complete")
    if champion != str(loaded.model_version):
        blockers.append("Loaded model differs from registry champion")
    if qualification != "passed-v1":
        blockers.append("Champion governance qualification is unavailable or not passed")
    blockers.extend(f"{item['symbol']}: {item['status']}" for item in symbols if item["status"] != "current")
    blockers.extend(f"{item['symbol']} online: {item['status']}" for item in online
                    if item["status"] != "matching_latest_close")
    return {"checked_at": datetime.now(timezone.utc).isoformat(),
            "forecast_gate": {"status": "blocked" if blockers else "eligible", "reasons": blockers,
                              "note": "Eligibility is a preflight, not a successful forecast probe."},
            "publication": publication, "symbols": symbols, "online": online, "model": model,
            "requests": _serving_requests(), "alerts": _active_alerts(),
            "outcomes": {"status": "not_measured",
                         "reason": "Forecasts and realized prices are not joined in a durable outcomes store."},
            "fleet": {"status": "not_measured",
                      "reason": "This API has no replica inventory or aggregate activation feed."}}
