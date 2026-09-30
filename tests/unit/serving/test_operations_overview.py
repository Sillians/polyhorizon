from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from fastapi.testclient import TestClient

from polyhorizon.serving.app.main import create_app
from polyhorizon.serving.api.routers.operations import _online_evidence, _symbol_evidence


CONFIG = Path(__file__).parent / "serving_test_config.yaml"


def test_overview_requires_operator_and_reports_real_evidence(monkeypatch):
    app = create_app(str(CONFIG), init_container=False)
    config = app.state.config
    config.security.operator_api_keys = ["operator"]
    now = datetime.now(timezone.utc)
    app.state.container = SimpleNamespace(
        config=config,
        model_handle=SimpleNamespace(model_version="2", loaded_at=now),
        prepared_handle=None,
        model_registry=SimpleNamespace(
            get_champion_version=lambda: "2",
            client=SimpleNamespace(get_model_version=lambda *_: SimpleNamespace(
                tags={"governance_qualification": "passed-v1"}
            )),
        ),
    )
    monkeypatch.setattr("polyhorizon.serving.api.routers.operations._publication",
                        lambda _container: {"status": "complete", "recent": [], "pending_count": 0})
    monkeypatch.setattr("polyhorizon.serving.api.routers.operations._symbol_evidence",
                        lambda _container, symbol, _session: {"symbol": symbol, "status": "current"})
    monkeypatch.setattr("polyhorizon.serving.api.routers.operations._online_evidence",
                        lambda _container, symbol: {"symbol": symbol, "status": "matching_latest_close"})
    monkeypatch.setattr("polyhorizon.serving.api.routers.operations._active_alerts",
                        lambda: {"status": "available", "count": 0, "items": []})
    with TestClient(app) as client:
        assert client.get("/v1/ops/overview").status_code == 403
        assert client.get("/v1/ops/overview", headers={"X-API-Key": "consumer"}).status_code == 403
        response = client.get("/v1/ops/overview", headers={"X-API-Key": "operator"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["forecast_gate"]["status"] == "eligible"
    assert payload["model"]["qualification"] == "passed-v1"
    assert payload["outcomes"]["status"] == "not_measured"
    assert len(payload["symbols"]) == len(config.client_metadata.supported_symbols)


def test_overview_detects_missing_session_bar_and_online_close_mismatch(monkeypatch):
    session = SimpleNamespace(market_open=pd.Timestamp("2026-09-23T13:30:00Z"),
                              market_close=pd.Timestamp("2026-09-23T20:00:00Z"))
    expected = pd.date_range(session.market_open + pd.Timedelta(minutes=30),
                             session.market_close, freq="30min")
    history = pd.DataFrame({"event_timestamp": expected.delete(4), "close": 100.0})
    store = SimpleNamespace(get_offline_history=lambda *_: history,
                            get_online_features=lambda *_: pd.DataFrame({"close": [101.0]}))
    config = SimpleNamespace(offline_store=SimpleNamespace(history_rows=780),
                             inference=SimpleNamespace(time_field="event_timestamp", max_encoder_length=8),
                             publication=SimpleNamespace(publication_delay_minutes=60))
    container = SimpleNamespace(config=config, feature_store=store)
    monkeypatch.setattr("polyhorizon.serving.api.routers.operations.require_post_close_features",
                        lambda *_args: None)
    offline = _symbol_evidence(container, "NVDA", session)
    assert offline["status"] == "incomplete"
    assert offline["observed_bars"] == 12
    assert offline["missing_bars"] == [expected[4].isoformat()]
    assert _online_evidence(container, "NVDA")["status"] == "close_mismatch"
