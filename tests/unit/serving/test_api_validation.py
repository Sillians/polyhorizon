from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import uuid

from fastapi.testclient import TestClient

from polyhorizon.serving.app.dependencies import get_forecast_service
from polyhorizon.serving.app.main import create_app
from polyhorizon.serving.services.forecast_service import ForecastResult


class StubForecastService:
    def predict(self, symbol: str, horizon: int | None = None, use_cache: bool = True) -> ForecastResult:
        now = datetime.now(timezone.utc).isoformat()
        return ForecastResult(
            symbol=symbol,
            horizon=horizon or 3,
            quantiles=[0.1, 0.5, 0.9],
            predictions=[
                {"step": 1, "timestamp": now, "p10": 1.0, "p50": 1.1, "p90": 1.2},
                {"step": 2, "timestamp": now, "p10": 1.1, "p50": 1.2, "p90": 1.3},
                {"step": 3, "timestamp": now, "p10": 1.2, "p50": 1.3, "p90": 1.4},
            ],
            base_price=1.0,
            absolute_change=0.3,
            percent_change=0.3,
            model_version="1",
            model_uri="models:/polyhorizon-test@champion",
            features_timestamp=now,
            cached=False,
        )


def _make_client() -> TestClient:
    config_path = Path(__file__).parent / "serving_test_config.yaml"
    app = create_app(config_path=str(config_path), init_container=False)
    app.dependency_overrides[get_forecast_service] = lambda: StubForecastService()
    return TestClient(app)


def test_forecast_validation_missing_symbol():
    client = _make_client()
    response = client.post("/v1/forecast", json={"horizon": 3})
    assert response.status_code == 422


def test_forecast_validation_invalid_horizon():
    client = _make_client()
    response = client.post("/v1/forecast", json={"symbol": "NVDA", "horizon": 0})
    assert response.status_code == 422


def test_forecast_endpoint_success():
    client = _make_client()
    response = client.post("/v1/forecast", json={"symbol": "NVDA", "horizon": 3})
    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == "NVDA"
    assert payload["horizon"] == 3
    assert len(payload["predictions"]) == 3
    assert payload["predictions"][0]["timestamp"]


def test_openapi_includes_example():
    client = _make_client()
    response = client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    body_schema = (
        schema["paths"]["/v1/forecast"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    )
    component_name = body_schema["$ref"].rsplit("/", 1)[-1]
    request_schema = schema["components"]["schemas"][component_name]
    assert request_schema["example"]["symbol"] == "NVDA"


def test_liveness_does_not_require_initialized_container():
    client = _make_client()
    response = client.get("/v1/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_request_id_is_generated_for_correlation():
    client = _make_client()
    response = client.get("/v1/health/live")
    assert uuid.UUID(response.headers["X-Request-ID"])


def test_supplied_request_id_is_propagated():
    client = _make_client()
    response = client.get("/v1/health/live", headers={"X-Request-ID": "trace-123"})
    assert response.headers["X-Request-ID"] == "trace-123"


def test_readiness_fails_without_initialized_container():
    client = _make_client()
    response = client.get("/v1/health/ready")
    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "reason": "model_not_loaded",
    }


def test_readiness_requires_loaded_version_to_match_champion():
    client = _make_client()
    client.app.state.container = SimpleNamespace(
        model_handle=SimpleNamespace(model=object(), model_version="6"),
        model_registry=SimpleNamespace(get_champion_version=lambda: "7"),
    )

    response = client.get("/v1/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "reason": "champion_not_active",
        "model_version": "6",
        "champion_version": "7",
    }


def test_readiness_succeeds_only_for_active_champion():
    client = _make_client()
    client.app.state.container = SimpleNamespace(
        model_handle=SimpleNamespace(model=object(), model_version="7"),
        model_registry=SimpleNamespace(get_champion_version=lambda: "7"),
    )

    response = client.get("/v1/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "model_version": "7",
        "champion_version": "7",
    }


def test_client_metadata_exposes_frontend_contract():
    client = _make_client()
    response = client.get("/v1/metadata")
    assert response.status_code == 200
    payload = response.json()
    assert payload["supported_symbols"] == ["NVDA", "AAPL", "MSFT"]
    assert payload["bars_per_day"] == 1
    assert payload["horizon_days"] == [1, 2, 3]
    assert payload["max_prediction_length"] == 3
    assert payload["api_key_header"] == "X-API-Key"
    assert payload["feature_debug_enabled"] is False
    assert payload["model_reload_enabled"] is False


def test_stale_features_exposes_stable_frontend_error_code():
    from polyhorizon.serving.services.freshness import StaleFeaturesError

    class StaleService:
        def predict(self, **kwargs):
            raise StaleFeaturesError("Feature cutoff is stale")

    client = _make_client()
    client.app.dependency_overrides[get_forecast_service] = lambda: StaleService()
    response = client.post("/v1/forecast", json={"symbol": "NVDA", "horizon": 3})
    assert response.status_code == 503
    assert response.json()["code"] == "stale_features"
    assert response.headers["Retry-After"] == "300"
