from pathlib import Path
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

from polyhorizon.serving.app.main import create_app
from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services.security import SecurityManager

CONFIG = Path(__file__).parent / "serving_test_config.yaml"


@pytest.mark.parametrize("path", ["/v1/model/reload", "/v1/features/debug", "/v1/ops/session", "/v1/ops/overview", "/v1/model/reload/"])
@pytest.mark.parametrize("key", ["", "consumer"])
def test_operator_routes_reject_public_and_consumer_keys_even_when_exempt(path, key):
    config = load_config(CONFIG)
    config.security.api_keys = ["consumer"]
    config.security.operator_api_keys = ["operator"]
    config.security.exempt_paths.append(path.rstrip("/"))
    request = Request({"type": "http", "path": path, "headers": [(b"x-api-key", key.encode())]})
    decision = SecurityManager(config).authorize(request)
    assert not decision.allowed
    assert decision.status_code == 403


def test_operator_session_works_without_model_container_and_enforces_auth():
    app = create_app(str(CONFIG), init_container=False)
    app.state.config.security.operator_api_keys = ["operator"]
    with TestClient(app) as client:
        assert client.get("/v1/ops/session").status_code == 403
        assert client.get("/v1/ops/session", headers={"X-API-Key": "consumer"}).status_code == 403
        response = client.get("/v1/ops/session", headers={"X-API-Key": "operator"})
        assert response.status_code == 200
        assert response.json() == {"role": "operator", "feature_debug_enabled": False,
                                   "model_reload_enabled": False, "grafana_url": "",
                                   "prefect_url": "", "mlflow_url": ""}


def test_operator_auth_uses_configured_header_and_api_prefix():
    config = load_config(CONFIG)
    config.api.version_prefix = "/api/v2"
    config.security.require_api_key = True
    config.security.api_key_header = "X-Operator-Test"
    config.security.operator_api_keys = ["operator"]
    request = Request({"type": "http", "path": "/api/v2/model/reload", "headers": [(b"x-operator-test", b"operator")]})
    assert SecurityManager(config).authorize(request).allowed


def test_only_operator_can_invoke_enabled_reload():
    app = create_app(str(CONFIG), init_container=False)
    config = app.state.config
    config.security.require_api_key = True
    config.security.api_keys = ["consumer"]
    config.security.operator_api_keys = ["operator"]
    config.client_metadata.model_reload_enabled = True
    calls = []

    def reload():
        calls.append(True)
        return SimpleNamespace(
            model_version="2", model_uri="models:/test@champion",
            loaded_at=datetime.now(timezone.utc),
        )

    app.state.container = SimpleNamespace(config=config, reload_champion=reload)
    with TestClient(app) as client:
        assert client.post("/v1/model/reload", headers={"X-API-Key": "consumer"}).status_code == 403
        assert calls == []
        assert client.post("/v1/model/reload", headers={"X-API-Key": "operator"}).status_code == 200
        assert calls == [True]
        assert client.get("/v1/features/debug?symbol=NVDA", headers={"X-API-Key": "operator"}).status_code == 404


def test_cors_wraps_operator_auth_for_preflight_and_denied_response():
    app = create_app(str(CONFIG), init_container=False)
    with TestClient(app) as client:
        preflight = client.options("/v1/ops/session", headers={
            "Origin": "https://frontend.example",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "X-API-Key",
        })
        assert preflight.status_code == 200
        denied = client.get("/v1/ops/session", headers={"Origin": "https://frontend.example"})
        assert denied.status_code == 403
        assert "access-control-allow-origin" in denied.headers
