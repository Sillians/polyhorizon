from __future__ import annotations

import io
import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from polyhorizon.training.tasks import model_activation_tasks


def test_activation_skips_when_governance_does_not_approve():
    result = model_activation_tasks.activate_champion_task.fn(
        {"approved": False},
        {"model_version": "9"},
    )

    assert result == {"status": "skipped", "reason": "not_approved"}


def test_activation_rejects_missing_session_lineage(monkeypatch):
    monkeypatch.setenv("SERVING_ACTIVATION_MODE", "reload")
    monkeypatch.setenv("SERVING_RELOAD_API_KEY", "test-secret")
    monkeypatch.setattr(model_activation_tasks, "_replica_urls", lambda _: ["http://replica:8000"])
    client = Mock()
    client.get_model_version.return_value = SimpleNamespace(tags={"governance_qualification": "passed-v1"})
    monkeypatch.setattr("mlflow.tracking.MlflowClient", lambda **_: client)
    config = SimpleNamespace(mlflow=SimpleNamespace(tracking_uri="file:///tmp/test"),
                             model_registry=SimpleNamespace(name="test", champion_alias="champion"))
    with pytest.raises(ValueError, match="session"):
        model_activation_tasks.activate_champion_task.fn({"approved": True}, {"model_version": "9"}, config)
    client.set_registered_model_alias.assert_not_called()


def test_activation_reloads_and_verifies_every_resolved_replica(monkeypatch):
    monkeypatch.setenv("SERVING_ACTIVATION_MODE", "reload")
    monkeypatch.setenv(
        "SERVING_RELOAD_URL",
        "http://model-serving:8000/v1/model/reload",
    )
    monkeypatch.setenv("SERVING_RELOAD_API_KEY", "test-secret")
    monkeypatch.setattr(
        model_activation_tasks.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (None, None, None, None, ("10.0.0.3", 8000)),
            (None, None, None, None, ("10.0.0.2", 8000)),
        ],
    )
    client = Mock()
    client.get_model_version.return_value = SimpleNamespace(tags={"governance_qualification": "passed-v1", "session_qualification": "nyse-full-session-v1"})
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="8")
    events = []
    client.set_registered_model_alias.side_effect = lambda **kwargs: events.append(("alias", kwargs["version"]))
    monkeypatch.setattr("mlflow.tracking.MlflowClient", lambda **_: client)

    def fake_urlopen(request, timeout):
        if isinstance(request, str):
            return io.BytesIO(json.dumps({"model_version": "9"}).encode())
        events.append((request.full_url, request.get_method(), timeout))
        return io.BytesIO(json.dumps({"model_version": "9"}).encode())

    monkeypatch.setattr(model_activation_tasks, "urlopen", fake_urlopen)
    config = SimpleNamespace(mlflow=SimpleNamespace(tracking_uri="file:///tmp/test"),
                             model_registry=SimpleNamespace(name="test", champion_alias="champion"))

    result = model_activation_tasks.activate_champion_task.fn(
        {"approved": True},
        {"model_version": "9"},
        config,
    )

    assert result == {
        "status": "activated",
        "model_version": "9",
        "replicas": [
            {"address": "http://10.0.0.2:8000"},
            {"address": "http://10.0.0.3:8000"},
        ],
    }
    assert events == [
        ("http://10.0.0.2:8000/v1/model/prepare", "POST", 120),
        ("http://10.0.0.3:8000/v1/model/prepare", "POST", 120),
        ("alias", "9"),
        ("http://10.0.0.2:8000/v1/model/activate", "POST", 120),
        ("http://10.0.0.3:8000/v1/model/activate", "POST", 120),
    ]


def test_prepare_failure_keeps_previous_champion(monkeypatch):
    monkeypatch.setenv("SERVING_ACTIVATION_MODE", "reload")
    monkeypatch.setenv("SERVING_RELOAD_API_KEY", "test-secret")
    monkeypatch.setattr(model_activation_tasks, "_replica_urls", lambda _: ["http://replica:8000"])
    client = Mock()
    client.get_model_version.return_value = SimpleNamespace(tags={"governance_qualification": "passed-v1", "session_qualification": "nyse-full-session-v1"})
    client.get_model_version_by_alias.return_value = SimpleNamespace(version="8")
    monkeypatch.setattr("mlflow.tracking.MlflowClient", lambda **_: client)
    monkeypatch.setattr(model_activation_tasks, "_post",
                        lambda *_args: (_ for _ in ()).throw(RuntimeError("load failed")))
    config = SimpleNamespace(mlflow=SimpleNamespace(tracking_uri="file:///tmp/test"),
                             model_registry=SimpleNamespace(name="test", champion_alias="champion"))
    with pytest.raises(RuntimeError, match="load failed"):
        model_activation_tasks.activate_champion_task.fn({"approved": True},
                                                         {"model_version": "9"}, config)
    client.set_registered_model_alias.assert_not_called()
