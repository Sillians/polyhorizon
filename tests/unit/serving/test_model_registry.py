from types import SimpleNamespace
from unittest.mock import patch

import pytest

from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services.model_registry import ModelRegistryClient


@pytest.fixture
def config():
    return load_config("tests/unit/serving/serving_test_config.yaml")


def _registry(config):
    with patch("polyhorizon.serving.services.model_registry.MlflowClient") as client_cls:
        registry = ModelRegistryClient(config)
    registry.client = client_cls.return_value
    return registry


def test_existing_champion_is_never_reassigned(config):
    registry = _registry(config)
    registry.client.get_model_version_by_alias.return_value = SimpleNamespace(version="2")

    assert registry.ensure_champion_alias() == "2"
    registry.client.search_model_versions.assert_not_called()
    registry.client.set_registered_model_alias.assert_not_called()


def test_missing_champion_bootstraps_latest_ready_version(config):
    registry = _registry(config)
    registry.client.get_model_version_by_alias.side_effect = RuntimeError("missing")
    registry.client.search_model_versions.return_value = [
        SimpleNamespace(version="1", status="READY", creation_timestamp=100),
        SimpleNamespace(version="3", status="FAILED_REGISTRATION", creation_timestamp=300),
        SimpleNamespace(version="2", status="READY", creation_timestamp=200,
                        tags={"governance_qualification": "passed-v1", "session_qualification": "nyse-full-session-v1"}),
        SimpleNamespace(version="4", status="READY", creation_timestamp=400),
    ]

    assert registry.ensure_champion_alias() == "2"
    registry.client.set_registered_model_alias.assert_called_once_with(
        name="polyhorizon-test", alias="champion", version="2"
    )
    registry.client.set_model_version_tag.assert_called_once_with(
        name="polyhorizon-test",
        version="2",
        key="champion_bootstrapped",
        value="true",
    )


def test_bootstrap_requires_a_ready_version(config):
    registry = _registry(config)
    registry.client.get_model_version_by_alias.side_effect = RuntimeError("missing")
    registry.client.search_model_versions.return_value = [
        SimpleNamespace(version="1", status="FAILED_REGISTRATION", creation_timestamp=100)
    ]

    with pytest.raises(RuntimeError, match="no governance-qualified READY versions"):
        registry.ensure_champion_alias()


def test_unqualified_ready_model_cannot_bootstrap(config):
    registry = _registry(config)
    registry.client.get_model_version_by_alias.side_effect = RuntimeError("missing")
    registry.client.search_model_versions.return_value = [
        SimpleNamespace(version="1", status="READY", tags={})
    ]
    with pytest.raises(RuntimeError, match="governance-qualified"):
        registry.ensure_champion_alias()
    registry.client.set_registered_model_alias.assert_not_called()


def test_governance_alone_cannot_bootstrap_without_session(config):
    registry = _registry(config)
    registry.client.get_model_version_by_alias.side_effect = RuntimeError("missing")
    registry.client.search_model_versions.return_value = [
        SimpleNamespace(version="1", status="READY", tags={"governance_qualification": "passed-v1"})
    ]
    with pytest.raises(RuntimeError, match="governance-qualified"):
        registry.ensure_champion_alias()
    registry.client.set_registered_model_alias.assert_not_called()
