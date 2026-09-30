from pathlib import Path

import pytest

from polyhorizon.serving.configs.settings import Config, SecurityConfig, load_config


def test_privileged_frontend_capabilities_require_authentication():
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    values = config.model_dump()
    values["client_metadata"]["model_reload_enabled"] = True

    with pytest.raises(ValueError, match="require API-key authentication"):
        Config.model_validate(values)


def test_operator_keys_must_not_overlap_consumer_keys():
    with pytest.raises(ValueError, match="separate"):
        SecurityConfig(api_keys=["same"], operator_api_keys=["same"])


def test_privileged_capabilities_require_operator_credentials():
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    values = config.model_dump()
    values["security"]["require_api_key"] = True
    values["client_metadata"]["model_reload_enabled"] = True
    with pytest.raises(ValueError, match="operator_api_keys"):
        Config.model_validate(values)
