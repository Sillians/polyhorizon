from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest
import torch

from polyhorizon.core.target_contract import attach_target_contract, validate_target_contract
from polyhorizon.serving.configs.settings import ForecastConfig, load_config
from polyhorizon.serving.services.forecast_service import ForecastService
from polyhorizon.serving.services.model_registry import ModelRegistryClient


@pytest.fixture
def config():
    config = load_config("tests/unit/serving/serving_test_config.yaml")
    config.forecast.target_type = "return"
    config.forecast.return_to_price_method = "log"
    return config


def artifact(config):
    model = torch.nn.Linear(1, 1)
    inference = config.inference
    model.dataset_parameters = {
        "target": "target", "group_ids": [inference.group_id_field],
        "max_encoder_length": inference.max_encoder_length,
        "max_prediction_length": inference.max_prediction_length,
        "static_categoricals": inference.static_categoricals,
        "time_varying_known_categoricals": inference.time_varying_known_categoricals,
        "time_varying_known_reals": inference.time_varying_known_reals,
        "time_varying_unknown_reals": inference.time_varying_unknown_reals,
    }
    training_config = SimpleNamespace(
        training=SimpleNamespace(max_encoder_length=inference.max_encoder_length,
                                 max_prediction_length=inference.max_prediction_length,
                                 quantiles=config.forecast.quantiles),
        features=SimpleNamespace(**{key: getattr(inference, key) for key in (
            "static_categoricals", "time_varying_known_categoricals",
            "time_varying_known_reals", "time_varying_unknown_reals")}),
    )
    attach_target_contract(model, model.dataset_parameters, training_config)
    model.cumulative_calibration = {
        "method": "empirical_cumulative_residuals_v1",
        "sample_count": 30,
        "quantiles": list(config.forecast.quantiles),
        "offsets": [[-0.02, 0.0, 0.02] for _ in range(inference.max_prediction_length)],
    }
    return model


def test_contract_survives_model_serialization(tmp_path, config):
    path = tmp_path / "model.pt"
    torch.save(artifact(config), path)
    model = torch.load(path, weights_only=False)
    assert validate_target_contract(model, config).return_to_price_method == "log"


@pytest.mark.parametrize("field,value", [
    ("target_type", "price"),
    ("return_to_price_method", "simple"),
    ("base_price_feature", "open"),
])
def test_rejects_serving_mismatch(config, field, value):
    setattr(config.forecast, field, value)
    with pytest.raises(ValueError, match=field):
        validate_target_contract(artifact(config), config)


@pytest.mark.parametrize("change", ["missing", "incomplete", "version", "definition", "dataset"])
def test_rejects_invalid_artifacts(config, change):
    model = artifact(config)
    if change == "missing":
        del model.target_contract
    elif change == "incomplete":
        del model.target_contract["return_to_price_method"]
    elif change == "version":
        model.target_contract["schema_version"] = 3
    elif change == "definition":
        model.target_contract["target_definition"] = "cumulative return"
    else:
        model.dataset_parameters["target"] = "close"
    with pytest.raises(ValueError):
        validate_target_contract(model, config)


def test_load_champion_fails_closed_without_fallback(config):
    with patch("polyhorizon.serving.services.model_registry.MlflowClient"), patch(
        "mlflow.pytorch.load_model", return_value=SimpleNamespace()
    ), patch("mlflow.pyfunc.load_model") as fallback:
        registry = ModelRegistryClient(config)
        with patch.object(registry, "ensure_champion_alias", return_value="1"):
            with pytest.raises(ValueError, match="target_contract"):
                registry.load_champion()
        fallback.assert_not_called()


def test_load_champion_accepts_verified_contract(config):
    model = artifact(config)
    with patch("polyhorizon.serving.services.model_registry.MlflowClient"), patch(
        "mlflow.pytorch.load_model", return_value=model
    ):
        registry = ModelRegistryClient(config)
        with patch.object(registry, "ensure_champion_alias", return_value="1"):
            assert registry.load_champion().model is model


def test_defaults_reconstruct_log_returns():
    config = ForecastConfig()
    assert config.target_type == "return"
    assert config.return_to_price_method == "log"
    service = object.__new__(ForecastService)
    service.config = SimpleNamespace(forecast=config)
    prices = service._returns_to_prices(100.0, np.log([1.1, 0.9]))
    np.testing.assert_allclose(prices, [110.0, 99.0])


def test_cannot_label_a_different_training_target():
    with pytest.raises(ValueError, match="Training dataset target"):
        attach_target_contract(SimpleNamespace(), {"target": "close"}, SimpleNamespace())
