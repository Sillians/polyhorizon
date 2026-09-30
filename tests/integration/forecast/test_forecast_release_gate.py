"""Mandatory, self-contained training artifact to HTTP forecast release gate."""

from pathlib import Path
from types import SimpleNamespace

import mlflow
import numpy as np
import pandas as pd
import torch
from fastapi.testclient import TestClient
from pytorch_forecasting import TimeSeriesDataSet

from polyhorizon.core.cumulative_calibration import fit_cumulative_residuals
from polyhorizon.core.target_contract import attach_target_contract
from polyhorizon.serving.app.dependencies import get_forecast_service
from polyhorizon.serving.app.main import create_app
from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services.forecast_service import ForecastService
from polyhorizon.serving.services.model_registry import ModelRegistryClient
from polyhorizon.serving.services.preprocess import ServingPreprocessor


class TinyReturnModel(torch.nn.Module):
    """Fit one return parameter; expose the same prediction API used by serving."""

    def __init__(self):
        super().__init__()
        self.mean_return = torch.nn.Parameter(torch.tensor(0.0))

    def forward(self, values):
        return self.mean_return.expand_as(values)

    def predict(self, _dataset, mode="quantiles", return_x=False):
        assert mode == "quantiles" and return_x is False
        step = torch.stack((self.mean_return - 0.002, self.mean_return,
                            self.mean_return + 0.002))
        return step.repeat(3, 1).unsqueeze(0).detach()


class InMemoryCache:
    def get(self, _key):
        return None

    def set(self, _key, _value):
        return None


def test_trained_artifact_serves_calibrated_forecast(tmp_path, monkeypatch):
    config_path = Path(__file__).parents[2] / "unit/serving/serving_test_config.yaml"
    config = load_config(config_path)
    config.mlflow.tracking_uri = tmp_path.as_uri()
    config.model_registry.name = "release-gate-model"
    config.forecast.target_type = "return"
    config.forecast.return_to_price_method = "log"
    config.inference.max_encoder_length = 8
    config.inference.max_prediction_length = 3
    config.offline_store.history_rows = 32

    timestamps = pd.DatetimeIndex(
        list(pd.date_range("2026-07-23T14:00Z", periods=13, freq="30min")) +
        list(pd.date_range("2026-07-24T14:00Z", periods=13, freq="30min"))
    )
    close = 100 * np.exp(np.arange(26) * 0.001)
    history = pd.DataFrame({"symbol": "NVDA", "event_timestamp": timestamps,
                            "open": close, "high": close * 1.001,
                            "low": close * 0.999, "close": close,
                            "total_volume": 1000.0})
    prepared = ServingPreprocessor(config).enrich(history)
    prepared = prepared.dropna(subset=["target"])
    dataset = TimeSeriesDataSet(
        prepared, time_idx="time_idx", target="target", group_ids=["symbol"],
        max_encoder_length=8, min_encoder_length=4,
        max_prediction_length=3, min_prediction_length=1,
        static_categoricals=["symbol"],
        time_varying_unknown_reals=["open", "high", "low", "close"],
        allow_missing_timesteps=True,
    )
    model = TinyReturnModel()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    observed = torch.tensor(prepared["target"].to_numpy(dtype="float32"))
    for _ in range(60):
        optimizer.zero_grad()
        torch.nn.functional.mse_loss(model(observed), observed).backward()
        optimizer.step()
    assert abs(model.mean_return.item() - 0.001) < 0.0001

    model.dataset_parameters = dataset.get_parameters()
    fitted = model.mean_return.item()
    residual_noise = np.random.default_rng(19).normal(0, 0.001, size=(40, 3))
    actual = np.full((40, 3), fitted) + residual_noise
    predicted = model.predict(dataset).numpy().repeat(40, axis=0)
    model.cumulative_calibration = fit_cumulative_residuals(
        actual, predicted, config.forecast.quantiles
    )
    training_config = SimpleNamespace(
        training=SimpleNamespace(max_encoder_length=8, max_prediction_length=3,
                                 quantiles=config.forecast.quantiles),
        features=SimpleNamespace(static_categoricals=["symbol"],
                                 time_varying_known_categoricals=[],
                                 time_varying_known_reals=[],
                                 time_varying_unknown_reals=["open", "high", "low", "close"]),
    )
    attach_target_contract(model, model.dataset_parameters, training_config)

    mlflow.set_tracking_uri(config.mlflow.tracking_uri)
    mlflow.set_experiment("release-gate")
    with mlflow.start_run():
        logged = mlflow.pytorch.log_model(model, name="tiny-return-model")
    registered = mlflow.register_model(logged.model_uri, config.model_registry.name)
    mlflow.MlflowClient().set_registered_model_alias(
        config.model_registry.name, "champion", registered.version
    )
    handle = ModelRegistryClient(config).load_version(registered.version)

    monkeypatch.setattr("polyhorizon.serving.services.forecast_service.require_post_close_features",
                        lambda *_args, **_kwargs: None)
    service = ForecastService(config, handle,
                              SimpleNamespace(get_feature_window=lambda *_: history),
                              InMemoryCache())
    app = create_app(config_path=str(config_path), init_container=False)
    app.dependency_overrides[get_forecast_service] = lambda: service
    response = TestClient(app).post("/v1/forecast", json={"symbol": "NVDA", "horizon": 3,
                                                        "use_cache": False})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["model_version"] == str(registered.version)
    assert len(payload["predictions"]) == 3
    assert payload["predictions"][0]["timestamp"].startswith("2026-07-27")
    assert all(row["p10"] <= row["p50"] <= row["p90"] for row in payload["predictions"])
    assert payload["predictions"][-1]["p50"] > payload["base_price"]
