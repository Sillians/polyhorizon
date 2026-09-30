from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from unittest.mock import Mock

from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services.forecast_service import ForecastService
from polyhorizon.serving.services.model_registry import ModelHandle


def test_post_process_includes_decoder_timestamps():
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    handle = ModelHandle(
        model=object(),
        model_uri="models:/polyhorizon-test/1",
        model_version="1",
        loaded_at=datetime.now(timezone.utc),
        model_flavor="pytorch",
    )
    service = ForecastService(
        config=config,
        model_handle=handle,
        feature_store=SimpleNamespace(),
        cache=SimpleNamespace(),
    )
    timestamps = pd.date_range("2026-07-20T14:00:00Z", periods=4, freq="30min")
    frame = pd.DataFrame(
        {
            "symbol": ["NVDA"] * 4,
            "event_timestamp": timestamps,
            "close": [100.0] * 4,
        }
    )
    predictions = np.array([[[99.0, 100.0, 101.0], [100.0, 101.0, 102.0], [101.0, 102.0, 103.0]]])

    result = service._post_process(predictions, frame, 3, timestamps[0].isoformat())

    assert [row["timestamp"] for row in result.predictions] == [
        timestamp.isoformat() for timestamp in timestamps[-3:]
    ]
    assert service._cache_key("nvda", 3).startswith("v3:NVDA:3:")


def test_stale_cached_and_source_features_cannot_reach_inference():
    from polyhorizon.serving.services.freshness import StaleFeaturesError
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    cache, store = Mock(), Mock()
    cache.get.return_value = {"features_timestamp": "2000-01-03T21:00:00Z"}
    store.get_feature_window.return_value = pd.DataFrame({"event_timestamp": ["2000-01-03T21:00:00Z"]})
    service = ForecastService(config, Mock(), store, cache)
    service._predict_quantiles = Mock()
    with pytest.raises(StaleFeaturesError):
        service.predict("NVDA", horizon=1)
    store.get_feature_window.assert_called_once()
    service._predict_quantiles.assert_not_called()
    cache.set.assert_not_called()


@pytest.mark.parametrize("horizon", [1, 13, 39])
@pytest.mark.parametrize("target_type", ["price", "return"])
def test_short_forecasts_pair_first_predictions_with_first_decoder_times(horizon, target_type, monkeypatch):
    from polyhorizon.serving.services.freshness import require_post_close_features
    monkeypatch.setattr("polyhorizon.serving.services.forecast_service.require_post_close_features",
                        lambda timestamp, delay: require_post_close_features(timestamp, delay, now="2026-07-27T14:00Z"))
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    config.inference.max_prediction_length = 39
    config.forecast.horizon = 39
    config.forecast.target_type = target_type
    config.forecast.return_to_price_method = "log"
    calibrated_model = SimpleNamespace(cumulative_calibration={
        "quantiles": [0.1, 0.5, 0.9],
        "offsets": [[-0.01, 0.0, 0.01] for _ in range(39)],
    })
    handle = ModelHandle(calibrated_model, "models:/polyhorizon-test/1", "1", datetime.now(timezone.utc), "pytorch")
    # The last observed bar is Friday's close; the full decoder spans Mon–Wed.
    history_times = pd.date_range(end="2026-07-24T20:00Z", periods=30, freq="30min")
    history = pd.DataFrame({"symbol": "NVDA", "event_timestamp": history_times,
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "total_volume": 1000.0})
    feature_store = Mock()
    feature_store.get_feature_window.return_value = history
    cache = Mock()
    cache.get.return_value = None
    service = ForecastService(config, handle, feature_store, cache)
    # Distinct values at every decoder position reveal any head/tail mixup.
    steps = np.arange(1, 40, dtype=float)
    values = np.column_stack([steps + 100, steps + 200, steps + 300])
    if target_type == "return":
        values = np.column_stack([steps * 0.0001, steps * 0.0002, steps * 0.0003])
    service._predict_quantiles = Mock(return_value=values[None, ...])
    service.parity_checker.check_once = Mock()

    result = service.predict("NVDA", horizon=horizon, use_cache=False)

    expected_times = [timestamp
        for date in ("2026-07-27", "2026-07-28", "2026-07-29")
        for timestamp in pd.date_range(f"{date}T14:00Z", periods=13, freq="30min")]
    assert result.horizon == horizon
    assert [row["step"] for row in result.predictions] == list(range(1, horizon + 1))
    assert [row["timestamp"] for row in result.predictions] == [t.isoformat() for t in expected_times[:horizon]]
    for col, name in enumerate(("p10", "p50", "p90")):
        expected = values[:horizon, col]
        if target_type == "return":
            expected = 100.0 * np.exp(np.cumsum(values[:horizon, 1]) +
                                       calibrated_model.cumulative_calibration["offsets"][0][col])
        assert [row[name] for row in result.predictions] == np.round(expected, 4).tolist()
    assert result.features_timestamp == history_times[-1].isoformat()
    assert result.base_price == 100.0
    assert result.absolute_change == round(result.predictions[-1]["p50"] - 100.0, 4)
    # Cache hits must preserve the exact timestamp/value pairs as well.
    cache.get.return_value = cache.set.call_args.args[1]
    cached = service.predict("NVDA", horizon=horizon)
    assert cached.cached and cached.predictions == result.predictions
    service._predict_quantiles.assert_called_once()


@pytest.mark.parametrize("prediction_length,frame_length", [(13, 69), (39, 38)])
def test_mismatched_decoder_fails_closed(prediction_length, frame_length):
    config = load_config(Path(__file__).parent / "serving_test_config.yaml")
    config.inference.max_prediction_length = 39
    service = ForecastService(config, Mock(), Mock(), Mock())
    frame = pd.DataFrame({"event_timestamp": pd.date_range("2026-07-20", periods=frame_length, freq="30min", tz="UTC")})
    with pytest.raises(ValueError, match="decoder"):
        service._post_process(np.ones((1, prediction_length, 3)), frame, 1, None)
