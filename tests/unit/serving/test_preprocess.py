from pathlib import Path

import pandas as pd

from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services.preprocess import ServingPreprocessor


def _config():
    path = Path(__file__).parent / "serving_test_config.yaml"
    return load_config(path)


def _history(rows: int = 30) -> pd.DataFrame:
    timestamps = pd.date_range("2026-07-20 14:00", periods=rows, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "symbol": ["NVDA"] * rows,
            "event_timestamp": timestamps,
            "open": [100.0] * rows,
            "high": [101.0] * rows,
            "low": [99.0] * rows,
            "close": [100.0 + index / 10 for index in range(rows)],
            "total_volume": [1000.0] * rows,
            "rolling_avg_close": [100.0] * rows,
            "rolling_volatility_close": [0.1] * rows,
        }
    )


def test_prediction_frame_appends_future_market_decoder_rows():
    config = _config()
    preprocessor = ServingPreprocessor(config)
    history = _history()

    result = preprocessor.build_prediction_frame(history)

    assert len(result) == len(history) + config.inference.max_prediction_length
    assert result[config.inference.time_field].is_monotonic_increasing
    assert result[config.inference.time_idx_field].tolist() == list(range(len(result)))
    assert result[config.inference.target].notna().all()
    assert result[config.inference.time_field].iloc[-1] > history["event_timestamp"].iloc[-1]
