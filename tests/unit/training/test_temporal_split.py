from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from polyhorizon.training.src.data.temporal_split import (
    timestamp_split, validation_window_mask, assert_validation_targets_after_cutoff,
)


@pytest.fixture
def bars():
    times = pd.date_range("2026-01-01", periods=60, freq="30min", tz="UTC")
    rows = []
    for symbol in ("NVDA", "AAPL", "MSFT"):
        for i, timestamp in enumerate(times):
            if symbol == "AAPL" and i % 5 == 0 or symbol == "MSFT" and i < 12:
                continue
            rows.append({"symbol": symbol, "event_timestamp": timestamp,
                         "time_idx": 1000 + i * 7, "close": 100 + i * 0.1,
                         "target": float(np.sin(i) * 0.01)})
    return pd.DataFrame(rows).sample(frac=1, random_state=42)


def test_global_timestamp_cutoff_with_unequal_histories(bars):
    original = bars.copy(deep=True)
    train, validation, cutoff, boundaries = timestamp_split(bars, ["symbol"], 0.25, 8, 3)
    assert cutoff == pd.Timestamp("2026-01-01T22:00Z")
    assert (train.event_timestamp <= cutoff).all()
    assert (validation.loc[validation.event_timestamp > cutoff].event_timestamp > train.event_timestamp.max()).all()
    assert validation[validation.event_timestamp <= cutoff].groupby("symbol").size().tolist() == [8, 8, 8]
    assert boundaries.training_last_idx.nunique() > 1
    pd.testing.assert_frame_equal(bars, original)


def test_timezone_representation_does_not_change_boundary(bars):
    utc_split = timestamp_split(bars, ["symbol"], 0.25, 8, 3)
    bars.event_timestamp = bars.event_timestamp.dt.tz_convert("America/New_York")
    local_split = timestamp_split(bars, ["symbol"], 0.25, 8, 3)
    assert local_split[2] == utc_split[2]
    pd.testing.assert_frame_equal(local_split[0], utc_split[0])


@pytest.mark.parametrize("ratio", [0, 1, -0.1, 1.1, float("nan")])
def test_invalid_split_ratio_fails(bars, ratio):
    with pytest.raises(ValueError, match="validation_ratio"):
        timestamp_split(bars, ["symbol"], ratio, 8, 3)


@pytest.mark.parametrize("problem", ["duplicate", "null_time", "no_holdout", "short_history"])
def test_ambiguous_or_insufficient_groups_fail(bars, problem):
    if problem == "duplicate":
        bars = pd.concat([bars, bars.iloc[:1]])
    elif problem == "null_time":
        bars.loc[bars.index[0], "event_timestamp"] = pd.NaT
    elif problem == "no_holdout":
        bars = bars[~((bars.symbol == "MSFT") & (bars.event_timestamp > pd.Timestamp("2026-01-01T22:00Z")))]
    else:
        bars = bars[~((bars.symbol == "MSFT") & (bars.event_timestamp < pd.Timestamp("2026-01-01T21:00Z")))]
    with pytest.raises(ValueError):
        timestamp_split(bars, ["symbol"], 0.25, 8, 3)


def test_boundary_equality_and_missing_target_timestamps_rejected():
    cutoff = pd.Timestamp("2026-01-01T12:00Z")
    frame = pd.DataFrame({"symbol": ["NVDA"] * 3, "time_idx": [0, 1, 2],
                          "event_timestamp": pd.date_range(cutoff, periods=3, freq="30min")})
    boundary = pd.DataFrame({"symbol": ["NVDA"], "training_last_idx": [0]})
    index = pd.DataFrame({"symbol": ["NVDA"] * 2, "time_idx_first_prediction": [0, 1], "time_idx_last": [2, 2]})
    assert validation_window_mask(index, boundary, ["symbol"]).tolist() == [False, True]
    with pytest.raises(ValueError, match="strictly after"):
        assert_validation_targets_after_cutoff(index, frame, ["symbol"], cutoff)
    assert assert_validation_targets_after_cutoff(index.iloc[1:], frame, ["symbol"], cutoff) > cutoff
    with pytest.raises(ValueError, match="observed timestamp"):
        assert_validation_targets_after_cutoff(index.iloc[1:], frame[frame.time_idx != 1], ["symbol"], cutoff)


@pytest.mark.parametrize("categorical_symbols", [False, True])
def test_actual_tft_decoder_targets_never_cross_cutoff(bars, categorical_symbols):
    from polyhorizon.training.src.data.dataset import TFTDatasetFactory

    if categorical_symbols:
        bars.symbol = bars.symbol.astype("category")

    config = SimpleNamespace(
        features=SimpleNamespace(target="target", group_ids=["symbol"], static_categoricals=["symbol"],
            feature_cols=["close", "target"], time_varying_known_categoricals=[],
            time_varying_known_reals=[], time_varying_unknown_reals=["close", "target"]),
        training=SimpleNamespace(validation_ratio=0.25, max_encoder_length=8, max_prediction_length=3),
    )
    with patch("polyhorizon.training.src.data.dataset.get_logger"):
        factory = TFTDatasetFactory(config)
    with patch("mlflow.log_input"), patch("mlflow.log_params"), patch("mlflow.log_dict") as log_split, patch.object(
        factory, "_log_dataset_artifacts"
    ):
        training, validation = factory.create_datasets(bars)
    cutoff = pd.Timestamp(factory.split_metadata["training_cutoff_utc"])
    assert pd.Timestamp(factory.split_metadata["earliest_validation_target_utc"]) > cutoff
    log_split.assert_called_once_with(factory.split_metadata, "temporal_split.json")
    _, validation_frame, _, _ = timestamp_split(bars, ["symbol"], 0.25, 8, 3)
    # Demonstrate that plain from_dataset includes pre-cutoff decoder targets,
    # even with stop_randomization=True: this is the original leakage regression.
    from pytorch_forecasting import TimeSeriesDataSet
    unfiltered = TimeSeriesDataSet.from_dataset(training, validation_frame, stop_randomization=True)
    with pytest.raises(ValueError, match="strictly after"):
        assert_validation_targets_after_cutoff(unfiltered.decoded_index, validation_frame, ["symbol"], cutoff)
    lookup = validation_frame.set_index(["symbol", "time_idx"]).event_timestamp
    seen = set()
    for x, _ in validation.to_dataloader(train=False, batch_size=32, num_workers=0):
        decoded = validation.x_to_index(x)
        for row, symbol in enumerate(decoded.symbol):
            seen.add(symbol)
            length = int(x["decoder_lengths"][row])
            for idx in x["decoder_time_idx"][row, :length].tolist():
                assert lookup.loc[(symbol, idx)] > cutoff
    assert seen == {"NVDA", "AAPL", "MSFT"}
    assert len(training) > 0
