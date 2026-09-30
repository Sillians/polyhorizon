import numpy as np
import pytest

from polyhorizon.core.cumulative_calibration import (
    calibrated_price_paths,
    fit_cumulative_residuals,
)


def test_calibrated_paths_use_cumulative_residuals():
    actual = np.full((40, 3), 0.01)
    prediction = np.repeat(np.array([[[0.0, 0.01, 0.02]]]), 40, axis=0)
    prediction = np.repeat(prediction, 3, axis=1)
    fitted = fit_cumulative_residuals(actual, prediction, [0.1, 0.5, 0.9])
    prices = calibrated_price_paths(100, prediction[0], fitted)
    np.testing.assert_allclose(prices[:, 1], 100 * np.exp([0.01, 0.02, 0.03]))
    assert np.all(np.diff(prices, axis=1) >= 0)


def test_calibration_rejects_small_or_misaligned_samples():
    with pytest.raises(ValueError, match="at least 30"):
        fit_cumulative_residuals(np.ones((2, 3)), np.ones((2, 3, 3)), [0.1, 0.5, 0.9])
    with pytest.raises(ValueError, match="matching"):
        fit_cumulative_residuals(np.ones((40, 3)), np.ones((40, 2, 3)), [0.1, 0.5, 0.9])
