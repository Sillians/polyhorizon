"""Calibrate cumulative log-return intervals on held-out decoder paths."""

import numpy as np


def fit_cumulative_residuals(actual, predicted, quantiles):
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    quantiles = tuple(float(value) for value in quantiles)
    if actual.ndim != 2 or predicted.shape != (*actual.shape, len(quantiles)):
        raise ValueError("Calibration paths must have matching sample and horizon dimensions")
    if actual.shape[0] < 30 or not np.isfinite(actual).all() or not np.isfinite(predicted).all():
        raise ValueError("Calibration requires at least 30 finite held-out paths")
    median_idx = quantiles.index(0.5)
    residuals = np.cumsum(actual - predicted[:, :, median_idx], axis=1)
    offsets = np.quantile(residuals, quantiles, axis=0).T
    return {"method": "empirical_cumulative_residuals_v1",
            "sample_count": int(actual.shape[0]),
            "quantiles": list(quantiles), "offsets": offsets.tolist()}


def calibrated_price_paths(base_price, step_quantiles, calibration):
    predictions = np.asarray(step_quantiles, dtype=float)
    offsets = np.asarray(calibration["offsets"], dtype=float)
    quantiles = calibration["quantiles"]
    if predictions.ndim != 2 or offsets.shape != predictions.shape:
        raise ValueError("Calibration horizon and prediction shape do not match")
    median = predictions[:, quantiles.index(0.5)]
    cumulative = np.cumsum(median)[:, None] + offsets
    return float(base_price) * np.exp(cumulative)
