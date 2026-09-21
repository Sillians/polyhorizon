"""Absolute promotion requirements, including when no incumbent exists."""

import numpy as np


def evaluation_evidence(actuals, predictions, quantiles):
    actuals = np.asarray(actuals)
    predictions = np.asarray(predictions)
    if actuals.ndim != 2 or predictions.shape != (*actuals.shape, len(quantiles)):
        raise ValueError("Evaluation arrays must align exactly on samples and horizons")
    if not actuals.size or not np.isfinite(actuals).all() or not np.isfinite(predictions).all():
        raise ValueError("Evaluation requires nonempty, finite targets and predictions")
    if 0.5 not in quantiles or len(quantiles) < 3 or list(quantiles) != sorted(set(quantiles)):
        raise ValueError("Evaluation requires ordered distinct quantiles including the median")
    if not all(0 < q < 1 for q in quantiles):
        raise ValueError("Quantiles must lie strictly between zero and one")
    if (np.diff(predictions, axis=-1) < 0).any():
        raise ValueError("Crossing quantiles cannot qualify for promotion")
    # Zero log-return is the no-price-change baseline; no fitting or leakage.
    baseline_mae = float(np.abs(actuals).mean())
    mae = float(np.abs(actuals - predictions[..., quantiles.index(0.5)]).mean())
    calibration = np.abs((actuals[..., None] <= predictions).mean(axis=0) - quantiles)
    return {
        "sample_count": int(actuals.shape[0]),
        "baseline_mae": baseline_mae,
        "baseline_mae_improvement": (baseline_mae - mae) / baseline_mae if baseline_mae > 0 else float("nan"),
        "maximum_calibration_error": float(calibration.max()),
    }


def qualification_failures(metrics, policy):
    def finite(value):
        if isinstance(value, dict):
            return all(finite(v) for v in value.values())
        if isinstance(value, (list, tuple)):
            return all(finite(v) for v in value)
        return isinstance(value, (int, float, np.number)) and bool(np.isfinite(value))

    required = ("mae", "hit_rate", "composite_score", "sample_count", "baseline_mae",
                "baseline_mae_improvement", "maximum_calibration_error")
    if any(key not in metrics for key in required) or not finite(metrics):
        return ["missing_or_nonfinite_metrics"]
    failures = []
    if metrics["sample_count"] < policy.minimum_evaluation_samples:
        failures.append("insufficient_samples")
    if metrics["baseline_mae_improvement"] < policy.minimum_baseline_mae_improvement:
        failures.append("baseline_not_beaten")
    if metrics["maximum_calibration_error"] > policy.maximum_calibration_error:
        failures.append("poor_calibration")
    return failures
