import importlib
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest

from polyhorizon.training.configs.settings import GovernanceConfig
from polyhorizon.training.src.governance.qualification import evaluation_evidence, qualification_failures


def good_metrics():
    return dict(mae=0.01, hit_rate=0.6, composite_score=0.4, sample_count=100,
                baseline_mae=0.02, baseline_mae_improvement=0.5,
                maximum_calibration_error=0.05)


@pytest.mark.parametrize("field,value,reason", [
    ("sample_count", 99, "insufficient_samples"),
    ("baseline_mae_improvement", 0.01, "baseline_not_beaten"),
    ("maximum_calibration_error", 0.11, "poor_calibration"),
    ("mae", float("nan"), "missing_or_nonfinite_metrics"),
    ("composite_score", float("inf"), "missing_or_nonfinite_metrics"),
])
def test_absolute_requirements(field, value, reason):
    metrics = good_metrics()
    metrics[field] = value
    assert reason in qualification_failures(metrics, GovernanceConfig())


def test_evidence_baseline_and_calibration_by_horizon():
    actuals = np.linspace(-1, 1, 100)[:, None]
    predictions = np.broadcast_to([-0.8, 0., 0.8], (100, 1, 3)).copy()
    evidence = evaluation_evidence(actuals, predictions, [0.1, 0.5, 0.9])
    assert evidence["sample_count"] == 100
    assert evidence["baseline_mae_improvement"] == 0
    assert evidence["maximum_calibration_error"] < 1e-10
    predictions[0, 0, 0] = np.nan
    with pytest.raises(ValueError, match="finite"):
        evaluation_evidence(actuals, predictions, [0.1, 0.5, 0.9])


def test_crossing_quantiles_and_shape_mismatch_fail():
    with pytest.raises(ValueError, match="Crossing"):
        evaluation_evidence(np.ones((100, 1)), np.tile([2, 1, 3], (100, 1, 1)), [0.1, 0.5, 0.9])
    with pytest.raises(ValueError, match="align"):
        evaluation_evidence(np.ones((100, 2)), np.ones((99, 2, 3)), [0.1, 0.5, 0.9])


def test_missing_and_nested_nonfinite_metrics_fail():
    metrics = good_metrics()
    metrics["forecast_metrics"] = {"coverage": [float("inf")]}
    assert qualification_failures(metrics, GovernanceConfig()) == ["missing_or_nonfinite_metrics"]
    assert qualification_failures({}, GovernanceConfig()) == ["missing_or_nonfinite_metrics"]


@pytest.mark.parametrize("qualified", [False, True])
def test_first_champion_cannot_skip_gates(qualified):
    with patch("polyhorizon.training.utils.logger.get_logger"):
        module = importlib.import_module(
            "polyhorizon.training.src.governance.champs_challenger_model_judge")
    config = SimpleNamespace(model_registry=SimpleNamespace(
        name="test", champion_alias="champion", staging_alias="challenger"),
        governance=GovernanceConfig())
    with patch.object(module, "MlflowClient"):
        judge = module.ModelJudge(config)
    judge.get_model_by_alias = Mock(return_value=None)
    metrics = good_metrics()
    if not qualified:
        metrics["sample_count"] = 1
    judge.evaluate_across_windows = Mock(return_value={"windows": [metrics] * 3})
    judge._promote = Mock()
    pytorch_flavor = importlib.import_module("mlflow.pytorch")
    with patch.object(pytorch_flavor, "load_model") as loader:
        result = judge.compare_and_promote_with_metrics(
            {"model_version": "7", "model_uri": "models:/test@challenger"}, [object()] * 3)
    loader.assert_called_once_with("models:/test/7")
    assert result["promoted"] is False
    assert result.get("approved", False) is qualified
    assert judge._promote.called is qualified
