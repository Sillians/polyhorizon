from types import SimpleNamespace

import numpy as np
import pandas as pd

from polyhorizon.training.src.monitoring.main_drift_detection import SymbolDriftDetector


def detector():
    config = SimpleNamespace(
        drift=SimpleNamespace(ignore_columns=["symbol", "event_timestamp"], threshold_auc=0.7,
                              threshold=0.2, threshold_wasserstein=0.1, threshold_psi=0.2),
        features=SimpleNamespace(target="target"),
    )
    return SymbolDriftDetector(config)


def test_psi_counts_out_of_range_values():
    subject = detector()
    reference = pd.Series(np.arange(100, dtype=float))
    current = pd.Series(np.arange(100, dtype=float) + 1000)
    assert subject._psi(reference, current) > 0.2


def test_constant_reference_shift_is_not_no_drift():
    assert np.isinf(detector()._psi(pd.Series([1.0] * 100), pd.Series([2.0] * 30)))


def test_target_returns_are_sorted_within_symbol():
    data = pd.DataFrame({"symbol": ["NVDA", "NVDA", "NVDA"],
                         "event_timestamp": [3, 1, 2], "close": [121.0, 100.0, 110.0]})
    returns = detector()._compute_target_series(data).to_numpy()
    assert np.allclose(returns, [np.log(1.1), np.log(1.1)])


def test_insufficient_target_data_is_inconclusive():
    data = pd.DataFrame({"symbol": ["NVDA", "NVDA"],
                         "event_timestamp": [1, 2], "close": [100.0, 101.0]})
    assert detector().check_target_drift(data, data)["status"] == "insufficient_data"
