import numpy as np
from typing import Dict, Iterable, List, Tuple

""" 
full multi‑horizon probabilistic evaluation
Supports y_true [N, H] and y_pred_quantiles [N, H, Q].
Computes:
    - Pinball loss (averaged over quantiles)
    - Coverage + interval width for prediction intervals
    - Calibration error per quantile (how far observed ≤ predicted is from expected)
    - Deterministic metrics on median: MAE, RMSE, SMAPE, WAPE
Returns overall + per‑horizon breakdown.
"""

class TFTForecastEvaluator:
    """
    Multi-horizon, quantile-aware evaluator for TFT forecasts.

    Expected shapes:
      y_true: [N, H] or [N]
      y_pred_quantiles: [N, H, Q] or [N, Q]
    """

    @staticmethod
    def pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, quantile: float) -> float:
        delta = y_true - y_pred
        loss = np.where(delta >= 0, quantile * delta, (quantile - 1) * delta)
        return float(np.mean(loss))

    @staticmethod
    def coverage(y_true: np.ndarray, q_low: np.ndarray, q_high: np.ndarray) -> float:
        within_bounds = (y_true >= q_low) & (y_true <= q_high)
        return float(np.mean(within_bounds))

    @staticmethod
    def interval_width(q_low: np.ndarray, q_high: np.ndarray) -> float:
        return float(np.mean(q_high - q_low))

    @staticmethod
    def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        return float(np.mean(np.abs(y_true - y_pred)))

    @staticmethod
    def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    @staticmethod
    def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        denom = (np.abs(y_true) + np.abs(y_pred)) / 2.0
        return float(np.mean(np.where(denom == 0, 0.0, np.abs(y_true - y_pred) / denom)))

    @staticmethod
    def wape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        denom = np.sum(np.abs(y_true))
        if denom == 0:
            return 0.0
        return float(np.sum(np.abs(y_true - y_pred)) / denom)

    @staticmethod
    def _ensure_3d(y_true: np.ndarray, y_pred_quantiles: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        if y_true.ndim == 1:
            y_true = y_true[:, None]
        if y_pred_quantiles.ndim == 2:
            y_pred_quantiles = y_pred_quantiles[:, None, :]
        return y_true, y_pred_quantiles

    @staticmethod
    def _find_quantile_index(quantiles: List[float], target: float) -> int:
        if target in quantiles:
            return quantiles.index(target)
        return int(np.argmin(np.abs(np.array(quantiles) - target)))

    def evaluate(
        self,
        y_true: np.ndarray,
        y_pred_quantiles: np.ndarray,
        quantiles: Iterable[float],
        interval: Tuple[float, float] = (0.1, 0.9),
        median_quantile: float = 0.5,
    ) -> Dict[str, Dict[str, List[float] | float]]:
        quantiles = list(quantiles)
        y_true, y_pred_quantiles = self._ensure_3d(y_true, y_pred_quantiles)

        n, h, q = y_pred_quantiles.shape
        if y_true.shape[0] != n or y_true.shape[1] != h:
            raise ValueError("y_true and y_pred_quantiles must align on [N, H].")
        if q != len(quantiles):
            raise ValueError("Quantiles length must match last dimension of y_pred_quantiles.")

        q_low_idx = self._find_quantile_index(quantiles, interval[0])
        q_high_idx = self._find_quantile_index(quantiles, interval[1])
        q_med_idx = self._find_quantile_index(quantiles, median_quantile)

        by_horizon: Dict[str, List[float]] = {
            "pinball": [],
            "coverage": [],
            "interval_width": [],
            "mae": [],
            "rmse": [],
            "smape": [],
            "wape": [],
        }

        calibration_errors = {str(qv): [] for qv in quantiles}

        for horizon in range(h):
            y_h = y_true[:, horizon]
            preds_h = y_pred_quantiles[:, horizon, :]

            # Probabilistic metrics
            pinball_avg = float(np.mean([self.pinball_loss(y_h, preds_h[:, qi], qv) for qi, qv in enumerate(quantiles)]))
            by_horizon["pinball"].append(pinball_avg)
            by_horizon["coverage"].append(self.coverage(y_h, preds_h[:, q_low_idx], preds_h[:, q_high_idx]))
            by_horizon["interval_width"].append(self.interval_width(preds_h[:, q_low_idx], preds_h[:, q_high_idx]))

            # Deterministic metrics on median
            y_med = preds_h[:, q_med_idx]
            by_horizon["mae"].append(self.mae(y_h, y_med))
            by_horizon["rmse"].append(self.rmse(y_h, y_med))
            by_horizon["smape"].append(self.smape(y_h, y_med))
            by_horizon["wape"].append(self.wape(y_h, y_med))

            # Calibration: fraction of y <= q_pred vs q
            for qi, qv in enumerate(quantiles):
                observed = float(np.mean(y_h <= preds_h[:, qi]))
                calibration_errors[str(qv)].append(abs(observed - qv))

        overall = {
            "pinball": float(np.mean(by_horizon["pinball"])),
            "coverage": float(np.mean(by_horizon["coverage"])),
            "interval_width": float(np.mean(by_horizon["interval_width"])),
            "mae": float(np.mean(by_horizon["mae"])),
            "rmse": float(np.mean(by_horizon["rmse"])),
            "smape": float(np.mean(by_horizon["smape"])),
            "wape": float(np.mean(by_horizon["wape"])),
            "calibration_error": {
                qv: float(np.mean(errs)) for qv, errs in calibration_errors.items()
            },
        }

        return {
            "overall": overall,
            "by_horizon": by_horizon,
        }
    
