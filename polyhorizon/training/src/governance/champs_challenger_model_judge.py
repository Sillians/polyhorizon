import numpy as np
import mlflow
import torch
from mlflow.tracking import MlflowClient
from pytorch_forecasting import TimeSeriesDataSet
from typing import Dict, Any, Iterable, Optional, Tuple, Union

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.configs.settings import GovernanceConfig
from polyhorizon.training.src.governance.qualification import evaluation_evidence, qualification_failures
from polyhorizon.training.src.monitoring.metrics import TFTForecastEvaluator
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("Champions and Challenger Model Judge")


DEFAULT_DIRECTION_THRESHOLD = 0.001  # ignore <0.1% moves
DEFAULT_IMPROVEMENT_THRESHOLD = 0.02  # 2% improvement required
DEFAULT_MAX_MAE_DEGRADATION = 0.10  # reject if MAE degrades >10%
DEFAULT_SCORE_WEIGHTS = (0.7, 0.3)  # (hit_rate_weight, mae_weight)


class ModelJudge:
    def __init__(
        self,
        config: Config,
        direction_threshold: Optional[float] = None,
        improvement_threshold: Optional[float] = None,
        max_mae_degradation: Optional[float] = None,
        score_weights: Optional[Tuple[float, float]] = None,
        stability_max_std: Optional[float] = None,
    ):
        self.config = config
        self.client = MlflowClient()
        self.model_name = self.config.model_registry.name
        self.champion_alias = self.config.model_registry.champion_alias # e.g., "champion"
        self.challenger_alias = self.config.model_registry.staging_alias # e.g., "challenger"
        governance_cfg = getattr(self.config, "governance", None)
        self.qualification_policy = governance_cfg or GovernanceConfig()
        self.direction_threshold = self._resolve_threshold(
            direction_threshold,
            getattr(governance_cfg, "direction_threshold", DEFAULT_DIRECTION_THRESHOLD),
        )
        self.improvement_threshold = self._resolve_threshold(
            improvement_threshold,
            getattr(governance_cfg, "improvement_threshold", DEFAULT_IMPROVEMENT_THRESHOLD),
        )
        self.max_mae_degradation = self._resolve_threshold(
            max_mae_degradation,
            getattr(governance_cfg, "max_mae_degradation", DEFAULT_MAX_MAE_DEGRADATION),
        )
        self.score_weights = score_weights or tuple(getattr(governance_cfg, "score_weights", DEFAULT_SCORE_WEIGHTS))
        self.stability_max_std = self._resolve_threshold(
            stability_max_std,
            getattr(governance_cfg, "stability_max_std", None),
        )

    def get_model_by_alias(self, alias: str):
        """Loads a model from the registry using its alias."""
        try:
            version = self.client.get_model_version_by_alias(self.model_name, alias)
        except mlflow.exceptions.MlflowException as exc:
            if exc.error_code == "RESOURCE_DOES_NOT_EXIST":
                return None
            raise
        return mlflow.pytorch.load_model(f"models:/{self.model_name}/{version.version}")

    def evaluate_financial_performance(
        self,
        model: Any,
        dataset: TimeSeriesDataSet,
    ) -> Dict[str, float]:
        """
        Runs inference and calculates financial utility metrics.
        """
        if dataset is None:
            raise ValueError("dataset must not be None")

        model.eval()
        with torch.no_grad():
            # Use quantile-aware evaluation
            raw_preds = model.predict(dataset, mode="quantiles", return_x=False)
            # raw_preds shape: [N, horizon, quantiles]

        # Extract aligned targets from the dataloader
        actuals = torch.cat([y[0] for x, y in dataset.to_dataloader(train=False)])
        y_true = actuals.detach().cpu().numpy()
        raw_preds = raw_preds.detach().cpu().numpy()
        evidence = evaluation_evidence(y_true, raw_preds, self.config.training.quantiles)

        # Select median quantile (p50) from predictions
        quantile_index = self._get_median_quantile_index()
        median_preds = raw_preds[..., quantile_index]
        y_pred = median_preds

        y_true_2d, y_pred_2d = self._align_forecasts(y_true, y_pred)
        forecast_metrics = self._evaluate_forecasts(y_true_2d, raw_preds)
        if y_true_2d.size < 2 or y_pred_2d.size < 2:
            logger.warning("Not enough data points to compute return-based metrics.")
            return {
                "mae": np.nan,
                "hit_rate": np.nan,
                "composite_score": np.nan,
                "forecast_metrics": forecast_metrics,
            }

        # The target is already a one-step log return. Taking another
        # difference here would evaluate returns-of-returns instead of the
        # quantity the TFT was trained to predict.
        target_values = y_true_2d.flatten()
        predicted_values = y_pred_2d.flatten()
        target_values, predicted_values = self._align_series(target_values, predicted_values)

        mae = float(np.mean(np.abs(target_values - predicted_values)))

        direction_true = np.where(
            np.abs(target_values) > self.direction_threshold,
            np.sign(target_values),
            0,
        )
        direction_pred = np.where(
            np.abs(predicted_values) > self.direction_threshold,
            np.sign(predicted_values),
            0,
        )

        mask = direction_true != 0
        if np.any(mask):
            hit_rate = float(np.mean(direction_true[mask] == direction_pred[mask]))
        else:
            logger.warning("Directional hit rate mask empty; all moves below threshold.")
            hit_rate = 0.0

        hit_weight, mae_weight = self.score_weights
        score = float(hit_weight * hit_rate - mae_weight * mae)

        return {
            **evidence,
            "mae": mae,
            "hit_rate": hit_rate,
            "composite_score": score,
            "forecast_metrics": forecast_metrics,
        }

    def evaluate_across_windows(
        self,
        model: Any,
        datasets: Iterable[TimeSeriesDataSet],
    ) -> Dict[str, float]:
        scores = []
        maes = []
        hit_rates = []
        windows = []
        for ds in datasets:
            metrics = self.evaluate_financial_performance(model, ds)
            windows.append(metrics)
            scores.append(metrics["composite_score"])
            maes.append(metrics["mae"])
            hit_rates.append(metrics["hit_rate"])

        return {
            "mae": float(np.mean(maes)),
            "hit_rate": float(np.mean(hit_rates)),
            "composite_score": float(np.mean(scores)),
            "stability": float(np.std(scores)),
            "windows": windows,
        }

    def compare_and_promote(
        self,
        challenger_metadata: Dict[str, Any],
        inference_ds: Union[TimeSeriesDataSet, Iterable[TimeSeriesDataSet]],
    ):
        """
        Head-to-head battle between Champion and the new Challenger.
        """
        result = self.compare_and_promote_with_metrics(challenger_metadata, inference_ds)
        return result["promoted"]

    def compare_and_promote_with_metrics(
        self,
        challenger_metadata: Dict[str, Any],
        inference_ds: Union[TimeSeriesDataSet, Iterable[TimeSeriesDataSet]],
    ) -> Dict[str, Any]:
        """
        Head-to-head battle between Champion and the new Challenger.
        Returns promotion decision plus detailed metrics.
        """
        # 1. Load both players
        champion = self.get_model_by_alias(self.champion_alias)
        # We use the specific version just trained to ensure we aren't 
        # accidentally comparing against an older challenger
        challenger_uri = f"models:/{self.model_name}/{challenger_metadata['model_version']}"
        challenger = mlflow.pytorch.load_model(challenger_uri)
        if not isinstance(inference_ds, TimeSeriesDataSet):
            inference_ds = list(inference_ds)

        # 2. Evaluation
        if isinstance(inference_ds, TimeSeriesDataSet):
            challenger_metrics = self.evaluate_financial_performance(challenger, inference_ds)
            champion_metrics = None
        else:
            challenger_metrics = self.evaluate_across_windows(challenger, inference_ds)
            champion_metrics = None
        
        windows = challenger_metrics.get("windows", [challenger_metrics])
        reasons = []
        if not windows:
            reasons.append("empty_evaluation")
        for window in windows:
            reasons.extend(qualification_failures(window, self.qualification_policy))
            if not self._passes_horizon_gates(window):
                reasons.append("horizon_gates")
        if self.stability_max_std is not None and "stability" in challenger_metrics:
            stability = challenger_metrics["stability"]
            if not np.isfinite(stability) or stability > self.stability_max_std:
                reasons.append("unstable_metrics")
        if reasons:
            logger.warning("Promotion rejected: %s", reasons)
            return {"promoted": False, "challenger_metrics": challenger_metrics,
                    "champion_metrics": None, "improvement": None, "rejection_reasons": reasons}

        if champion is None:
            logger.info("No Champion found. Initializing first production model.")
            self._promote(challenger_metadata['model_version'], "Initial production promotion")
            return {
                "promoted": True,
                "challenger_metrics": challenger_metrics,
                "champion_metrics": None,
                "improvement": None,
            }

        if champion_metrics is None:
            if isinstance(inference_ds, TimeSeriesDataSet):
                champion_metrics = self.evaluate_financial_performance(champion, inference_ds)
            else:
                champion_metrics = self.evaluate_across_windows(champion, inference_ds)
        
        # 3. Decision Gate
        improvement = self._compute_improvement(
            challenger_metrics['composite_score'],
            champion_metrics['composite_score'],
        )
        
        logger.info("--- Battle Results for %s ---", self.model_name)
        logger.info(
            "Champion Score: %.4f (Hit Rate: %.2f%%, MAE: %.6f)",
            champion_metrics['composite_score'],
            champion_metrics['hit_rate'] * 100,
            champion_metrics['mae'],
        )
        logger.info(
            "Challenger Score: %.4f (Hit Rate: %.2f%%, MAE: %.6f)",
            challenger_metrics['composite_score'],
            challenger_metrics['hit_rate'] * 100,
            challenger_metrics['mae'],
        )

        self._log_promotion_metrics(challenger_metrics, champion_metrics)

        if not self._passes_risk_checks(challenger_metrics, champion_metrics):
            logger.info("Challenger rejected due to risk guardrails.")
            return {
                "promoted": False,
                "challenger_metrics": challenger_metrics,
                "champion_metrics": champion_metrics,
                "improvement": improvement,
            }

        # Threshold check: Challenger must be at least 2% better
        if improvement is not None and improvement > self.improvement_threshold:
            logger.info(
                "Challenger is %.2f%% better. Promoting to @%s!",
                improvement * 100,
                self.champion_alias,
            )
            self._promote(challenger_metadata['model_version'], f"Beat champion by {improvement:.2%}")
            return {
                "promoted": True,
                "challenger_metrics": challenger_metrics,
                "champion_metrics": champion_metrics,
                "improvement": improvement,
            }
        else:
            if improvement is None:
                logger.info("Challenger rejected. Improvement could not be computed safely.")
            else:
                logger.info("Challenger rejected. Improvement of %.2f%% below threshold.", improvement * 100)
            return {
                "promoted": False,
                "challenger_metrics": challenger_metrics,
                "champion_metrics": champion_metrics,
                "improvement": improvement,
            }

    def _promote(self, version: str, comment: str):
        """Assigns the @champion alias to the winning version."""
        old_champion_version = self._get_current_champion_version()
        self.client.set_model_version_tag(
            name=self.model_name, version=version,
            key="governance_qualification", value="passed-v1",
        )
        self.client.set_registered_model_alias(
            name=self.model_name,
            alias=self.champion_alias,
            version=version
        )
        self.client.update_model_version(
            name=self.model_name,
            version=version,
            description=comment
        )
        if old_champion_version is not None:
            self.client.set_model_version_tag(
                name=self.model_name,
                version=old_champion_version,
                key="replaced_by",
                value=version
            )

    def _get_current_champion_version(self) -> Optional[str]:
        try:
            version = self.client.get_model_version_by_alias(
                name=self.model_name,
                alias=self.champion_alias,
            )
            return version.version
        except mlflow.exceptions.MlflowException as exc:
            if exc.error_code == "RESOURCE_DOES_NOT_EXIST":
                return None
            raise

    def _get_median_quantile_index(self) -> int:
        quantiles = self.config.training.quantiles
        if not quantiles:
            return 1
        try:
            return quantiles.index(0.5)
        except ValueError:
            return len(quantiles) // 2

    def _align_series(self, y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        min_len = min(y_true.size, y_pred.size)
        if y_true.size != y_pred.size:
            logger.warning(
                "Length mismatch between y_true (%d) and y_pred (%d); truncating to %d.",
                y_true.size,
                y_pred.size,
                min_len,
            )
        return y_true[:min_len], y_pred[:min_len]

    def _align_forecasts(self, y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        if y_true.ndim == 1:
            y_true = y_true[:, None]
        if y_pred.ndim == 1:
            y_pred = y_pred[:, None]
        n = min(y_true.shape[0], y_pred.shape[0])
        h = min(y_true.shape[1], y_pred.shape[1])
        if y_true.shape != y_pred.shape:
            logger.warning(
                "Forecast shape mismatch y_true %s vs y_pred %s; truncating to (%d, %d).",
                y_true.shape,
                y_pred.shape,
                n,
                h,
            )
        return y_true[:n, :h], y_pred[:n, :h]

    def _compute_improvement(self, challenger_score: float, champion_score: float) -> Optional[float]:
        if not np.isfinite(challenger_score) or not np.isfinite(champion_score):
            return None
        denom = abs(champion_score)
        if denom < 1e-9:
            return None
        return (challenger_score - champion_score) / denom

    def _passes_risk_checks(self, challenger_metrics: Dict[str, float], champion_metrics: Dict[str, float]) -> bool:
        if not np.isfinite(challenger_metrics["mae"]) or not np.isfinite(champion_metrics["mae"]):
            logger.warning("MAE is nonfinite; failing risk checks.")
            return False
        if challenger_metrics["mae"] > champion_metrics["mae"] * (1.0 + self.max_mae_degradation):
            logger.info(
                "MAE degradation too high: challenger %.6f vs champion %.6f.",
                challenger_metrics["mae"],
                champion_metrics["mae"],
            )
            return False
        if self.stability_max_std is not None and "stability" in challenger_metrics:
            stability = challenger_metrics["stability"]
            if not np.isnan(stability) and stability > self.stability_max_std:
                logger.info(
                    "Stability too volatile: challenger std %.6f exceeds max %.6f.",
                    stability,
                    self.stability_max_std,
                )
                return False
        if not all(self._passes_horizon_gates(window)
                   for window in challenger_metrics.get("windows", [challenger_metrics])):
            return False
        return True

    def _resolve_threshold(self, override: Optional[float], default: Optional[float]) -> Optional[float]:
        if override is not None:
            return override
        return default

    def _evaluate_forecasts(self, y_true: np.ndarray, raw_preds: np.ndarray) -> Dict[str, Any]:
        quantiles = self.config.training.quantiles
        evaluator = TFTForecastEvaluator()

        y_true_2d = y_true
        if y_true_2d.ndim == 1:
            y_true_2d = y_true_2d[:, None]

        preds = raw_preds
        if preds.ndim == 2:
            preds = preds[:, None, :]

        n = min(y_true_2d.shape[0], preds.shape[0])
        h = min(y_true_2d.shape[1], preds.shape[1])
        if n == 0 or h == 0:
            return {"overall": {}, "by_horizon": {}}

        return evaluator.evaluate(
            y_true=y_true_2d[:n, :h],
            y_pred_quantiles=preds[:n, :h, :],
            quantiles=quantiles,
        )

    def _log_promotion_metrics(self, challenger_metrics: Dict[str, Any], champion_metrics: Dict[str, Any]) -> None:
        if mlflow.active_run() is None:
            return
        self._log_metric_bundle("challenger", challenger_metrics)
        self._log_metric_bundle("champion", champion_metrics)

    def _log_metric_bundle(self, prefix: str, metrics: Dict[str, Any]) -> None:
        flat: Dict[str, float] = {}
        for key in ("mae", "hit_rate", "composite_score", "stability"):
            if key in metrics and metrics[key] is not None and not np.isnan(metrics[key]):
                flat[f"{prefix}_{key}"] = float(metrics[key])
        mlflow.log_metrics(flat)

        forecast = metrics.get("forecast_metrics") or {}
        overall = forecast.get("overall") or {}
        by_horizon = forecast.get("by_horizon") or {}

        for metric_name, value in overall.items():
            if isinstance(value, dict):
                for qv, v in value.items():
                    if v is None or np.isnan(v):
                        continue
                    mlflow.log_metric(f"{prefix}_forecast_{metric_name}_{qv}", float(v))
            else:
                if value is None or np.isnan(value):
                    continue
                mlflow.log_metric(f"{prefix}_forecast_{metric_name}", float(value))

        for metric_name, values in by_horizon.items():
            if not isinstance(values, list):
                continue
            for idx, v in enumerate(values, start=1):
                if v is None or np.isnan(v):
                    continue
                mlflow.log_metric(f"{prefix}_forecast_{metric_name}_h{idx}", float(v))

    def _passes_horizon_gates(self, challenger_metrics: Dict[str, Any]) -> bool:
        governance_cfg = getattr(self.config, "governance", None)
        gates = getattr(governance_cfg, "horizon_gates", []) if governance_cfg else []
        if not gates:
            return True

        forecast = challenger_metrics.get("forecast_metrics") or {}
        by_horizon = forecast.get("by_horizon") or {}
        if not by_horizon:
            logger.warning("Missing forecast by-horizon metrics; failing horizon gates.")
            return False

        for gate in gates:
            max_h = int(gate.horizon_max)
            for metric_name, limit in (
                ("wape", gate.max_wape),
                ("mae", gate.max_mae),
                ("smape", gate.max_smape),
            ):
                if limit is None:
                    continue
                series = by_horizon.get(metric_name)
                if not series or len(series) < max_h:
                    logger.warning("Missing %s for horizon gates.", metric_name)
                    return False
                for h_idx in range(min(max_h, len(series))):
                    value = series[h_idx]
                    if value is None or not np.isfinite(value):
                        return False
                    if value > limit:
                        logger.info(
                            "Horizon gate failed: %s h%d %.6f > %.6f.",
                            metric_name,
                            h_idx + 1,
                            value,
                            limit,
                        )
                        return False

            if gate.min_coverage is not None:
                series = by_horizon.get("coverage")
                if not series or len(series) < max_h:
                    logger.warning("Missing coverage for horizon gates.")
                    return False
                for h_idx in range(min(max_h, len(series))):
                    value = series[h_idx]
                    if value is None or not np.isfinite(value):
                        return False
                    if value < gate.min_coverage:
                        logger.info(
                            "Horizon gate failed: coverage h%d %.6f < %.6f.",
                            h_idx + 1,
                            value,
                            gate.min_coverage,
                        )
                        return False

        return True
