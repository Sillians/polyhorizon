import pandas as pd
import numpy as np
import mlflow
import tempfile
from pathlib import Path
from typing import Dict, Any
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score

from evidently import Report
from evidently.presets import DataDriftPreset

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.utils.logger import get_logger
from datetime import datetime, timezone
logger = get_logger("DriftDetector")

_MIN_REF_ROWS = 50
_MIN_CUR_ROWS = 20
_PSI_BINS = 10


""" 
- adversarial AUC (powerful global signal)
- PSI per feature (robust univariate drift)
- Wasserstein (if SciPy available), with graceful fallback
- Logs aggregate metrics (pct symbols drifted, avg AUC)
- JSON drift report + HTML drift report in MLflow
- Better drift gating in retraining flow now uses config.drift.threshold as drift percent threshold.

Covariate drift + target drift both drive retraining.
Target drift is computed using the same training target logic to prevent inconsistencies.
MLflow now logs target drift metrics alongside symbol drift.
"""

class SymbolDriftDetector:
    def __init__(self, config: Config):
        self.config = config
        self.ignore_cols = self.config.drift.ignore_columns
        self.threshold = self.config.drift.threshold_auc
        self.threshold_pct = self.config.drift.threshold
        self.threshold_wasserstein = self.config.drift.threshold_wasserstein
        self.target_col = self.config.features.target

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filters for numeric features and removes noise/IDs."""
        return df.drop(columns=self.ignore_cols, errors='ignore').select_dtypes(include=[np.number]).dropna()

    def _psi(self, ref: pd.Series, cur: pd.Series) -> float:
        ref = ref.replace([np.inf, -np.inf], np.nan).dropna()
        cur = cur.replace([np.inf, -np.inf], np.nan).dropna()
        if ref.empty or cur.empty:
            raise ValueError("PSI requires nonempty samples")
        quantiles = np.linspace(0, 1, _PSI_BINS + 1)
        breakpoints = ref.quantile(quantiles).values
        breakpoints = np.unique(breakpoints)
        if len(breakpoints) < 2:
            return 0.0 if (cur == ref.iloc[0]).all() else float("inf")
        breakpoints[0] = -np.inf
        breakpoints[-1] = np.inf
        ref_counts = np.histogram(ref, bins=breakpoints)[0] / len(ref)
        cur_counts = np.histogram(cur, bins=breakpoints)[0] / len(cur)
        ref_counts = np.maximum(ref_counts, 1e-6)
        cur_counts = np.maximum(cur_counts, 1e-6)
        return float(np.sum((cur_counts - ref_counts) * np.log(cur_counts / ref_counts)))

    def _wasserstein(self, ref: pd.Series, cur: pd.Series) -> float | None:
        try:
            from scipy.stats import wasserstein_distance  # type: ignore
        except Exception:
            return None
        ref = ref.replace([np.inf, -np.inf], np.nan).dropna()
        cur = cur.replace([np.inf, -np.inf], np.nan).dropna()
        if ref.empty or cur.empty:
            return None
        return float(wasserstein_distance(ref.values, cur.values))

    def _compute_target_series(self, df: pd.DataFrame) -> pd.Series:
        if self.target_col in df.columns:
            series = df[self.target_col]
            return series.replace([np.inf, -np.inf], np.nan).dropna()

        df = df.sort_values(["symbol", "event_timestamp"]).copy()
        if "log_close" not in df.columns:
            close = pd.to_numeric(df["close"], errors="coerce")
            df["log_close"] = np.log(close.where(close > 0))
        df["log_close"] = pd.to_numeric(df["log_close"], errors="coerce").replace([np.inf, -np.inf], np.nan)

        target = (
            df.groupby("symbol", observed=True)["log_close"]
            .diff()
            .replace([np.inf, -np.inf], np.nan)
            .dropna()
        )
        return target

    def check_target_drift(self, ref_df: pd.DataFrame, cur_df: pd.DataFrame) -> Dict[str, Any]:
        ref_target = self._compute_target_series(ref_df)
        cur_target = self._compute_target_series(cur_df)

        if len(ref_target) < _MIN_REF_ROWS or len(cur_target) < _MIN_CUR_ROWS:
            return {"drift_detected": False, "method": "insufficient_data", "status": "insufficient_data",
                    "samples": {"ref": len(ref_target), "cur": len(cur_target)}}

        psi = self._psi(ref_target, cur_target)
        wasserstein = self._wasserstein(ref_target, cur_target)
        ref_mean = float(ref_target.mean())
        cur_mean = float(cur_target.mean())
        ref_std = float(ref_target.std(ddof=0))
        cur_std = float(cur_target.std(ddof=0))
        std_ratio = float(cur_std / ref_std) if ref_std > 0 else None

        drift_detected = False
        if psi > self.config.drift.threshold_psi or (wasserstein is not None and wasserstein > self.threshold_wasserstein):
            drift_detected = True

        return {
            "drift_detected": drift_detected,
            "status": "ok",
            "psi": round(psi, 6),
            "wasserstein": None if wasserstein is None else round(wasserstein, 6),
            "ref_mean": round(ref_mean, 6),
            "cur_mean": round(cur_mean, 6),
            "ref_std": round(ref_std, 6),
            "cur_std": round(cur_std, 6),
            "std_ratio": None if std_ratio is None else round(std_ratio, 6),
        }

    def check_symbol_drift(self, 
                           ref_df: pd.DataFrame, 
                           cur_df: pd.DataFrame, 
                           symbol: str) -> Dict:
        """Detects drift and identifies the 'culprit' features."""
        s_ref = self._prepare_data(ref_df[ref_df['symbol'] == symbol])
        s_cur = self._prepare_data(cur_df[cur_df['symbol'] == symbol])

        columns = sorted(set(s_ref.columns) & set(s_cur.columns) - {self.target_col})
        if not columns:
            raise ValueError(f"No shared numeric drift features for {symbol}")
        s_ref, s_cur = s_ref[columns], s_cur[columns]

        if len(s_ref) < _MIN_REF_ROWS or len(s_cur) < _MIN_CUR_ROWS:
            return {"symbol": symbol, "drift_detected": False, "method": "insufficient_data",
                    "status": "insufficient_data", "samples": {"ref": len(s_ref), "cur": len(s_cur)}}

        # Label data: Reference (0) vs Current (1)
        X = pd.concat([s_ref.assign(target=0), s_cur.assign(target=1)])
        y = X.pop('target')
        
        clf = RandomForestClassifier(n_estimators=50, max_depth=3, random_state=42)
        auc = cross_val_score(clf, X, y, cv=3, scoring='roc_auc').mean()
        
        # Identify top drifting features (adversarial importances)
        clf.fit(X, y)
        importances = pd.Series(clf.feature_importances_, index=X.columns).sort_values(ascending=False)
        top_drifters = importances.head(3).to_dict()

        # Additional univariate drift diagnostics
        psi_scores = {col: self._psi(s_ref[col], s_cur[col]) for col in s_ref.columns}
        top_psi = dict(sorted(psi_scores.items(), key=lambda kv: kv[1], reverse=True)[:3])
        avg_psi = float(np.mean(list(psi_scores.values()))) if psi_scores else 0.0

        wasserstein_scores = {}
        for col in s_ref.columns:
            w = self._wasserstein(s_ref[col], s_cur[col])
            if w is not None:
                wasserstein_scores[col] = w
        top_wasserstein = dict(sorted(wasserstein_scores.items(), key=lambda kv: kv[1], reverse=True)[:3])
        avg_wasserstein = float(np.mean(list(wasserstein_scores.values()))) if wasserstein_scores else None

        drift_detected = (auc > self.threshold) or any(v > self.config.drift.threshold_psi for v in psi_scores.values())

        return {
            "symbol": symbol,
            "auc_score": round(auc, 3),
            "drift_detected": drift_detected,
            "status": "ok",
            "top_drift_features": top_drifters,
            "psi_avg": round(avg_psi, 6),
            "psi_top": top_psi,
            "wasserstein_avg": None if avg_wasserstein is None else round(avg_wasserstein, 6),
            "wasserstein_top": top_wasserstein,
            "samples": {"ref": len(s_ref), "cur": len(s_cur)}
        }

    def run_full_suite(self, ref_df: pd.DataFrame, cur_df: pd.DataFrame):
        """Runs per-symbol drift + global Evidently report + MLflow logging."""
        symbols = sorted(set(cur_df['symbol'].dropna()))
        if not symbols or set(symbols) != set(ref_df['symbol'].dropna()):
            raise ValueError("Drift windows must contain the same nonempty symbol universe")
        full_results = {}
        
        is_nested = mlflow.active_run() is not None
        with mlflow.start_run(run_name="drift_analysis", nested=is_nested):
            # 1. Per-Symbol Adversarial Check
            for sym in symbols:
                full_results[sym] = self.check_symbol_drift(ref_df, cur_df, sym)
            
            # 2. Aggregate Metrics
            drifted_symbols = [s for s, r in full_results.items() if r.get('drift_detected')]
            pct_symbols_drifted = len(drifted_symbols) / len(symbols)
            avg_auc = float(np.mean([r.get("auc_score", 0.0) for r in full_results.values()])) if full_results else 0.0
            target_by_symbol = {
                sym: self.check_target_drift(ref_df.loc[ref_df.symbol == sym], cur_df.loc[cur_df.symbol == sym])
                for sym in symbols
            }
            target_drift = self.check_target_drift(ref_df, cur_df)
            target_drift["by_symbol"] = target_by_symbol
            target_drift["drift_detected"] = target_drift["drift_detected"] or any(
                result["drift_detected"] for result in target_by_symbol.values()
            )
            if any(result.get("status") != "ok" for result in target_by_symbol.values()):
                target_drift["status"] = "insufficient_data"
            mlflow.log_metrics({
                "pct_symbols_drifted": pct_symbols_drifted,
                "total_drifted_count": len(drifted_symbols),
                "avg_drift_auc": avg_auc,
                "target_drift_psi": target_drift.get("psi", 0.0) or 0.0,
                "target_drift_wasserstein": target_drift.get("wasserstein", 0.0) or 0.0,
            })
            
            # 3. Log detailed JSON results as an artifact
            mlflow.log_dict(full_results, "per_symbol_drift.json")
            mlflow.log_dict(target_drift, "target_drift.json")

            # 4. Generate & Log Evidently Report (Global View)
            # Only use columns that exist in both for the global report
            common_cols = list(set(ref_df.columns) & set(cur_df.columns))
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=ref_df[common_cols], current_data=cur_df[common_cols])
            
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S_UTC")
            with tempfile.TemporaryDirectory() as directory:
                report_path = Path(directory) / f"drift_report_{timestamp}.html"
                report.save_html(str(report_path))
                mlflow.log_artifact(str(report_path))

            try:
                mlflow.log_dict(report.as_dict(), "drift_report.json")
            except Exception:
                logger.warning("Failed to serialize drift report to JSON.")
            
            summary = {
                "pct_symbols_drifted": pct_symbols_drifted,
                "total_drifted_count": len(drifted_symbols),
                "avg_drift_auc": avg_auc,
                "threshold_pct": self.threshold_pct,
                "threshold_auc": self.threshold,
                "threshold_wasserstein": self.threshold_wasserstein,
            }

            logger.info(
                "Drift Analysis Complete. Report logged. %d symbols drifted (%.2f%%).",
                len(drifted_symbols),
                pct_symbols_drifted * 100,
            )
            return {
                "symbol_results": full_results,
                "target_drift": target_drift,
                "summary": summary,
            }
