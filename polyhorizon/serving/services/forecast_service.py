from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.services.cache import RedisCache
from polyhorizon.serving.services.feature_store import FeatureStoreClient
from polyhorizon.serving.services.model_registry import ModelHandle
from polyhorizon.serving.services.feature_parity import FeatureParityChecker
from polyhorizon.serving.services.preprocess import ServingPreprocessor
from polyhorizon.serving.services import metrics
from polyhorizon.serving.services.freshness import require_post_close_features, StaleFeaturesError
from polyhorizon.serving.utils.logger import get_logger


@dataclass
class ForecastResult:
    symbol: str
    horizon: int
    quantiles: List[float]
    predictions: List[Dict[str, Any]]
    base_price: float
    absolute_change: float
    percent_change: float
    model_version: str
    model_uri: str
    features_timestamp: str | None
    cached: bool


class ForecastService:
    def __init__(
        self,
        config: Config,
        model_handle: ModelHandle,
        feature_store: FeatureStoreClient,
        cache: RedisCache,
    ) -> None:
        self.config = config
        self.model_handle = model_handle
        self.feature_store = feature_store
        self.cache = cache
        self.preprocessor = ServingPreprocessor(config)
        self.parity_checker = FeatureParityChecker(config)
        self.logger = get_logger("ForecastService")

    def _cache_key(self, symbol: str, horizon: int) -> str:
        # Model versioning prevents a rollout from serving a previous model's
        # forecast until the cache TTL expires.
        # v3 invalidates forecasts cached before decoder timestamp alignment.
        return f"v3:{symbol.upper()}:{horizon}:model-{self.model_handle.model_version}"

    def predict(self, symbol: str, horizon: int | None = None, use_cache: bool = True) -> ForecastResult:
        horizon = horizon or self.config.forecast.horizon
        if horizon < 1:
            raise ValueError("Forecast horizon must be >= 1")
        if horizon > self.config.inference.max_prediction_length:
            raise ValueError("Requested horizon exceeds model max_prediction_length")

        from polyhorizon.core.product_symbols import require_product_symbol
        symbol = require_product_symbol(symbol)
        cache_key = self._cache_key(symbol, horizon)

        if use_cache:
            cached = self.cache.get(cache_key)
            if cached:
                try:
                    require_post_close_features(cached.get("features_timestamp"), self.config.publication.publication_delay_minutes)
                except StaleFeaturesError:
                    cached = None
            if cached:
                metrics.record_cache(True)
                cached["cached"] = True
                return ForecastResult(**cached)
            metrics.record_cache(False)

        window_df = self.feature_store.get_feature_window(
            symbol, self.config.offline_store.history_rows
        )
        if window_df.empty:
            raise ValueError(f"No feature history found for symbol {symbol}")

        feature_time = self._extract_latest_timestamp(window_df)
        require_post_close_features(feature_time, self.config.publication.publication_delay_minutes)
        inference_df = self._prepare_inference_dataframe(window_df)

        preds = self._predict_quantiles(inference_df)
        result = self._post_process(preds, inference_df, horizon, feature_time)

        self.cache.set(cache_key, result.__dict__)
        return result

    def _extract_latest_timestamp(self, df: pd.DataFrame) -> str | None:
        field = self.config.inference.time_field
        if field in df.columns and not df.empty:
            ts = pd.to_datetime(df[field].iloc[-1])
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            return ts.isoformat()
        return None

    def _prepare_inference_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        self.parity_checker.check_once()
        cfg = self.config.inference
        if len(df) < cfg.max_encoder_length:
            raise ValueError(
                f"Insufficient feature history: need {cfg.max_encoder_length} rows, got {len(df)}"
            )
        df = self.preprocessor.build_prediction_frame(df)

        required = set(
            cfg.time_varying_unknown_reals
            + cfg.time_varying_known_reals
            + cfg.time_varying_known_categoricals
            + cfg.static_categoricals
        )
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required features for inference: {missing}")

        if cfg.target not in df.columns:
            base_feature = self.config.forecast.base_price_feature
            if base_feature not in df.columns:
                raise ValueError("Target field missing and base price feature unavailable")
            df[cfg.target] = df[base_feature]

        return df

    def _predict_quantiles(self, df: pd.DataFrame) -> np.ndarray:
        model = self.model_handle.model
        quantiles = self.config.forecast.quantiles

        try:
            from pytorch_forecasting import TimeSeriesDataSet
        except Exception as exc:
            raise RuntimeError("pytorch-forecasting is required for TFT inference") from exc

        dataset_parameters = getattr(model, "dataset_parameters", None)
        if dataset_parameters:
            # Reuse persisted encoders and the training GroupNormalizer so
            # serving has exactly the same transformation contract as train.
            dataset = TimeSeriesDataSet.from_parameters(
                dataset_parameters,
                df,
                predict=True,
                stop_randomization=True,
            )
        else:
            cfg = self.config.inference
            dataset = TimeSeriesDataSet(
                df,
                time_idx=cfg.time_idx_field,
                target=cfg.target,
                group_ids=[cfg.group_id_field],
                max_encoder_length=cfg.max_encoder_length,
                min_encoder_length=max(1, cfg.max_encoder_length // 2),
                max_prediction_length=cfg.max_prediction_length,
                min_prediction_length=1,
                static_categoricals=cfg.static_categoricals,
                time_varying_known_reals=cfg.time_varying_known_reals,
                time_varying_known_categoricals=cfg.time_varying_known_categoricals,
                time_varying_unknown_reals=cfg.time_varying_unknown_reals,
                time_varying_unknown_categoricals=[],
                add_relative_time_idx=True,
                add_target_scales=True,
                add_encoder_length=True,
                allow_missing_timesteps=True,
            )

        preds = model.predict(dataset, mode="quantiles", return_x=False)
        return self._normalize_prediction_output(preds, quantiles)

    def _to_numpy(self, preds: Any) -> np.ndarray:
        if hasattr(preds, "detach"):
            return preds.detach().cpu().numpy()
        return np.asarray(preds)

    def _normalize_prediction_output(self, preds: Any, quantiles: List[float]) -> np.ndarray:
        if isinstance(preds, pd.DataFrame):
            column_map = {str(col): col for col in preds.columns}
            q_cols: List[str] = []
            for q in quantiles:
                q_key = str(q)
                p_key = f"p{int(q * 100)}"
                if q_key in column_map:
                    q_cols.append(column_map[q_key])
                elif p_key in column_map:
                    q_cols.append(column_map[p_key])
            if q_cols and len(q_cols) == len(quantiles):
                arr = preds[q_cols].to_numpy()
                return arr[None, ...]
            if preds.shape[1] == 1:
                arr = preds.to_numpy().reshape(-1, 1)
                arr = np.repeat(arr, repeats=len(quantiles), axis=1)
                return arr[None, ...]
            arr = preds.to_numpy()
            return arr[None, ...] if arr.ndim == 2 else arr

        arr = self._to_numpy(preds)
        if arr.ndim == 1:
            arr = arr.reshape(-1, 1)
            arr = np.repeat(arr, repeats=len(quantiles), axis=1)
            return arr[None, ...]
        if arr.ndim == 2:
            return arr[None, ...]
        if arr.ndim == 3:
            return arr
        raise ValueError("Unexpected prediction output shape")

    def _post_process(
        self,
        preds: np.ndarray,
        df: pd.DataFrame,
        horizon: int,
        feature_time: str | None,
    ) -> ForecastResult:
        quantiles = self.config.forecast.quantiles
        output_decimals = self.config.forecast.output_decimals
        base_feature = self.config.forecast.base_price_feature

        if preds.ndim == 3:
            if preds.shape[0] != 1:
                raise ValueError("Expected predictions for exactly one symbol window")
            preds = preds[0]

        decoder_length = self.config.inference.max_prediction_length
        if preds.ndim != 2 or preds.shape != (decoder_length, len(quantiles)):
            raise ValueError("Prediction shape must match the full decoder length and quantiles")
        if not 1 <= horizon <= decoder_length:
            raise ValueError("Requested horizon must be within the decoder length")
        if len(df) <= decoder_length:
            raise ValueError("Prediction frame must contain encoder history and the full decoder")
        time_field = self.config.inference.time_field
        # Resolve the complete decoder BEFORE slicing. tail(horizon) would pair
        # the first predictions with timestamps from the end of the decoder.
        decoder_times = pd.to_datetime(df[time_field].iloc[-decoder_length:], utc=True, errors="raise")
        if decoder_times.isna().any():
            raise ValueError("Decoder timestamps must not be missing")
        prediction_times = decoder_times.iloc[:horizon].tolist()
        preds = preds[:horizon]
        quantile_map = {q: i for i, q in enumerate(quantiles)}
        q_values = np.array(quantiles, dtype=float)

        def _closest_idx(target: float) -> int:
            if target in quantile_map:
                return quantile_map[target]
            return int(np.argmin(np.abs(q_values - target)))

        p10_idx = _closest_idx(0.1)
        p50_idx = _closest_idx(0.5)
        p90_idx = _closest_idx(0.9)

        if base_feature not in df.columns:
            raise ValueError(f"Base price feature '{base_feature}' not found in features")
        base_price = float(df[base_feature].iloc[-1])
        p10 = preds[:, p10_idx]
        p50 = preds[:, p50_idx]
        p90 = preds[:, p90_idx]
        if self.config.forecast.target_type == "return":
            p10 = self._returns_to_prices(base_price, p10)
            p50 = self._returns_to_prices(base_price, p50)
            p90 = self._returns_to_prices(base_price, p90)

        predictions = []
        for idx in range(len(p50)):
            predictions.append(
                {
                    "step": idx + 1,
                    "timestamp": prediction_times[idx].isoformat(),
                    "p10": round(float(p10[idx]), output_decimals),
                    "p50": round(float(p50[idx]), output_decimals),
                    "p90": round(float(p90[idx]), output_decimals),
                }
            )

        absolute_change = float(p50[-1] - base_price)
        percent_change = float(absolute_change / base_price) if base_price != 0 else 0.0

        return ForecastResult(
            symbol=str(df[self.config.inference.group_id_field].iloc[-1]),
            horizon=len(p50),
            quantiles=quantiles,
            predictions=predictions,
            base_price=round(base_price, output_decimals),
            absolute_change=round(absolute_change, output_decimals),
            percent_change=round(percent_change, output_decimals),
            model_version=self.model_handle.model_version,
            model_uri=self.model_handle.model_uri,
            features_timestamp=feature_time,
            cached=False,
        )

    def _returns_to_prices(self, base_price: float, returns: np.ndarray) -> np.ndarray:
        method = self.config.forecast.return_to_price_method
        prices = []
        current = base_price
        for r in returns:
            if method == "log":
                current = current * float(np.exp(r))
            else:
                current = current * (1.0 + float(r))
            prices.append(current)
        return np.array(prices)
