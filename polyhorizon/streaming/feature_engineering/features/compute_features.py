from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from polyhorizon.streaming.configs.settings import FeaturesConfig
DEFAULT_ROLLING_FEATURES = ["avg_close", "volatility_close"]
SUPPORTED_FEATURES = {"avg_close", "volatility_close", "returns", "log_returns"}


def _validate_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def resample_and_compute_rolling_features(
    df: DataFrame,
    *,
    features_config: FeaturesConfig,
    rolling_features: Optional[list[str]] = None,
) -> DataFrame:
    if rolling_features is None:
        rolling_features = list(DEFAULT_ROLLING_FEATURES)

    unknown_features = set(rolling_features) - SUPPORTED_FEATURES
    if unknown_features:
        raise ValueError(f"Unsupported rolling features: {sorted(unknown_features)}")

    time_col = features_config.time_col
    symbol_col = features_config.symbol_col
    open_col = features_config.open_col
    high_col = features_config.high_col
    low_col = features_config.low_col
    close_col = features_config.close_col
    volume_col = features_config.volume_col

    _validate_columns(
        df,
        [time_col, symbol_col, open_col, high_col, low_col, close_col, volume_col],
    )

    # Gold timestamps label the END of a bar. Re-bucketing by that timestamp
    # shifts every feature one interval; use its start when already windowed.
    bucket_time = "window_start" if "window_start" in df.columns else time_col
    df_prepared = df.withColumn("ts", F.col(bucket_time).cast("timestamp"))

    resampled = (
        df_prepared.groupBy(F.col(symbol_col), F.window(F.col("ts"), features_config.freq).alias("time_window"))
        .agg(
            F.min_by(F.col(open_col), F.col("ts")).alias(open_col),
            F.max_by(F.col(close_col), F.col("ts")).alias(close_col),
            F.max(F.col(high_col)).alias(high_col),
            F.min(F.col(low_col)).alias(low_col),
            F.sum(F.col(volume_col)).alias(volume_col),
        )
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .drop("time_window")
    )

    if time_col not in {"window_start", "window_end"}:
        resampled = resampled.withColumn(time_col, F.col("window_end"))

    window_spec = (
        Window.partitionBy(symbol_col)
        .orderBy(F.col("window_end").cast("timestamp").cast("long"))
        .rowsBetween(-features_config.lookback_bars, 0)
    )
    ordered_window = Window.partitionBy(symbol_col).orderBy(
        F.col("window_end").cast("timestamp").cast("long")
    )

    features_df = resampled

    if "avg_close" in rolling_features:
        features_df = features_df.withColumn("rolling_avg_close", F.avg(close_col).over(window_spec))

    if "volatility_close" in rolling_features:
        features_df = features_df.withColumn("rolling_volatility_close", F.stddev(close_col).over(window_spec))

    if "returns" in rolling_features or "log_returns" in rolling_features:
        lag_close = F.lag(F.col(close_col)).over(ordered_window)
        returns_col = (F.col(close_col) / lag_close) - F.lit(1)
        features_df = features_df.withColumn("returns", returns_col)
        if "log_returns" in rolling_features:
            features_df = features_df.withColumn(
                "log_returns",
                F.when(lag_close.isNotNull(), F.log(F.col(close_col) / lag_close)),
            )

    base_columns = [symbol_col, open_col, high_col, low_col, close_col, volume_col]
    time_columns = ["window_start", "window_end"]
    if time_col not in time_columns:
        time_columns.append(time_col)

    optional_columns = []
    if "avg_close" in rolling_features:
        optional_columns.append("rolling_avg_close")
    if "volatility_close" in rolling_features:
        optional_columns.append("rolling_volatility_close")
    if "returns" in rolling_features:
        optional_columns.append("returns")
    if "log_returns" in rolling_features:
        optional_columns.append("log_returns")

    final_columns = base_columns + optional_columns + time_columns
    return features_df.select(*final_columns)
