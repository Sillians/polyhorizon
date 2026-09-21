from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from polyhorizon.streaming.configs.settings import FeaturesConfig


def transform_ohlcv(df: DataFrame, features: FeaturesConfig) -> DataFrame:
    raw_symbol = features.raw_symbol_col
    raw_price = features.raw_price_col
    raw_volume = features.raw_volume_col

    aggregated = (
        df.withWatermark("event_time", features.watermark_delay)
        .groupBy(F.window("event_time", features.freq), F.col(raw_symbol))
        .agg(
            F.min_by(F.col(raw_price), F.col("event_time")).alias(features.open_col),
            F.max(F.col(raw_price)).alias(features.high_col),
            F.min(F.col(raw_price)).alias(features.low_col),
            F.max_by(F.col(raw_price), F.col("event_time")).alias(features.close_col),
            F.sum(F.col(raw_volume)).alias(features.volume_col),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumnRenamed(raw_symbol, features.symbol_col)
    )

    if features.time_col not in aggregated.columns:
        aggregated = aggregated.withColumn(features.time_col, F.col("window_end"))

    return aggregated.select(
        "window_start",
        "window_end",
        features.symbol_col,
        features.open_col,
        features.high_col,
        features.low_col,
        features.close_col,
        features.volume_col,
        *(
            [features.time_col]
            if features.time_col not in {"window_start", "window_end"}
            else []
        ),
    )
