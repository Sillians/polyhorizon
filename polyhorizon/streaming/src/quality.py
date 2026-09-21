from __future__ import annotations

from typing import List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from polyhorizon.streaming.configs.settings import FeaturesConfig


def _build_reason_column(conditions: List[Tuple[Column, str]]) -> Column:
    reason_cols = [F.when(condition, F.lit(reason)) for condition, reason in conditions]
    return F.concat_ws(";", *reason_cols)


def split_valid_invalid(
    df: DataFrame,
    features: FeaturesConfig,
) -> tuple[DataFrame, DataFrame]:
    symbol_col = features.raw_symbol_col
    price_col = features.raw_price_col
    volume_col = features.raw_volume_col
    ts_col = features.raw_timestamp_col

    conditions: List[Tuple[Column, str]] = [
        (F.col(symbol_col).isNull() | (F.length(F.trim(F.col(symbol_col))) == 0), "missing_symbol"),
        (F.col(price_col).isNull() | (F.col(price_col) <= 0), "invalid_price"),
        (F.col(volume_col).isNull() | (F.col(volume_col) < 0), "invalid_volume"),
        (F.col(ts_col).isNull() | (F.col(ts_col) <= 0), "invalid_timestamp"),
        (F.col("event_time").isNull(), "invalid_event_time"),
    ]

    late_threshold = features.late_data_threshold or features.watermark_delay
    late_condition = None
    if late_threshold:
        late_condition = F.col("event_time") < (
            F.current_timestamp() - F.expr(f"INTERVAL {late_threshold}")
        )

    if late_condition is not None and features.late_data_strategy == "dead_letter":
        conditions.append((late_condition, "late_event"))

    invalid_expr = None
    for condition, _ in conditions:
        invalid_expr = condition if invalid_expr is None else (invalid_expr | condition)

    invalid_expr = invalid_expr if invalid_expr is not None else F.lit(False)

    invalid_df = (
        df.where(invalid_expr)
        .withColumn("dq_reason", _build_reason_column(conditions))
        .withColumn("ingested_at", F.current_timestamp())
    )

    valid_df = df.where(~invalid_expr)

    if late_condition is not None and features.late_data_strategy == "drop":
        valid_df = valid_df.where(~late_condition)

    return valid_df, invalid_df
