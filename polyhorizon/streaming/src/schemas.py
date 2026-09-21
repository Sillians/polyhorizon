from __future__ import annotations

from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from polyhorizon.streaming.configs.settings import FeaturesConfig


def build_raw_schema(features: FeaturesConfig) -> StructType:
    return StructType(
        [
            StructField(features.raw_symbol_col, StringType(), True),
            StructField(features.raw_price_col, DoubleType(), True),
            StructField(features.raw_volume_col, DoubleType(), True),
            StructField(features.raw_timestamp_col, LongType(), True),
        ]
    )


def build_bronze_schema(features: FeaturesConfig) -> StructType:
    return StructType(
        [
            StructField(features.raw_symbol_col, StringType(), True),
            StructField(features.raw_price_col, DoubleType(), True),
            StructField(features.raw_volume_col, DoubleType(), True),
            StructField(features.raw_timestamp_col, LongType(), True),
            StructField("event_time", TimestampType(), True),
        ]
    )


def build_gold_schema(features: FeaturesConfig) -> StructType:
    fields = [
        StructField("window_start", TimestampType(), True),
        StructField("window_end", TimestampType(), True),
        StructField(features.symbol_col, StringType(), True),
        StructField(features.open_col, DoubleType(), True),
        StructField(features.high_col, DoubleType(), True),
        StructField(features.low_col, DoubleType(), True),
        StructField(features.close_col, DoubleType(), True),
        StructField(features.volume_col, DoubleType(), True),
    ]

    if features.time_col not in {"window_start", "window_end"}:
        fields.append(StructField(features.time_col, TimestampType(), True))

    return StructType(fields)


def build_dead_letter_schema(features: FeaturesConfig) -> StructType:
    return StructType(
        [
            StructField(features.raw_symbol_col, StringType(), True),
            StructField(features.raw_price_col, DoubleType(), True),
            StructField(features.raw_volume_col, DoubleType(), True),
            StructField(features.raw_timestamp_col, LongType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("dq_reason", StringType(), True),
            StructField("ingested_at", TimestampType(), True),
        ]
    )
