from __future__ import annotations

import pytest
import sys

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from polyhorizon.streaming.configs.settings import FeaturesConfig  # noqa: E402
from polyhorizon.streaming.feature_engineering.features.compute_features import (  # noqa: E402
    resample_and_compute_rolling_features,
)  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    with pytest.MonkeyPatch.context() as environment:
        environment.setenv("PYSPARK_PYTHON", sys.executable)
        spark_session = (
            SparkSession.builder.master("local[1]")
            .appName("feature-engineering-tests")
            .getOrCreate()
        )
        yield spark_session
        spark_session.stop()


@pytest.fixture()
def features_config() -> FeaturesConfig:
    return FeaturesConfig(
        freq="30 minutes",
        lookback_bars=2,
        watermark_delay="2 minutes",
        raw_symbol_col="symbol",
        raw_price_col="price",
        raw_volume_col="volume",
        raw_timestamp_col="timestamp",
        time_col="event_time",
        symbol_col="symbol",
        open_col="open",
        high_col="high",
        low_col="low",
        close_col="close",
        volume_col="volume",
    )


def _sample_df(spark: SparkSession):
    rows = [
        ("AAPL", 100.0, 101.0, 99.0, 100.5, 200.0, "2024-01-01 10:00:00"),
        ("AAPL", 101.0, 102.0, 100.0, 101.5, 150.0, "2024-01-01 10:10:00"),
        ("AAPL", 102.0, 103.0, 101.0, 102.5, 120.0, "2024-01-01 10:40:00"),
    ]
    df = spark.createDataFrame(
        rows,
        schema=["symbol", "open", "high", "low", "close", "volume", "event_time"],
    )
    return df.withColumn("event_time", F.col("event_time").cast("timestamp"))


def test_compute_features_columns(spark: SparkSession, features_config: FeaturesConfig) -> None:
    df = _sample_df(spark)

    result = resample_and_compute_rolling_features(
        df,
        features_config=features_config,
        rolling_features=["avg_close", "volatility_close", "returns"],
    )

    assert "rolling_avg_close" in result.columns
    assert "rolling_volatility_close" in result.columns
    assert "returns" in result.columns
    assert "window_start" in result.columns
    assert "window_end" in result.columns


def test_missing_required_columns_raises(spark: SparkSession, features_config: FeaturesConfig) -> None:
    df = _sample_df(spark).drop("close")

    with pytest.raises(ValueError, match="Missing required columns"):
        resample_and_compute_rolling_features(
            df,
            features_config=features_config,
        )


def test_unknown_feature_raises(spark: SparkSession, features_config: FeaturesConfig) -> None:
    df = _sample_df(spark)

    with pytest.raises(ValueError, match="Unsupported rolling features"):
        resample_and_compute_rolling_features(
            df,
            features_config=features_config,
            rolling_features=["unknown_feature"],
        )


def test_gold_window_end_does_not_shift_feature_bar(spark, features_config):
    gold = _sample_df(spark).limit(1).withColumn("window_start", F.to_timestamp(F.lit("2024-01-01 10:00:00")))
    gold = gold.withColumn("event_time", F.to_timestamp(F.lit("2024-01-01 10:30:00")))
    row = resample_and_compute_rolling_features(gold, features_config=features_config).first()
    assert str(row.window_start) == "2024-01-01 10:00:00"
    assert str(row.window_end) == "2024-01-01 10:30:00"


@pytest.mark.parametrize("session_timezone", ["UTC", "America/New_York"])
def test_kafka_reader_preserves_epoch_milliseconds(spark, features_config, session_timezone):
    import json
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from polyhorizon.streaming.configs.settings import KafkaConnectionConfig
    from polyhorizon.streaming.src.kafka_io import read_kafka_stream
    from polyhorizon.streaming.src.schemas import build_raw_schema

    epoch_ms = 1790000000123
    payload = json.dumps({"symbol": "AAPL", "price": 100.0, "volume": 10.0, "timestamp": epoch_ms})
    reader = MagicMock()
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = spark.createDataFrame([(payload,)], ["value"])
    config = SimpleNamespace(features=features_config,
        kafka_connection=KafkaConnectionConfig(kafka_servers=["unused:9092"], kafka_topic="test",
            tickers_csv="unused", consumer_group_id="test"))
    previous = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", session_timezone)
        frame = read_kafka_stream(SimpleNamespace(readStream=reader), config, build_raw_schema(features_config))
        assert frame.select(F.unix_millis("event_time")).first()[0] == epoch_ms
    finally:
        spark.conf.set("spark.sql.session.timeZone", previous)
