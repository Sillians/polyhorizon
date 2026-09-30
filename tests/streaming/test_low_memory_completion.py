import sys
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from polyhorizon.streaming.feature_engineering.release import bounded_feature_history, collect_bounded, qualify_trades_distributed
from polyhorizon.streaming.feature_engineering.features.compute_features import resample_and_compute_rolling_features
from polyhorizon.streaming.configs.settings import FeaturesConfig
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.core.session_qualification import qualify_trades


@pytest.fixture(scope="module")
def spark():
    with pytest.MonkeyPatch.context() as env:
        env.setenv("PYSPARK_PYTHON", sys.executable)
        session = (SparkSession.builder.master("local[1]").appName("bounded-completion-test")
                   .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8").getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        yield session
        session.stop()


def test_rolling_history_matches_unbounded_result(spark):
    times = pd.date_range("2026-09-01T00:00Z", periods=1000, freq="30min")
    rows = [(s, t.to_pydatetime(), (t + pd.Timedelta(minutes=30)).to_pydatetime(), float(i + 1))
            for s in PRODUCT_SYMBOLS for i, t in enumerate(times)]
    gold = spark.createDataFrame(rows, ["symbol", "window_start", "window_end", "close"])
    for col in ["open", "high", "low", "total_volume"]:
        gold = gold.withColumn(col, F.col("close"))
    opening = times[-13].to_pydatetime()
    close = (times[-1] + pd.Timedelta(minutes=30)).to_pydatetime()
    small = bounded_feature_history(gold, opening, close, 60)
    assert small.count() == 3 * (60 + 13)
    cfg = FeaturesConfig(freq="30 minutes", lookback_bars=60, time_col="window_end", symbol_col="symbol",
                         open_col="open", high_col="high", low_col="low", close_col="close", volume_col="total_volume")
    def frame(df):
        return (resample_and_compute_rolling_features(df, features_config=cfg)
            .filter(F.col("window_start") >= F.lit(opening)).orderBy("symbol", "window_start").toPandas())
    pd.testing.assert_frame_equal(frame(small), frame(gold))


def test_tick_qualification_matches_pandas_without_collecting_ticks(spark):
    opening, close = pd.Timestamp("2026-09-29T13:30Z"), pd.Timestamp("2026-09-29T20:00Z")
    rows = [(s, t.to_pydatetime(), 10., 1.) for s in PRODUCT_SYMBOLS
            for t in pd.date_range(opening, close, freq="1min", inclusive="left")]
    frame = spark.createDataFrame(rows, ["symbol", "event_time", "price", "volume"])
    expected = qualify_trades(frame.toPandas(), close)
    assert qualify_trades_distributed(frame, opening, close) == expected
    with pytest.raises(ValueError, match="Invalid"):
        qualify_trades_distributed(frame.withColumn("price", F.lit(float("nan"))), opening, close)
    with pytest.raises(ValueError, match="gap"):
        qualify_trades_distributed(frame.filter(F.col("event_time") >= F.lit(opening + pd.Timedelta(minutes=3))), opening, close)


def test_driver_collection_bound(spark):
    with pytest.raises(ValueError, match="bounded"):
        collect_bounded(spark.range(5), 2)
