"""Finalize closed Gold windows and produce a version-pinned feature release."""

import os
import pandas as pd
from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from polyhorizon.core.dataset_release import DatasetRelease, validate_feature_frame
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.core.session_qualification import qualify_session
from polyhorizon.streaming.src.market_schedule import get_market_session
from zoneinfo import ZoneInfo
from polyhorizon.streaming.src.transformers import transform_ohlcv
from polyhorizon.streaming.feature_engineering.features.compute_features import resample_and_compute_rolling_features


def collect_bounded(frame, maximum):
    """Enforce the bound before sending rows through Arrow to the driver."""
    bounded = frame.limit(maximum + 1)
    if bounded.count() > maximum:
        raise ValueError("Completion frame exceeds bounded driver capacity")
    return bounded.toPandas()


def bounded_feature_history(gold, opening, close, lookback, symbol="symbol"):
    if not 0 < lookback <= 10000:
        raise ValueError("Unsupported bounded feature lookback")
    past = gold.filter(F.col("window_end") <= F.lit(opening))
    order = Window.partitionBy(symbol).orderBy(F.col("window_end").desc())
    past = past.withColumn("_history_rank", F.row_number().over(order)).filter(
        F.col("_history_rank") <= lookback).drop("_history_rank")
    current = gold.filter((F.col("window_start") >= F.lit(opening)) &
                          (F.col("window_end") <= F.lit(close)))
    return past.unionByName(current)


def qualify_trades_distributed(trades, opening, close):
    """Equivalent to qualify_trades without collecting the complete tick history."""
    order = Window.partitionBy("symbol").orderBy("event_time")
    millis = F.unix_millis("event_time")
    rows = trades.withColumn("_gap", millis - F.lag(millis).over(order))
    invalid = F.lit(False)
    for name in ("price", "volume"):
        col = F.col(name).cast("double")
        invalid = invalid | col.isNull() | F.isnan(col) | (col <= 0) | (F.abs(col) == float("inf"))
    metrics = rows.groupBy("symbol").agg(F.count("*").alias("n"),
        F.min(millis).alias("first"), F.max(millis).alias("last"),
        F.max("_gap").alias("gap"), F.max(invalid.cast("int")).alias("invalid")).limit(len(PRODUCT_SYMBOLS) + 1).collect()
    if {r.symbol for r in metrics} != set(PRODUCT_SYMBOLS):
        raise ValueError("Trade coverage requires every product symbol")
    evidence = {}
    for row in metrics:
        if row.invalid:
            raise ValueError("Invalid session trade price or volume")
        # Spark Row timestamps otherwise use the driver's host timezone.
        first = pd.Timestamp(row.first, unit="ms", tz="UTC")
        last = pd.Timestamp(row.last, unit="ms", tz="UTC")
        gap = max(float((first - pd.Timestamp(opening)).total_seconds()),
                  float((pd.Timestamp(close) - last).total_seconds()), float(row.gap or 0) / 1000)
        # Millisecond source timestamps: avoid floating-point epoch subtraction noise.
        gap = round(gap, 3)
        if gap > 120:
            raise ValueError(f"Trade coverage gap exceeds 120 seconds for {row.symbol}")
        evidence[row.symbol] = {"trade_count": row.n, "maximum_gap_seconds": gap}
    return evidence


def complete_dataset(spark, config, market_close) -> DatasetRelease:
    storage = config.streaming_storage
    session = get_market_session(market_close.astimezone(ZoneInfo("America/New_York")), "NYSE", "America/New_York")
    if session is None or session[1] != market_close:
        raise ValueError("Close does not match an NYSE session")
    session_start = session[0]
    session_bound = len(PRODUCT_SYMBOLS) * int((market_close - session_start).total_seconds() // 1800)
    dataset_id = uuid4()
    # Independent tables prevent concurrent live Gold updates from changing a release.
    gold_path = f"{storage.gold.rstrip('/')}-completed/{dataset_id}"
    feature_path = f"{storage.feature_output_path.rstrip('/')}/datasets/{dataset_id}"
    bronze_version = int(DeltaTable.forPath(spark, storage.bronze).history(1).first()["version"])
    bronze = spark.read.format("delta").option("versionAsOf", bronze_version).load(storage.bronze)
    bronze = bronze.filter(F.col(config.features.raw_symbol_col).isin(list(PRODUCT_SYMBOLS)))
    bronze = bronze.filter(F.col("event_time") < F.lit(market_close))
    # Batch recomputation flushes final bars that streaming watermarks cannot emit
    # without a later event. This is complete relative to the drained Bronze snapshot.
    gold = transform_ohlcv(bronze, config.features).filter(F.col("window_end") <= F.lit(market_close))
    # Historical aggregation remains distributed; only the current session is
    # written/collected for qualification. Rolling computation uses the last N
    # historical bars per symbol, not an unbounded historical pandas frame.
    current_gold = gold.filter(F.col("window_start") >= F.lit(session_start))
    qualification = qualify_session(collect_bounded(current_gold, session_bound), market_close)
    current_gold.write.format("delta").mode("errorifexists").save(gold_path)
    session_trades = bronze.filter(F.col("event_time") >= F.lit(session_start)).select(
        F.col(config.features.raw_symbol_col).alias("symbol"),
        F.col(config.features.raw_price_col).alias("price"),
        F.col(config.features.raw_volume_col).alias("volume"), "event_time")
    qualification["trade_coverage"] = qualify_trades_distributed(session_trades, session_start, market_close)
    history = bounded_feature_history(gold, session_start, market_close,
                                      config.features.lookback_bars, config.features.symbol_col)
    features = resample_and_compute_rolling_features(history, features_config=config.features)
    # Publish only this session. Historical warmup rows are not a new release.
    features = features.filter(F.col("window_start") >= F.lit(session_start))
    # stddev is undefined for the first bar of each symbol: discard warmup rows
    # explicitly instead of imputing with future observations in PostgreSQL.
    features = features.filter(F.col("rolling_volatility_close").isNotNull())
    features = features.withColumn("event_timestamp", F.col("window_end"))
    frame = collect_bounded(features, session_bound)
    validate_feature_frame(frame)
    last_bars = pd.to_datetime(frame.event_timestamp, utc=True).groupby(frame.symbol).max()
    if not (last_bars == market_close).all():
        raise ValueError("Gold completion requires the closing bar for every product symbol")
    features = features.withColumn("total_volume", F.col("total_volume").cast("long"))
    features.write.format("delta").mode("errorifexists").save(feature_path)
    release = DatasetRelease(
        dataset_id=dataset_id, bronze_path=storage.bronze, bronze_version=bronze_version,
        gold_path=gold_path, gold_version=0, feature_path=feature_path, feature_version=0,
        frequency=config.features.freq, lookback_bars=config.features.lookback_bars,
        image_tag=os.getenv("IMAGE_TAG", "local"), market_close=market_close,
        session_qualification=qualification,
        completed_at=datetime.now(timezone.utc), row_count=len(frame),
        min_event_time=pd.to_datetime(frame.event_timestamp, utc=True).min().to_pydatetime(),
        max_event_time=pd.to_datetime(frame.event_timestamp, utc=True).max().to_pydatetime(),
    )
    # Durable manifest for operator replay, written only after both Delta commits.
    spark.createDataFrame([(release.model_dump_json(),)], ["value"]).coalesce(1).write.mode(
        "errorifexists"
    ).text(f"{storage.feature_output_path.rstrip('/')}/manifests/{dataset_id}")
    return release
