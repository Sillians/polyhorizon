"""Finalize closed Gold windows and produce a version-pinned feature release."""

import os
import pandas as pd
from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from polyhorizon.core.dataset_release import DatasetRelease, validate_feature_frame
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.streaming.src.transformers import transform_ohlcv
from polyhorizon.streaming.feature_engineering.features.compute_features import resample_and_compute_rolling_features


def complete_dataset(spark, config, market_close) -> DatasetRelease:
    storage = config.streaming_storage
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
    gold.write.format("delta").mode("errorifexists").save(gold_path)
    gold = spark.read.format("delta").option("versionAsOf", 0).load(gold_path)
    features = resample_and_compute_rolling_features(gold, features_config=config.features)
    # stddev is undefined for the first bar of each symbol: discard warmup rows
    # explicitly instead of imputing with future observations in PostgreSQL.
    features = features.filter(F.col("rolling_volatility_close").isNotNull())
    features = features.withColumn("event_timestamp", F.col("window_end"))
    frame = features.toPandas()
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
        completed_at=datetime.now(timezone.utc), row_count=len(frame),
        min_event_time=pd.to_datetime(frame.event_timestamp, utc=True).min().to_pydatetime(),
        max_event_time=pd.to_datetime(frame.event_timestamp, utc=True).max().to_pydatetime(),
    )
    # Durable manifest for operator replay, written only after both Delta commits.
    spark.createDataFrame([(release.model_dump_json(),)], ["value"]).coalesce(1).write.mode(
        "errorifexists"
    ).text(f"{storage.feature_output_path.rstrip('/')}/manifests/{dataset_id}")
    return release
