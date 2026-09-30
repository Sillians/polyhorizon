"""Isolated image smoke test: local temporary Delta data, no network or real feeds."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from pyspark.sql import SparkSession
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.streaming.configs.settings import FeaturesConfig
from polyhorizon.streaming.feature_engineering.release import complete_dataset
from polyhorizon.streaming.src.delta_sinks import ensure_delta_table, upsert_to_delta
from polyhorizon.streaming.src.schemas import build_gold_schema
from polyhorizon.streaming.src.transformers import transform_ohlcv


def main():
    spark = (SparkSession.builder.master("local[1]").appName("isolated-low-memory-smoke")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
        .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "128")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        with TemporaryDirectory(prefix="polyhorizon-smoke-") as directory:
            root = Path(directory).as_uri()
            cfg = SimpleNamespace(streaming_storage=SimpleNamespace(bronze=root + "/bronze",
                gold=root + "/gold", feature_output_path=root + "/features"),
                features=FeaturesConfig(freq="30 minutes", lookback_bars=60, time_col="window_end",
                    symbol_col="symbol", open_col="open", high_col="high", low_col="low",
                    close_col="close", volume_col="total_volume"))
            rows = [(s, float(100 + i // 30), 1., datetime(2026, 9, day, 13, 30, tzinfo=timezone.utc) + timedelta(minutes=i))
                    for day in (28, 29) for s in PRODUCT_SYMBOLS for i in range(390)]
            spark.createDataFrame(rows, ["symbol", "price", "volume", "event_time"]).coalesce(1).write.format("delta").save(cfg.streaming_storage.bronze)
            first = complete_dataset(spark, cfg, datetime(2026, 9, 28, 20, tzinfo=timezone.utc))
            second = complete_dataset(spark, cfg, datetime(2026, 9, 29, 20, tzinfo=timezone.utc))
            assert first.row_count == 36 and second.row_count == 39
            assert second.session_qualification["trade_coverage"]["NVDA"]["trade_count"] == 390
            assert spark.read.format("delta").load(second.gold_path).count() == 39
            assert spark.read.format("delta").load(first.feature_path).count() == 36
            target, checkpoint = root + "/gold-p8-v1", root + "/cp-gold-p8-v1"
            ensure_delta_table(spark, target, build_gold_schema(cfg.features))
            def replay():
                source = spark.readStream.format("delta").option("maxFilesPerTrigger", "32").load(cfg.streaming_storage.bronze)
                query = (transform_ohlcv(source, cfg.features).writeStream
                    .foreachBatch(lambda df, batch: upsert_to_delta(spark, target, df, batch, "symbol", 1000000, 168, []))
                    .option("checkpointLocation", checkpoint).trigger(availableNow=True).start())
                if not query.awaitTermination(180):
                    query.stop()
                    raise RuntimeError("Checkpoint replay timed out")
            replay()
            count = spark.read.format("delta").load(target).count()
            assert count > 0
            replay()
            assert spark.read.format("delta").load(target).count() == count
            assert spark.read.format("delta").load(cfg.streaming_storage.bronze).count() == len(rows)
            offsets = Path(directory, "cp-gold-p8-v1", "offsets")
            logs = [p for p in offsets.iterdir() if p.name.isdigit()]
            assert any('"spark.sql.shuffle.partitions":"8"' in p.read_text() for p in logs)
            print(json.dumps({"status": "passed", "session_rows": [first.row_count, second.row_count],
                              "shuffle_partitions": 8, "bronze_rows_unchanged": len(rows), "replay_restart_idempotent": True}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
