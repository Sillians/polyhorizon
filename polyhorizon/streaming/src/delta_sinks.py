from __future__ import annotations

from typing import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from polyhorizon.streaming.utils.logger import get_logger


def ensure_delta_table(spark: SparkSession, path: str, schema) -> None:
    logger = get_logger("DeltaTable")
    if DeltaTable.isDeltaTable(spark, path):
        logger.info("Delta table already exists at %s", path)
        return

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").mode("overwrite").save(path)
    logger.info("Created empty Delta table at %s", path)


def run_maintenance(spark: SparkSession, path: str, retention_hours: int) -> None:
    logger = get_logger("DeltaMaintenance")
    try:
        dt = DeltaTable.forPath(spark, path)
        dt.optimize().executeCompaction()
        dt.vacuum(retention_hours)
        logger.info("Maintenance completed for %s", path)
    except Exception:
        logger.error("Maintenance failed for %s", path, exc_info=True)


def start_bronze_stream(
    df: DataFrame,
    path: str,
    checkpoint: str,
    trigger_interval: str,
    query_name: str | None = None,
):
    writer = (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "false")
        .option("optimizeWrite", "true")
        .option("autoOptimize.optimizeWrite", "true")
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
    )
    if query_name:
        writer = writer.queryName(query_name)
    return writer.start(path)


def start_dead_letter_stream(
    df: DataFrame,
    path: str,
    checkpoint: str,
    trigger_interval: str,
    foreach_batch_fn=None,
    query_name: str | None = None,
):
    writer = df.writeStream.option("checkpointLocation", checkpoint).trigger(
        processingTime=trigger_interval
    )
    if query_name:
        writer = writer.queryName(query_name)

    if foreach_batch_fn is not None:
        return writer.foreachBatch(foreach_batch_fn).start()

    return writer.format("delta").outputMode("append").start(path)


def start_gold_stream(
    df: DataFrame,
    checkpoint: str,
    trigger_interval: str,
    foreach_batch_fn,
    query_name: str | None = None,
):
    writer = (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=trigger_interval)
    )
    if query_name:
        writer = writer.queryName(query_name)
    return writer.start()


def upsert_to_delta(
    spark: SparkSession,
    path: str,
    micro_batch_df: DataFrame,
    batch_id: int,
    symbol_col: str,
    maintenance_batch_interval: int,
    retention_hours: int,
    maintenance_paths: Iterable[str],
) -> None:
    logger = get_logger("DeltaUpsert")
    if micro_batch_df.rdd.isEmpty():
        logger.info("Batch %s: Empty micro-batch, skipping upsert", batch_id)
        return

    optimized_batch = micro_batch_df.repartition(symbol_col).cache()

    gold_table = DeltaTable.forPath(spark, path)
    merge_condition = f"target.window_start = updates.window_start AND target.{symbol_col} = updates.{symbol_col}"

    gold_table.alias("target").merge(
        optimized_batch.alias("updates"), merge_condition
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    optimized_batch.unpersist()
    logger.info("Batch %s: Upsert completed", batch_id)

    if batch_id > 0 and batch_id % maintenance_batch_interval == 0:
        for maintenance_path in maintenance_paths:
            run_maintenance(spark, maintenance_path, retention_hours)
