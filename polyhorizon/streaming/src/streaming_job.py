from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from polyhorizon.streaming.configs.settings import Config, load_config
from polyhorizon.streaming.src.delta_sinks import (
    ensure_delta_table,
    start_bronze_stream,
    start_dead_letter_stream,
    start_gold_stream,
    upsert_to_delta,
)
from polyhorizon.streaming.src.kafka_io import read_kafka_stream
from polyhorizon.streaming.src.market_schedule import get_market_session
from polyhorizon.streaming.src.metrics import StreamingMetricsListener, StreamingMetricsPublisher
from polyhorizon.streaming.src.quality import split_valid_invalid
from polyhorizon.streaming.src.schemas import (
    build_bronze_schema,
    build_dead_letter_schema,
    build_gold_schema,
    build_raw_schema,
)
from polyhorizon.streaming.src.spark_factory import build_spark_session
from polyhorizon.streaming.src.transformers import transform_ohlcv
from polyhorizon.streaming.src.preflight_check import check_infrastructure
from polyhorizon.streaming.utils.logger import get_logger, log_execution_time


class TradesStreamingJob:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger("StreamingJob")
        self.paths = {
            "bronze": self.config.streaming_storage.bronze,
            "gold": self.config.streaming_storage.gold,
            "cp_bronze": self.config.streaming_storage.cp_bronze,
            "cp_gold": self.config.streaming_storage.cp_gold,
            "dead_letter": self.config.streaming_storage.dead_letter,
            "cp_dead_letter": self.config.streaming_storage.cp_dead_letter,
        }
        self.maintenance_batch_interval = self.config.streaming_storage.maintenance_batch_interval
        self.retention_hours = self.config.streaming_storage.retention_hours
        self.trigger_interval = self.config.streaming_storage.trigger_interval
        self.schema = build_raw_schema(self.config.features)
        self._validate_config()
        self.spark: SparkSession | None = None
        self.dataset_release = None

    def _validate_config(self) -> None:
        missing_paths = [name for name, value in self.paths.items() if not value]
        if missing_paths:
            raise ValueError(f"Missing required storage paths: {missing_paths}")

        if not self.config.kafka_connection.kafka_servers:
            raise ValueError("kafka_servers must not be empty")
        if not self.config.kafka_connection.kafka_topic:
            raise ValueError("kafka_topic must not be empty")
        if not self.config.streaming_storage.trigger_interval:
            raise ValueError("streaming_storage.trigger_interval must not be empty")

        if self.config.features.late_data_strategy == "dead_letter" and (
            not self.paths["dead_letter"] or not self.paths["cp_dead_letter"]
        ):
            raise ValueError("Dead-letter strategy requires dead_letter and cp_dead_letter paths")

    def _ensure_spark(self) -> SparkSession:
        if self.spark is None:
            self.spark = build_spark_session(self.config)
        return self.spark

    def _ensure_tables(self) -> None:
        bronze_schema = build_bronze_schema(self.config.features)
        gold_schema = build_gold_schema(self.config.features)
        dead_letter_schema = build_dead_letter_schema(self.config.features)

        ensure_delta_table(self.spark, self.paths["bronze"], bronze_schema)
        ensure_delta_table(self.spark, self.paths["gold"], gold_schema)
        ensure_delta_table(self.spark, self.paths["dead_letter"], dead_letter_schema)

    def _stop_streams(self) -> None:
        if self.spark is None:
            return
        for query in self.spark.streams.active:
            query.stop()
            query.awaitTermination()

    @log_execution_time
    def run(self, clean_start: bool = False, dry_run: bool = False) -> None:
        try:
            if dry_run:
                self.logger.info("Dry run requested. Config and schema validated. Exiting before Spark start.")
                return

            self._ensure_spark()
            self.spark.conf.set("spark.sql.session.timeZone", "UTC")

            if self.config.monitoring_and_metrics.enable_metrics:
                self.spark.streams.addListener(
                    StreamingMetricsListener(self.config.monitoring_and_metrics)
                )

            if clean_start:
                self.logger.warning("Clean start requested - deleting checkpoints")
                sc = self.spark.sparkContext
                HadoopPath = sc._gateway.jvm.org.apache.hadoop.fs.Path
                FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
                conf = sc._jsc.hadoopConfiguration()

                for key in ("cp_bronze", "cp_gold", "cp_dead_letter"):
                    p = HadoopPath(self.paths[key])
                    fs = FileSystem.get(p.toUri(), conf)
                    if fs.exists(p):
                        fs.delete(p, True)
                        self.logger.info("Deleted checkpoint: %s", self.paths[key])

            self._ensure_tables()

            raw_stream = read_kafka_stream(self.spark, self.config, self.schema)
            valid_df, invalid_df = split_valid_invalid(raw_stream, self.config.features)

            metrics_publisher = StreamingMetricsPublisher(self.config.monitoring_and_metrics)

            def dead_letter_writer(batch_df, batch_id):
                if batch_df.rdd.isEmpty():
                    return
                counts = batch_df.groupBy("dq_reason").count().collect()
                for row in counts:
                    metrics_publisher.push_dead_letter_counts(row["dq_reason"], row["count"])
                batch_df.write.format("delta").mode("append").save(self.paths["dead_letter"])

            dead_letter_query = start_dead_letter_stream(
                invalid_df,
                self.paths["dead_letter"],
                self.paths["cp_dead_letter"],
                self.trigger_interval,
                foreach_batch_fn=dead_letter_writer,
                query_name="dead-letter",
            )

            bronze_query = start_bronze_stream(
                valid_df,
                self.paths["bronze"],
                self.paths["cp_bronze"],
                self.trigger_interval,
                query_name="bronze",
            )

            bronze_df = (
                self.spark.readStream
                .format("delta")
                .option("ignoreChanges", "true")
                .option("ignoreDeletes", "true")
                .load(self.paths["bronze"])
            )

            gold_query = start_gold_stream(
                transform_ohlcv(bronze_df, self.config.features),
                self.paths["cp_gold"],
                self.trigger_interval,
                lambda batch_df, batch_id: upsert_to_delta(
                    self.spark,
                    self.paths["gold"],
                    batch_df,
                    batch_id,
                    self.config.features.symbol_col,
                    self.maintenance_batch_interval,
                    self.retention_hours,
                    [self.paths["bronze"], self.paths["gold"]],
                ),
                query_name="gold",
            )

            self.logger.info(
                "Streaming queries started - Bronze ID: %s, Gold ID: %s, Dead-letter ID: %s",
                bronze_query.id,
                gold_query.id,
                dead_letter_query.id,
            )

            schedule = self.config.market_schedule
            session = get_market_session(datetime.now(ZoneInfo(schedule.timezone)), schedule.calendar, schedule.timezone)
            if not schedule.enabled or session is None:
                raise ValueError("Dataset completion requires an enabled market session schedule")
            _, market_close = session
            shutdown_at = market_close + timedelta(minutes=schedule.shutdown_grace_minutes)
            while datetime.now(ZoneInfo(schedule.timezone)) < shutdown_at:
                if self.spark.streams.awaitAnyTermination(1):
                    raise RuntimeError("Streaming query terminated before scheduled Gold completion")
            # Drain Kafka first; Gold is finalized from this committed Bronze version.
            # A drain/stop failure prevents publishing any completion manifest.
            bronze_query.processAllAvailable()
            bronze_query.stop()
            gold_query.stop()
            dead_letter_query.stop()
            for query in (bronze_query, gold_query, dead_letter_query):
                if query.exception() is not None:
                    raise RuntimeError(f"Query failed during completion: {query.exception()}")
            from polyhorizon.streaming.feature_engineering.release import complete_dataset
            self.dataset_release = complete_dataset(self.spark, self.config, market_close)
        except KeyboardInterrupt:
            self.logger.warning("Streaming job interrupted by user")
            self._stop_streams()
            raise
        except Exception:
            self.logger.critical("Fatal error in streaming job", exc_info=True)
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.logger.info("Shutting down Spark session")
            if self.spark is not None:
                self.spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run PolyHorizon Spark streaming job")
    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Path to streaming config YAML",
    )
    parser.add_argument(
        "--clean-start",
        action="store_true",
        help="Delete checkpoints before starting",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip infrastructure pre-flight checks",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate config and schema without starting Spark streaming queries",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    config = load_config(args.config_path)

    if not args.skip_preflight and not check_infrastructure(config):
        print("\nPre-flight checks FAILED. Fix the issues before starting Spark.")
        sys.exit(1)

    print("\nAll systems GO. Starting Spark job.")
    job = TradesStreamingJob(config=config)

    if args.dry_run:
        job.run(clean_start=args.clean_start, dry_run=True)
        sys.exit(0)

    job.run(clean_start=args.clean_start, dry_run=False)
