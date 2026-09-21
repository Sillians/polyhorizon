from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from polyhorizon.streaming.configs.settings import Config, load_config
from polyhorizon.streaming.src.spark_factory import build_spark_session
from polyhorizon.streaming.utils.logger import get_logger

logger = get_logger("StreamingDebug")


def create_spark(config: Config) -> SparkSession:
    """Create a SparkSession for quick connectivity/debug checks."""
    return build_spark_session(config, app_name="polyhorizon-streaming-debug")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark connectivity check")
    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Path to streaming config YAML",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = load_config(args.config_path)
    spark = create_spark(config)

    sc = spark.sparkContext
    logger.info("Master: %s", sc.master)
    logger.info("App ID: %s", sc.applicationId)
    logger.info("Executor memory: %s", sc.getConf().get("spark.executor.memory"))
    logger.info("S3 endpoint: %s", sc.getConf().get("spark.hadoop.fs.s3a.endpoint"))

    spark.stop()


if __name__ == "__main__":
    main()
