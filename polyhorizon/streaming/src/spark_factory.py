from __future__ import annotations

from pyspark.sql import SparkSession

from polyhorizon.streaming.configs.settings import Config
from polyhorizon.streaming.utils.logger import get_logger


def build_spark_session(config: Config, app_name: str = "Polyhorizon_streaming_job") -> SparkSession:
    logger = get_logger("SparkFactory")
    packages = config.spark_connection.packages
    if not packages:
        raise ValueError("spark_connection.packages must not be empty")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
    )

    if config.spark_connection.spark_master:
        builder = builder.master(config.spark_connection.spark_master)

    base_conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.endpoint": str(config.bucket_details.seaweedfs_s3_endpoint),
        "spark.hadoop.fs.s3a.access.key": config.bucket_details.seaweedfs_access_key.get_secret_value(),
        "spark.hadoop.fs.s3a.secret.key": config.bucket_details.seaweedfs_secret_key.get_secret_value(),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    resolved_conf = {**base_conf, **config.spark_connection.conf}

    for key, value in resolved_conf.items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.spark_connection.spark_log_level)
    logger.info("Spark session initialized")
    return spark
