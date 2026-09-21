from __future__ import annotations

from pyspark.sql import SparkSession

from polyhorizon.streaming.configs.settings import Config


def get_spark_session(app_name: str, config: Config) -> SparkSession:
    packages = config.spark_connection.packages
    if not packages:
        raise ValueError("spark_connection.packages must not be empty")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", str(config.bucket_details.seaweedfs_s3_endpoint))
        .config(
            "spark.hadoop.fs.s3a.access.key",
            config.bucket_details.seaweedfs_access_key.get_secret_value(),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            config.bucket_details.seaweedfs_secret_key.get_secret_value(),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    if config.spark_connection.spark_master:
        builder = builder.master(config.spark_connection.spark_master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.spark_connection.spark_log_level)
    return spark
