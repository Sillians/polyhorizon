from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from polyhorizon.streaming.configs.settings import Config
from polyhorizon.streaming.utils.logger import get_logger


def read_kafka_stream(spark: SparkSession, config: Config, schema) -> DataFrame:
    logger = get_logger("KafkaReader")
    logger.info("Starting Kafka source stream")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(config.kafka_connection.kafka_servers))
        .option("subscribe", config.kafka_connection.kafka_topic)
        .option("startingOffsets", config.kafka_connection.starting_offsets)
        .option("failOnDataLoss", str(config.kafka_connection.fail_on_data_loss).lower())
        .option("maxOffsetsPerTrigger", str(config.kafka_connection.max_offsets_per_trigger))
        .option("kafka.fetch.min.bytes", str(config.kafka_connection.fetch_min_bytes))
        .option("kafka.fetch.max.wait.ms", str(config.kafka_connection.fetch_max_wait_ms))
        .option("kafka.max.partition.fetch.bytes", str(config.kafka_connection.max_partition_fetch_bytes))
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn(
            "event_time",
            # Finnhub supplies epoch milliseconds. Converting through a formatted
            # seconds string drops subsecond ordering (and depends on session TZ).
            F.timestamp_millis(F.col(config.features.raw_timestamp_col)),
        )
    )

    return df
