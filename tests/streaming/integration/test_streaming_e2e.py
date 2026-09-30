from __future__ import annotations

import json
import os
import tempfile
import time

import pytest

pyspark = pytest.importorskip("pyspark")

from kafka import KafkaProducer  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

try:
    from delta import configure_spark_with_delta_pip  # noqa: E402
except ImportError:  # pragma: no cover - optional dependency for e2e
    configure_spark_with_delta_pip = None


@pytest.mark.skipif(os.getenv("RUN_STREAMING_E2E") != "1", reason="Set RUN_STREAMING_E2E=1 to run")
def test_streaming_e2e_kafka_to_delta() -> None:
    if configure_spark_with_delta_pip is None:
        pytest.skip("delta-spark not available")

    bootstrap = os.getenv("E2E_KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.getenv("E2E_KAFKA_TOPIC", "polyhorizon-e2e")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    payload = {
        "symbol": "AAPL",
        "price": 100.5,
        "volume": 10.0,
        "timestamp": int(time.time() * 1000),
    }
    producer.send(topic, payload)
    producer.flush()
    producer.close()

    packages = os.getenv(
        "E2E_SPARK_PACKAGES",
        "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("streaming-e2e")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "2")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "delta-output")
        checkpoint_path = os.path.join(tmpdir, "chk")

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING) as value")
        )

        query = (
            df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start(output_path)
        )
        query.awaitTermination(60)

        result = spark.read.format("delta").load(output_path)
        assert result.count() > 0

    spark.stop()
