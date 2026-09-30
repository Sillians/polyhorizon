"""Bounded test flows; never deploy these to the application's Prefect server."""
import asyncio
import hashlib
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse
from uuid import UUID, uuid4

from prefect import flow

ENDPOINT = "http://127.0.0.1:18333"
BROKER = "127.0.0.1:19092"


def fixture_config(run_id):
    from polyhorizon.features.configs.settings import ConnectionParams

    run_id = UUID(run_id).hex
    root = Path(os.environ["ORCHESTRATION_REHEARSAL_ROOT"])
    repo = root / run_id / "feast"
    return SimpleNamespace(
        paths=SimpleNamespace(feast_repo_path=str(repo)),
        bucket_details=SimpleNamespace(feature_output_path=f"s3a://rehearsal-{run_id}/features",
            seaweedfs_s3_endpoint=ENDPOINT, seaweedfs_access_key="test", seaweedfs_secret_key="test"),
        connection_parameters=ConnectionParams(host="127.0.0.1", port=15432, database="publication_test",
            user="postgres", password="publication-test-only", connect_timeout=5),
        data=SimpleNamespace(db_schema=f"rehearsal_{run_id}", offline_table_name="ohlcv",
            snapshot_table_name="snapshot", snapshot_days=30, enable_partitioning=False),
        data_contract=SimpleNamespace(required_columns=["symbol", "event_timestamp", "close", "total_volume"],
            column_types={"close": "float", "total_volume": "int"}, allow_extra_columns=True),
        feast_parameters=SimpleNamespace(lookback_days=30, max_backfill_days=90),
        project=SimpleNamespace(feature_view="stock_ohlcv_features"),
    )


def prepare_feast_repo(config, run_id):
    import yaml

    repo = Path(config.paths.feast_repo_path)
    repo.mkdir(parents=True)
    settings = {
        "project": f"rehearsal_{run_id}", "provider": "local",
        "registry": str(repo / "registry.db"), "entity_key_serialization_version": 3,
        "offline_store": config.connection_parameters.model_dump(exclude={"connect_timeout"})
            | {"type": "postgres", "db_schema": config.data.db_schema},
        "online_store": {"type": "redis", "connection_string": "127.0.0.1:16379"},
    }
    # Generated runtime fixtures contain only public test credentials.
    (repo / "feature_store.yaml").write_text(yaml.safe_dump(settings))
    shutil.copyfile(Path(__file__).with_name("feast_definitions.py"), repo / "definitions.py")


@flow(name="feature-store-flow", retries=0)
def publish_rehearsal(dataset_release: dict):
    """Test config adapter around the real publisher, including real feast apply."""
    from polyhorizon.core.dataset_release import DatasetRelease
    from polyhorizon.features.src.publish_dataset import publish_dataset

    release = DatasetRelease.model_validate(dataset_release)
    bucket = urlparse(release.feature_path).netloc
    if not bucket.startswith("rehearsal-"):
        raise ValueError("Only disposable rehearsal buckets are allowed")
    config = fixture_config(bucket.removeprefix("rehearsal-"))
    return publish_dataset(dataset_release, apply_changes=True, config=config)


async def produce_fixture(s3, bucket, topic, close):
    import websockets
    from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
    from polyhorizon.ingestion.finnhub_producer.producer import FinnhubProducer, ProducerRuntimeConfig

    payload = ("symbol\n" + "\n".join(PRODUCT_SYMBOLS) + "\n").encode()
    s3.put_object(Bucket=bucket, Key="universe/companies.csv", Body=payload)
    pointer = {"schema_version": "1.0", "run_id": bucket,
        "fetched_at": datetime.now(timezone.utc).isoformat(), "symbols": list(PRODUCT_SYMBOLS),
        "constituent_count": len(PRODUCT_SYMBOLS), "artifacts": {"companies.csv": {
            "key": "universe/companies.csv", "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}}
    s3.put_object(Bucket=bucket, Key="universe/current.json", Body=json.dumps(pointer).encode())
    trades = [{"s": symbol, "p": 100.0 + i, "v": 10.0,
               "t": int((close - timedelta(minutes=75 - i * 30)).timestamp() * 1000) + 123}
              for symbol in PRODUCT_SYMBOLS for i in range(3)]
    trades.append({"s": PRODUCT_SYMBOLS[0], "p": -5.0, "v": 10.0, "t": trades[-1]["t"]})

    async def feed(websocket):
        subscriptions = [json.loads(await websocket.recv()) for _ in PRODUCT_SYMBOLS]
        assert {item["symbol"] for item in subscriptions} == set(PRODUCT_SYMBOLS)
        await websocket.send(json.dumps({"type": "trade", "data": trades}))
        await websocket.wait_closed()

    async with websockets.serve(feed, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        runtime = ProducerRuntimeConfig(
            finnhub_token="test", finnhub_ws_url=f"ws://127.0.0.1:{port}",
            max_symbols_per_connection=3, max_total_symbols=3, subscription_delay=0,
            connection_retry_delay=1, rate_limit_delay=1, max_retries=2,
            ping_interval=20, ping_timeout=20, close_timeout=2, max_message_size=1048576,
            kafka_servers=[BROKER], kafka_topic=topic, producer_acks="all", producer_retries=3,
            producer_max_in_flight=1, tickers_s3_path=f"s3://{bucket}/universe/companies.csv",
            universe_current_key="universe/current.json", universe_max_age_hours=30,
            seaweed_endpoint=ENDPOINT, seaweed_access_key="test", seaweed_secret_key="test", metrics_interval=50,
        )
        producer = FinnhubProducer(settings=SimpleNamespace(), runtime_config=runtime)
        task = asyncio.create_task(producer.start())
        try:
            async with asyncio.timeout(30):
                while producer.metrics["messages_sent"] < len(trades):
                    if task.done():
                        await task
                        raise RuntimeError("Producer exited before sending the fixture")
                    await asyncio.sleep(0.1)
            assert producer.subscribed_symbols == set(PRODUCT_SYMBOLS)
        finally:
            await producer.stop()
            await asyncio.wait_for(task, timeout=10)
    return len(trades)


def consume_fixture(spark, config, bucket):
    from polyhorizon.streaming.src.kafka_io import read_kafka_stream
    from polyhorizon.streaming.src.quality import split_valid_invalid
    from polyhorizon.streaming.src.schemas import build_raw_schema
    from polyhorizon.streaming.src.delta_sinks import start_bronze_stream, start_dead_letter_stream

    raw = read_kafka_stream(spark, config, build_raw_schema(config.features))
    valid, invalid = split_valid_invalid(raw, config.features)
    checkpoint = f"s3a://{bucket}/checkpoints/bronze"
    bronze = start_bronze_stream(valid, config.streaming_storage.bronze, checkpoint, "1 second")
    dead = start_dead_letter_stream(invalid, f"s3a://{bucket}/dead", f"s3a://{bucket}/checkpoints/dead", "1 second")
    try:
        bronze.processAllAvailable()
        dead.processAllAvailable()
    finally:
        bronze.stop()
        dead.stop()
    assert bronze.exception() is None and dead.exception() is None
    frame = spark.read.format("delta").load(config.streaming_storage.bronze)
    assert frame.count() == 9
    assert {row.event_time.microsecond for row in frame.select("event_time").collect()} == {123000}, "Kafka timestamps lost millisecond precision"
    rejected = spark.read.format("delta").load(f"s3a://{bucket}/dead").collect()
    assert len(rejected) == 1 and rejected[0].dq_reason == "invalid_price"
    # Reuse the exact checkpoint; restarting must not append the same trades.
    restart = start_bronze_stream(valid, config.streaming_storage.bronze, checkpoint, "1 second")
    try:
        restart.processAllAvailable()
    finally:
        restart.stop()
    assert restart.exception() is None
    assert spark.read.format("delta").load(config.streaming_storage.bronze).count() == 9


@flow(name="kafka-publication-rehearsal", retries=0, timeout_seconds=600)
def scheduled_rehearsal():
    import boto3
    import psycopg2
    from feast import FeatureStore
    from kafka.admin import KafkaAdminClient, NewTopic
    from pyspark.sql import SparkSession
    from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
    from polyhorizon.streaming.configs.settings import FeaturesConfig, KafkaConnectionConfig
    from polyhorizon.streaming.feature_engineering.release import complete_dataset
    from polyhorizon.streaming.tasks.spark_streaming_flow import publish_completed_dataset

    run_id = uuid4().hex
    config = fixture_config(run_id)
    bucket, topic = f"rehearsal-{run_id}", f"rehearsal-{run_id}"
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id="test", aws_secret_access_key="test")
    s3.create_bucket(Bucket=bucket)
    admin = KafkaAdminClient(bootstrap_servers=[BROKER])
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    spark = None
    try:
        close = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        sent = asyncio.run(produce_fixture(s3, bucket, topic, close))
        prepare_feast_repo(config, run_id)
        spark = (SparkSession.builder.master("local[2]").appName("kafka-publication-rehearsal")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
            .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", "test").config("spark.hadoop.fs.s3a.secret.key", "test")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        streaming = SimpleNamespace(
            streaming_storage=SimpleNamespace(bronze=f"s3a://{bucket}/bronze", gold=f"s3a://{bucket}/gold",
                feature_output_path=config.bucket_details.feature_output_path),
            features=FeaturesConfig(freq="30 minutes", lookback_bars=2, time_col="window_end", symbol_col="symbol",
                open_col="open", high_col="high", low_col="low", close_col="close", volume_col="total_volume",
                late_data_threshold="7 days"),
            kafka_connection=KafkaConnectionConfig(kafka_servers=[BROKER], kafka_topic=topic,
                tickers_csv="unused", consumer_group_id=run_id, starting_offsets="earliest",
                fail_on_data_loss=True, fetch_min_bytes=1),
        )
        consume_fixture(spark, streaming, bucket)
        release = complete_dataset(spark, streaming, close)
        assert release.row_count == 6
        spark.stop()
        spark = None
        handoff = publish_completed_dataset(release.model_dump(mode="json"))
        store = FeatureStore(repo_path=config.paths.feast_repo_path)
        online = store.get_online_features(features=["stock_ohlcv_features:close", "stock_ohlcv_features:total_volume"],
            entity_rows=[{"symbol": symbol} for symbol in PRODUCT_SYMBOLS]).to_dict()
        assert online["close"] == [102.0] * 3
        assert online["total_volume"] == [10] * 3
        with psycopg2.connect(**config.connection_parameters.model_dump()) as db, db.cursor() as cursor:
            cursor.execute(f"SELECT dataset_id,status FROM {config.data.db_schema}.dataset_publications")
            assert cursor.fetchone() == (str(release.dataset_id), "complete")
        result = {"sent": sent, "bronze_rows": 9, "dead_letter_rows": 1,
            "feature_rows": release.row_count, "checkpoint_restart": "no_duplicates", **handoff}
        (Path(os.environ["ORCHESTRATION_REHEARSAL_ROOT"]) / "result.json").write_text(json.dumps(result))
        return result
    finally:
        if spark is not None:
            spark.stop()
        admin.delete_topics([topic])
        admin.close()
        with psycopg2.connect(**config.connection_parameters.model_dump()) as db, db.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {config.data.db_schema} CASCADE")
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            if page.get("Contents"):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": obj["Key"]} for obj in page["Contents"]]})
        s3.delete_bucket(Bucket=bucket)
