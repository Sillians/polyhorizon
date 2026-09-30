"""Run against tests/integration/features/compose.yaml, never the app stack."""
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(os.getenv("PUBLICATION_E2E") != "1", reason="Set PUBLICATION_E2E=1 with the isolated Compose stack")


def test_gold_to_feast_with_pending_retry(tmp_path, monkeypatch):
    import boto3
    import psycopg2
    from pyspark.sql import SparkSession
    from feast import Entity, FeatureView, FeatureStore, Field, ValueType
    from feast.types import Float32, Int64, String
    from feast.repo_config import RepoConfig
    from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
    from polyhorizon.core.dataset_release import publication_source_query
    from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
    from polyhorizon.features.configs.settings import ConnectionParams
    from polyhorizon.features.feast_ops.feature_manager import FeatureManager
    from polyhorizon.features.src.publish_dataset import publish_dataset
    from polyhorizon.streaming.configs.settings import FeaturesConfig
    from polyhorizon.streaming.feature_engineering.release import complete_dataset

    run_id = uuid4().hex
    schema = f"publication_{run_id}"
    project = f"publication_{run_id}"
    bucket = f"publication-{run_id}"
    endpoint = "http://127.0.0.1:18333"
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                      aws_secret_access_key="test", region_name="us-east-1")
    for attempt in range(30):
        try:
            s3.create_bucket(Bucket=bucket)
            break
        except Exception:
            if attempt == 29:
                raise
            time.sleep(1)

    # Delta metadata renames require conditional S3 copies. Fail fast if the
    # object-store image cannot honor them instead of waiting for Hadoop retries.
    s3.put_object(Bucket=bucket, Key="copy-probe-source", Body=b"publication")
    etag = s3.head_object(Bucket=bucket, Key="copy-probe-source")["ETag"]
    s3.copy_object(Bucket=bucket, Key="copy-probe-target",
                   CopySource={"Bucket": bucket, "Key": "copy-probe-source"}, CopySourceIfMatch=etag)

    connection = ConnectionParams(host="127.0.0.1", port=15432, database="publication_test",
                                  user="postgres", password="publication-test-only", connect_timeout=5)
    root = f"s3a://{bucket}/features"
    config = SimpleNamespace(
        bucket_details=SimpleNamespace(feature_output_path=root, seaweedfs_s3_endpoint=endpoint,
                                       seaweedfs_access_key="test", seaweedfs_secret_key="test"),
        data=SimpleNamespace(db_schema=schema, offline_table_name="ohlcv", snapshot_table_name="snapshot",
                             snapshot_days=30, enable_partitioning=False),
        connection_parameters=connection,
        data_contract=SimpleNamespace(required_columns=["symbol", "event_timestamp", "close", "total_volume"],
                                      column_types={"close": "float", "total_volume": "int"}, allow_extra_columns=True),
        feast_parameters=SimpleNamespace(lookback_days=30, max_backfill_days=90),
        project=SimpleNamespace(feature_view="stock_ohlcv_features"),
    )
    offline = connection.model_dump(exclude={"connect_timeout"}) | {"type": "postgres", "db_schema": schema}
    store = FeatureStore(config=RepoConfig(
        project=project, provider="local", registry=str(tmp_path / "registry.db"),
        offline_store=offline, online_store={"type": "redis", "connection_string": "127.0.0.1:16379"},
        entity_key_serialization_version=3,
    ))
    monkeypatch.setattr("polyhorizon.features.feast_ops.feature_manager.init_feature_store", lambda **kwargs: store)
    monkeypatch.setenv("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[2]").appName("publication-e2e")
             .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
             .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.databricks.delta.snapshotPartitions", "2")
             .config("spark.hadoop.fs.s3a.endpoint", endpoint)
             .config("spark.hadoop.fs.s3a.access.key", "test")
             .config("spark.hadoop.fs.s3a.secret.key", "test")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    import pandas_market_calendars as mcal
    now = datetime.now(timezone.utc)
    sessions = mcal.get_calendar("NYSE").schedule(start_date=(now - timedelta(days=10)).date(), end_date=now.date())
    completed_sessions = sessions.loc[sessions.market_close < now]
    session = completed_sessions.iloc[-2]
    next_session = completed_sessions.iloc[-1]
    close = session.market_close.to_pydatetime()
    opening = session.market_open.to_pydatetime()
    minutes = int((close - opening).total_seconds() // 60)
    expected_rows = (minutes // 30 - 1) * len(PRODUCT_SYMBOLS)
    stream_config = SimpleNamespace(
        streaming_storage=SimpleNamespace(bronze=f"s3a://{bucket}/bronze", gold=f"s3a://{bucket}/gold", feature_output_path=root),
        features=FeaturesConfig(freq="30 minutes", lookback_bars=2, time_col="window_end", symbol_col="symbol",
            open_col="open", high_col="high", low_col="low", close_col="close", volume_col="total_volume"),
    )
    db = psycopg2.connect(**connection.model_dump())
    db.autocommit = True
    try:
        rows = [(symbol, 100.0 + i // 30, 10.0, opening + timedelta(minutes=i))
                for symbol in PRODUCT_SYMBOLS for i in range(minutes)]
        spark.createDataFrame(rows, ["symbol", "price", "volume", "event_time"]).write.format("delta").save(stream_config.streaming_storage.bronze)
        release = complete_dataset(spark, stream_config, close)
        assert release.row_count == expected_rows
        assert release.bronze_version == release.gold_version == release.feature_version == 0

        # Exercise a real PostgreSQL commit followed by a controlled online failure.
        materialize = FeatureManager.materialize_features
        with monkeypatch.context() as failure:
            def fail_online(*args, **kwargs):
                raise RuntimeError("injected online-store outage")
            failure.setattr(FeatureManager, "materialize_features", fail_online)
            with pytest.raises(RuntimeError, match="injected"):
                publish_dataset(release.model_dump(mode="json"), config=config)
        assert FeatureManager.materialize_features is materialize
        with db.cursor() as cursor:
            cursor.execute(f"SELECT status FROM {schema}.dataset_publications")
            assert cursor.fetchone()[0] == "postgres_complete"
            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT dataset_version) FROM {schema}.snapshot")
            assert cursor.fetchone() == (expected_rows, 1)

        entity = Entity(name="symbol", join_keys=["symbol"], value_type=ValueType.STRING)
        source = PostgreSQLSource(name="ohlcv", query=publication_source_query(schema, "ohlcv"),
                                  timestamp_field="event_timestamp", created_timestamp_column="created_at")
        view = FeatureView(name="stock_ohlcv_features", entities=[entity], ttl=timedelta(days=7),
                           schema=[Field(name="close", dtype=Float32),
                                   Field(name="total_volume", dtype=Int64),
                                   Field(name="dataset_version", dtype=String)], source=source, online=True)
        store.apply([entity, view])
        result = publish_dataset(release.model_dump(mode="json"), config=config)
        assert result["status"] == "success"
        online = store.get_online_features(features=["stock_ohlcv_features:close",
                    "stock_ohlcv_features:total_volume", "stock_ohlcv_features:dataset_version"],
                    entity_rows=[{"symbol": symbol} for symbol in PRODUCT_SYMBOLS]).to_dict()
        assert online["close"] == [100.0 + minutes // 30 - 1] * 3
        assert online["total_volume"] == [300] * 3
        assert online["dataset_version"] == [str(release.dataset_id)] * 3
        assert publish_dataset(release.model_dump(mode="json"), config=config)["already_published"]
        with db.cursor() as cursor:
            cursor.execute(f"SELECT status FROM {schema}.dataset_publications")
            assert cursor.fetchone()[0] == "complete"
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.ohlcv")
            assert cursor.fetchone()[0] == expected_rows
        # Session-sized releases must preserve prior qualified training history.
        next_open, next_close = next_session.market_open.to_pydatetime(), next_session.market_close.to_pydatetime()
        next_minutes = int((next_close - next_open).total_seconds() // 60)
        more = [(symbol, 120.0 + i // 30, 10.0, next_open + timedelta(minutes=i))
                for symbol in PRODUCT_SYMBOLS for i in range(next_minutes)]
        spark.createDataFrame(more, ["symbol", "price", "volume", "event_time"]).write.format("delta").mode("append").save(stream_config.streaming_storage.bronze)
        second = complete_dataset(spark, stream_config, next_close)
        assert second.row_count == (next_minutes // 30) * len(PRODUCT_SYMBOLS)
        assert second.min_event_time > release.max_event_time
        publish_dataset(second.model_dump(mode="json"), config=config)
        with db.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*), COUNT(DISTINCT dataset_version) FROM {schema}.snapshot")
            assert cursor.fetchone() == (expected_rows + second.row_count, 2)
    finally:
        spark.stop()
        # These names were generated exclusively for this test run.
        with db.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        db.close()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            if page.get("Contents"):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": obj["Key"]} for obj in page["Contents"]]})
        s3.delete_bucket(Bucket=bucket)
