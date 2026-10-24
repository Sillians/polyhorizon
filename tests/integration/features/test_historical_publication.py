"""Training-only historical publication on the isolated S3/Postgres stack."""
import os
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest

pytestmark = pytest.mark.skipif(os.getenv("PUBLICATION_E2E") != "1", reason="Requires isolated publication Compose stack")


def test_historical_publication_is_atomic_idempotent_and_cannot_rewind_online(tmp_path, monkeypatch):
    import boto3
    import psycopg2
    from datetime import date, datetime, timezone
    from polyhorizon.core.historical_qualification import HistoricalSource, bars_digest
    from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
    from polyhorizon.features.configs.settings import ConnectionParams
    from polyhorizon.features.src.publish_dataset import publish_dataset
    from polyhorizon.ingestion.historical_backfill import build_release
    from polyhorizon.training.src.data.generate_training_dataframe import PostgresDataLoader

    schema, bucket = "history_" + uuid4().hex, "history-" + uuid4().hex
    endpoint = "http://127.0.0.1:18333"
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test")
    s3.create_bucket(Bucket=bucket)
    connection = ConnectionParams(host="127.0.0.1", port=15432, database="publication_test",
                                  user="postgres", password="publication-test-only", connect_timeout=5)
    config = SimpleNamespace(bucket_details=SimpleNamespace(feature_output_path=f"s3://{bucket}/features",
        seaweedfs_s3_endpoint=endpoint, seaweedfs_access_key="test", seaweedfs_secret_key="test"),
        data=SimpleNamespace(db_schema=schema, offline_table_name="ohlcv", snapshot_table_name="snapshot",
                             min_days_required=3650, min_history_sessions=1), connection_parameters=connection,
        data_contract=SimpleNamespace(required_columns=["symbol", "event_timestamp", "close", "total_volume"],
            column_types={"close": "float", "total_volume": "int"}, allow_extra_columns=True))
    options = {"AWS_ENDPOINT_URL": endpoint, "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
               "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false"}
    bars = pd.DataFrame([dict(symbol=s, window_start=t, window_end=t + pd.Timedelta(minutes=30),
        open=100. + i, high=102. + i, low=99. + i, close=101. + i, total_volume=100)
        for s in PRODUCT_SYMBOLS for i, t in enumerate(pd.date_range("2026-09-29T13:30Z", "2026-09-29T20:00Z", freq="30min", inclusive="left"))])
    source = HistoricalSource(provider="csv", adjustment="unadjusted", start_date=date(2026, 9, 29),
        end_date=date(2026, 9, 29), retrieved_at=datetime(2026, 9, 30, tzinfo=timezone.utc),
        content_sha256=bars_digest(bars), request_sha256="a" * 64)
    db = psycopg2.connect(**connection.model_dump())
    db.autocommit = True
    try:
        release = build_release(bars, source, config.bucket_details.feature_output_path,
                                f"s3://{bucket}/raw", options, lookback_bars=2)
        result = publish_dataset(release.model_dump(mode="json"), config=config)
        assert result["publication_scope"] == "training_only"
        assert result["online_materialized"] is False
        assert publish_dataset(release.model_dump(mode="json"), config=config)["already_published"]
        with db.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {schema}.snapshot_historical")
            assert cur.fetchone()[0] == 36
            cur.execute(f"SELECT count(*) FROM {schema}.snapshot")
            assert cur.fetchone()[0] == 0
            cur.execute("SELECT to_regclass(%s), to_regclass(%s)", (f"{schema}.ohlcv", f"{schema}.latest_training_snapshot"))
            assert cur.fetchone() == (None, None)
            cur.execute(f"SELECT status FROM {schema}.dataset_publications")
            assert cur.fetchone()[0] == "complete"
        loader = PostgresDataLoader(config)
        assert len(loader.fetch_data(track_mlflow=False)) == 36
        config.data.min_history_sessions = 60
        with pytest.raises(ValueError, match="60 qualified sessions"):
            loader.fetch_data(track_mlflow=False)
        loader.get_engine().dispose()
        # Forge evidence: must fail before opening a PostgreSQL connection.
        bad = release.model_dump(mode="json")
        bad["session_qualification"]["session_count"] = 100
        monkeypatch.setattr("polyhorizon.features.src.publish_dataset.get_db_conn", lambda *_: pytest.fail("DB touched before qualification"))
        with pytest.raises(ValueError, match="mismatched historical"):
            publish_dataset(bad, config=config)
    finally:
        with db.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        db.close()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
            if page.get("Contents"):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in page["Contents"]]})
        s3.delete_bucket(Bucket=bucket)
