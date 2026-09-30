from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
import pytest

from polyhorizon.core.dataset_release import DatasetRelease, publication_source_query, validate_feature_frame
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.features.src.publish_dataset import publish_dataset, read_release, _interior_session_gaps


def test_publication_source_selects_one_version_and_rejects_sql_identifiers():
    query = publication_source_query("feast", "ohlcv")
    assert "SELECT * FROM feast.ohlcv WHERE dataset_version" in query
    assert "FROM feast.dataset_publications" in query
    assert "ORDER BY max_event_time DESC, updated_at DESC LIMIT 1" in query
    with pytest.raises(ValueError):
        publication_source_query("feast; DROP SCHEMA public", "ohlcv")


def test_publication_manager_does_not_fetch_external_symbol_universe():
    from polyhorizon.features.feast_ops.feature_manager import FeatureManager

    with patch("polyhorizon.features.feast_ops.feature_manager.init_feature_store"), patch(
        "polyhorizon.features.feast_ops.feature_manager.load_sp500_symbols"
    ) as fetch:
        manager = FeatureManager(SimpleNamespace(), load_symbols=False)
        assert manager.symbols == list(PRODUCT_SYMBOLS)
        fetch.assert_not_called()


@pytest.fixture
def frame():
    return pd.DataFrame({
        "symbol": list(PRODUCT_SYMBOLS),
        "window_start": pd.to_datetime(["2026-09-21T19:30Z"] * 3),
        "window_end": pd.to_datetime(["2026-09-21T20:00Z"] * 3),
        "event_timestamp": pd.to_datetime(["2026-09-21T20:00Z"] * 3),
        "open": [100.] * 3, "high": [102.] * 3, "low": [99.] * 3,
        "close": [101.] * 3, "total_volume": [1000] * 3,
        "rolling_avg_close": [100.] * 3, "rolling_volatility_close": [1.] * 3,
    })


@pytest.fixture
def release():
    dataset_id = uuid4()
    close = datetime(2026, 9, 21, 20, tzinfo=timezone.utc)
    return DatasetRelease(
        dataset_id=dataset_id, bronze_path="s3a://bucket/bronze", bronze_version=15,
        gold_path=f"s3a://bucket/gold-completed/{dataset_id}", gold_version=0,
        feature_path=f"s3a://bucket/features/datasets/{dataset_id}", feature_version=0,
        image_tag="test", market_close=close, completed_at=close,
        frequency="30 minutes", lookback_bars=60,
        min_event_time=close, max_event_time=close, row_count=3,
    )


def test_manifest_and_frame_validation(release, frame):
    release.validate_root("s3://bucket/features/")
    validate_feature_frame(frame, release)
    with pytest.raises(ValueError, match="path"):
        release.validate_root("s3://wrong/features")
    with pytest.raises(ValueError, match="manifest"):
        validate_feature_frame(pd.concat([frame, frame.assign(window_start=frame.window_start - pd.Timedelta(minutes=30))]), release)


def test_interior_session_gap_metric_does_not_claim_leading_completeness():
    timestamps = pd.to_datetime(["2026-09-23T18:30Z", "2026-09-23T19:30Z", "2026-09-23T20:00Z"])
    frame = pd.DataFrame({"symbol": ["NVDA"] * 3, "event_timestamp": timestamps})
    assert _interior_session_gaps(frame, datetime(2026, 9, 23, 20, tzinfo=timezone.utc)) == {"NVDA": 1}


@pytest.mark.parametrize("column,value", [("close", -1), ("high", 50), ("total_volume", 1.5),
                                        ("rolling_volatility_close", float("nan")), ("open", float("inf"))])
def test_invalid_features_fail(column, value, frame):
    frame[column] = value
    with pytest.raises(ValueError):
        validate_feature_frame(frame)


def test_validation_failure_never_connects_to_postgres(release):
    with patch("polyhorizon.features.src.publish_dataset.read_release", side_effect=ValueError("invalid")), patch(
        "polyhorizon.features.src.publish_dataset.get_db_conn"
    ) as connect:
        with pytest.raises(ValueError, match="invalid"):
            publish_dataset(release.model_dump(mode="json"), config=SimpleNamespace())
        connect.assert_not_called()


def config():
    return SimpleNamespace(data=SimpleNamespace(db_schema="feast", offline_table_name="ohlcv",
                                               snapshot_table_name="snapshot", snapshot_days=180),
                           feast_parameters=SimpleNamespace(lookback_days=30),
                           project=SimpleNamespace(feature_view="stock_ohlcv_features"))


def test_feast_failure_leaves_pending_commit_and_retry_reuses_it(release, frame):
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    # Lock acquired, no previous run, no pending run, no newer release/data.
    cursor.fetchone.side_effect = [(True,), None, None, (None,), (None,)]
    with patch("polyhorizon.features.src.publish_dataset.read_release", return_value=frame), patch(
        "polyhorizon.features.src.publish_dataset.get_db_conn", return_value=conn
    ), patch("polyhorizon.features.src.publish_dataset.add_missing_columns"), patch(
        "polyhorizon.features.src.publish_dataset._record_schema_version"
    ), patch(
        "polyhorizon.features.feast_ops.feature_manager.FeatureManager"
    ) as manager:
        manager.return_value.materialize_features.side_effect = RuntimeError("Redis unavailable")
        with pytest.raises(RuntimeError, match="Redis"):
            publish_dataset(release.model_dump(mode="json"), config=config())
        assert conn.commit.call_count == 1
        assert not any("SET status='complete'" in call.args[0] for call in cursor.execute.call_args_list)
        conn.close.assert_called_once()

        conn.reset_mock()
        cursor.fetchone.side_effect = [(True,), (release.model_dump(mode="json"), "postgres_complete"), None, (release.max_event_time,)]
        manager.return_value.materialize_features.side_effect = None
        result = publish_dataset(release.model_dump(mode="json"), config=config())
        assert result["status"] == "success"
        cursor.copy_expert.assert_not_called()
        assert conn.commit.call_count == 1
        assert manager.return_value.materialize_features.call_args.kwargs["incremental"] is False


def test_reader_pins_version_and_normalizes_s3_path(release, frame):
    cfg = SimpleNamespace(bucket_details=SimpleNamespace(feature_output_path="s3://bucket/features",
        seaweedfs_s3_endpoint="http://s3:8333", seaweedfs_access_key="test", seaweedfs_secret_key="test"))
    with patch("polyhorizon.features.src.publish_dataset.DeltaTable") as table, patch(
        "polyhorizon.features.src.publish_dataset.validate_data_contract"
    ), patch("polyhorizon.features.src.publish_dataset.qualify_session", return_value={"market_open": "2026-09-21T13:30:00+00:00"}), patch(
        "polyhorizon.features.src.publish_dataset.qualify_trades", return_value={}
    ):
        import pyarrow as pa
        import pyarrow.dataset as ds
        release.session_qualification = {"market_open": "2026-09-21T13:30:00+00:00", "trade_coverage": {}}
        table.return_value.to_pyarrow_dataset.return_value = ds.dataset(pa.table({
            "symbol": ["NVDA"], "price": [100.], "volume": [1.],
            "event_time": pa.array([release.market_close], type=pa.timestamp("us", tz="UTC"))}))
        table.return_value.to_pandas.return_value = frame
        read_release(release, cfg)
        assert table.call_args_list[0].kwargs["version"] == 0
        assert table.call_args.kwargs["version"] == release.bronze_version
        assert table.call_args.args[0].startswith("s3://")


@pytest.mark.parametrize("state", ["complete", "pending_other", "older", "changed_manifest"])
def test_publication_guards_do_not_write_or_materialize(state, release, frame):
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    if state == "complete":
        replies = [(True,), (release.model_dump(mode="json"), "complete")]
    elif state == "pending_other":
        replies = [(True,), None, ("another-dataset",)]
    elif state == "changed_manifest":
        replies = [(True,), ({"different": True}, "postgres_complete")]
    else:
        replies = [(True,), None, None, (release.max_event_time + pd.Timedelta(days=1),)]
    cursor.fetchone.side_effect = replies
    with patch("polyhorizon.features.src.publish_dataset.read_release", return_value=frame), patch(
        "polyhorizon.features.src.publish_dataset.get_db_conn", return_value=conn
    ), patch("polyhorizon.features.feast_ops.feature_manager.FeatureManager") as manager:
        if state == "complete":
            assert publish_dataset(release.model_dump(mode="json"), config=config())["already_published"]
        else:
            with pytest.raises((ValueError, RuntimeError)):
                publish_dataset(release.model_dump(mode="json"), config=config())
        cursor.copy_expert.assert_not_called()
        manager.assert_not_called()
        conn.close.assert_called_once()
