from __future__ import annotations

from pathlib import Path

import yaml
import pytest

from polyhorizon.features.configs.settings import Config


def _base_config_dict(parquet_path: str) -> dict:
    return {
        "project": {
            "name": "PolyHorizon Feature Store",
            "description": "test",
            "version": "0.1.0",
            "feast_project_name": "features",
            "feature_view": "stock_ohlcv_features",
        },
        "paths": {
            "base_dir": ".",
            "project_root": "proj",
            "feast_repo_path": "feature_repo",
            "feature_store_yaml": "feature_repo/feature_store.yaml",
            "yaml_file_path": "scripts/features.yaml",
        },
        "data": {
            "parquet_path": parquet_path,
            "min_days_required": 10,
            "required_symbols": 2,
            "db_schema": "feast_schema",
            "offline_table_name": "training_stock_ohlcv",
            "snapshot_table_name": "training_stock_ohlcv_snapshot",
            "snapshot_days": 30,
            "enable_partitioning": False,
            "partition_by": "window_start",
            "partition_granularity": "month",
        },
        "data_contract": {
            "required_columns": ["symbol", "event_timestamp", "window_start", "open", "high", "low", "close"],
            "column_types": {
                "symbol": "string",
                "event_timestamp": "datetime",
                "window_start": "datetime",
                "open": "float",
            },
            "allow_extra_columns": True,
        },
        "logging": {
            "log_level": "INFO",
            "log_dir": "/tmp",
            "log_format": "%(message)s",
            "log_date_format": "%Y-%m-%d",
            "log_to_file": False,
            "log_json_format": False,
            "max_file_size": 10,
            "backup_count": 3,
            "alert_emails": ["alerts@example.com"],
        },
        "monitoring": {
            "enable_metrics": True,
            "pushgateway_url": "http://localhost:9091",
            "metrics_prefix": "features",
        },
        "connection_parameters": {
            "host": "localhost",
            "port": 5432,
            "database": "feature_store_db",
            "user": "feast_user",
            "password": "feast_pass",
            "connect_timeout": 5,
        },
        "registry": {
            "host": "localhost",
            "port": 5432,
            "database": "postgres",
            "user": "postgres",
            "password": "postgres",
            "connect_timeout": 5,
            "schema": "feast_registry",
        },
        "features": {
            "stock_ohlcv_features": ["open", "high", "low", "close"],
            "symbols": ["AAPL", "MSFT"],
            "required_columns": ["symbol", "open", "high", "low", "close"],
        },
        "feast_parameters": {
            "lookback_days": 30,
            "freq": "30min",
            "ttl_days": 7,
            "z_threshold": 3.0,
            "drift_threshold": 0.05,
        },
        "feast_features": {
            "stock_ohlcv_features": ["stock_ohlcv_features:open"],
            "stock_price_prediction_features": ["stock_ohlcv_features:close"],
        },
        "redis_connection_parameters": {
            "redis_host": "localhost",
            "redis_port": 6379,
        },
        "bucket_details": {
            "seaweedfs_s3_endpoint": "http://localhost:8333",
            "seaweedfs_access_key": "access",
            "seaweedfs_secret_key": "secret",
            "tickers_s3_path": "s3://bucket-a/tickers.csv",
            "seaweedfs_s3_buckets": "bucket-a, bucket-b",
            "symbols_bucket": "bucket-a",
            "symbols_key": "symbols.csv",
            "feature_output_path": "s3://bucket-b/features/output",
            "feature_parquet_path": "s3://bucket-b/features/parquet",
        },
    }


def _write_config(tmp_path: Path, config_dict: dict) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config_dict))
    return config_path


def test_config_env_expansion_and_path_resolution(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TEST_PARQUET_PATH", "s3://bucket-a/data/train.parquet")

    config_dict = _base_config_dict(parquet_path="${TEST_PARQUET_PATH}")
    config_path = _write_config(tmp_path, config_dict)

    config = Config.from_yaml(config_path)

    assert config.data.parquet_path == "s3://bucket-a/data/train.parquet"
    assert Path(config.paths.project_root) == (tmp_path / "proj").resolve()
    assert config.bucket_details.seaweedfs_s3_buckets == ["bucket-a", "bucket-b"]


def test_bucket_validation_fails_on_unknown_bucket(tmp_path) -> None:
    config_dict = _base_config_dict(parquet_path="s3://bucket-a/data/train.parquet")
    config_dict["bucket_details"]["seaweedfs_s3_buckets"] = "bucket-a"
    config_dict["bucket_details"]["feature_output_path"] = "s3://bucket-x/features/output"

    config_path = _write_config(tmp_path, config_dict)

    with pytest.raises(ValueError, match="Bucket 'bucket-x' not found"):
        Config.from_yaml(config_path)
