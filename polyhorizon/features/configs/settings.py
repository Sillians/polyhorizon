from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus
import os
import re
import yaml

from pydantic import BaseModel, Field, AnyUrl, field_validator, model_validator
from pydantic.config import ConfigDict
from pydantic.types import PositiveInt, NonNegativeFloat

from dotenv import load_dotenv
load_dotenv()


class BaseConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ProjectConfig(BaseConfigModel):
    name: str
    description: str
    version: str
    feast_project_name: str
    feature_view: str


class DataConfig(BaseConfigModel):
    parquet_path: str
    min_days_required: PositiveInt
    required_symbols: PositiveInt
    db_schema: str
    offline_table_name: str
    snapshot_table_name: str
    snapshot_days: PositiveInt
    enable_partitioning: bool = False
    partition_by: str = "window_start"
    partition_granularity: str = "month"

    @field_validator(
        "db_schema",
        "offline_table_name",
        "snapshot_table_name",
        "partition_by",
    )
    @classmethod
    def validate_sql_identifier(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
        return value

    @field_validator("parquet_path")
    @classmethod
    def validate_parquet_path(cls, value: str) -> str:
        valid_prefixes = ("s3://", "s3a://", "file://")
        if value.startswith("/"):
            return value
        if not value.startswith(valid_prefixes):
            raise ValueError(f"parquet_path must start with {valid_prefixes} or be absolute")
        return value

    @field_validator("partition_granularity")
    @classmethod
    def validate_partition_granularity(cls, value: str) -> str:
        allowed = {"day", "month"}
        if value not in allowed:
            raise ValueError(f"partition_granularity must be one of {allowed}")
        return value


class LoggingConfig(BaseConfigModel):
    log_level: str
    log_dir: str
    log_format: str
    log_date_format: str
    log_to_file: bool
    log_json_format: bool
    max_file_size: PositiveInt
    backup_count: PositiveInt
    alert_emails: List[str]


class MonitoringConfig(BaseConfigModel):
    enable_metrics: bool = False
    pushgateway_url: str = "prometheus-pushgateway:9091"
    metrics_prefix: str = "features"


class ConnectionParams(BaseConfigModel):
    host: str
    port: PositiveInt
    database: str
    user: str
    password: str
    connect_timeout: PositiveInt

    @property
    def connection_string(self) -> str:
        safe_password = quote_plus(self.password)
        return (
            f"postgresql+psycopg2://{self.user}:{safe_password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


class RegistryConnectionParams(BaseConfigModel):
    host: str
    port: PositiveInt
    database: str
    user: str
    password: str
    connect_timeout: PositiveInt
    schema: str

    @property
    def connection_string(self) -> str:
        safe_password = quote_plus(self.password)
        return (
            f"postgresql+psycopg2://{self.user}:{safe_password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


class FeaturesConfig(BaseConfigModel):
    stock_ohlcv_features: List[str]
    symbols: List[str]
    required_columns: List[str]
    column_types: Dict[str, str] = Field(default_factory=dict)


class FeastParametersConfig(BaseConfigModel):
    lookback_days: PositiveInt = 30
    freq: str
    ttl_days: PositiveInt = 7
    z_threshold: NonNegativeFloat
    drift_threshold: NonNegativeFloat
    max_backfill_days: PositiveInt = 90


class DataContractConfig(BaseConfigModel):
    required_columns: List[str]
    column_types: Dict[str, str] = Field(default_factory=dict)
    allow_extra_columns: bool = True


class FeastFeatureRefs(BaseConfigModel):
    stock_ohlcv_features: List[str]
    stock_price_prediction_features: List[str]


class PathsConfig(BaseConfigModel):
    base_dir: str
    project_root: str
    feast_repo_path: str
    feature_store_yaml: str
    yaml_file_path: str


class RedisConnectionParams(BaseConfigModel):
    redis_host: str
    redis_port: PositiveInt

    @property
    def redis_conn_str(self) -> str:
        return f"{self.redis_host}:{self.redis_port}"


class BucketDetailsConfig(BaseConfigModel):
    seaweedfs_s3_endpoint: AnyUrl
    seaweedfs_access_key: str
    seaweedfs_secret_key: str
    tickers_s3_path: str
    seaweedfs_s3_buckets: List[str] = Field(default_factory=list)
    symbols_bucket: str
    symbols_key: str
    feature_output_path: str
    feature_parquet_path: str

    @field_validator("seaweedfs_s3_buckets", mode="before")
    @classmethod
    def parse_bucket_list(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [b.strip().strip("'\"") for b in raw.split(",") if b.strip()]
        return []

    @field_validator("tickers_s3_path", "feature_output_path", "feature_parquet_path")
    @classmethod
    def validate_s3_paths(cls, value: str) -> str:
        valid_prefixes = ("s3://", "s3a://")
        if not value.startswith(valid_prefixes):
            raise ValueError(f"S3 path must start with {valid_prefixes}: {value}")
        return value


class Config(BaseConfigModel):
    project: ProjectConfig
    data: DataConfig
    data_contract: DataContractConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig
    connection_parameters: ConnectionParams
    registry: RegistryConnectionParams
    features: FeaturesConfig
    feast_parameters: FeastParametersConfig
    feast_features: FeastFeatureRefs
    paths: PathsConfig
    redis_connection_parameters: RedisConnectionParams
    bucket_details: BucketDetailsConfig

    @model_validator(mode="after")
    def validate_buckets(self) -> "Config":
        buckets = set(self.bucket_details.seaweedfs_s3_buckets)
        if not buckets:
            return self

        def bucket_from_path(path: str) -> Optional[str]:
            if path.startswith(("s3://", "s3a://")):
                return path.split("://", 1)[1].split("/", 1)[0]
            return None

        paths = [
            self.bucket_details.tickers_s3_path,
            self.bucket_details.feature_output_path,
            self.bucket_details.feature_parquet_path,
        ]
        for path in paths:
            bucket = bucket_from_path(path)
            if bucket and bucket not in buckets:
                raise ValueError(
                    f"Bucket '{bucket}' not found in seaweedfs_s3_buckets"
                )
        return self

    @classmethod
    def from_yaml(cls, config_path: Path | str) -> "Config":
        config_path = Path(config_path).resolve()
        with open(config_path) as f:
            config_dict = yaml.safe_load(f) or {}

        def expand_env(value):
            if isinstance(value, str):
                return os.path.expandvars(value)
            if isinstance(value, dict):
                return {k: expand_env(v) for k, v in value.items()}
            if isinstance(value, list):
                return [expand_env(v) for v in value]
            return value

        config_dict = expand_env(config_dict)

        unresolved: list[tuple[str, str]] = []

        def find_unresolved(value, path: str = "") -> None:
            if isinstance(value, str):
                if re.search(r"\$\{[^}]+\}", value):
                    unresolved.append((path, value))
                return
            if isinstance(value, dict):
                for key, nested in value.items():
                    next_path = f"{path}.{key}" if path else str(key)
                    find_unresolved(nested, next_path)
                return
            if isinstance(value, list):
                for index, nested in enumerate(value):
                    next_path = f"{path}[{index}]" if path else f"[{index}]"
                    find_unresolved(nested, next_path)

        find_unresolved(config_dict)
        if unresolved:
            missing = ", ".join(f"{path}={value}" for path, value in unresolved)
            raise ValueError(f"Unresolved environment variables in config: {missing}")

        config_path_parent = config_path.parent
        if "paths" in config_dict:
            for key, value in config_dict["paths"].items():
                if isinstance(value, str):
                    path_value = Path(value)
                    if not path_value.is_absolute():
                        config_dict["paths"][key] = str((config_path_parent / path_value).resolve())

        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        project_root = Path(__file__).resolve().parents[1]
        default_path = project_root / "configs/feast_features.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)


def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
