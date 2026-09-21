from pathlib import Path
from typing import Dict, List, Optional
import os
import re
import yaml

from pydantic import BaseModel, Field, SecretStr, AnyUrl, field_validator, model_validator
from pydantic.config import ConfigDict
from pydantic.types import PositiveInt, NonNegativeInt, PositiveFloat, NonNegativeFloat

from dotenv import load_dotenv
load_dotenv()

_versions_env = Path(__file__).resolve().parents[3] / "docker" / "spark" / "versions.env"
if _versions_env.exists():
    load_dotenv(_versions_env)


class BaseConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ProjectConfig(BaseConfigModel):
    name: str
    description: str
    version: str


class FinnhubConnectionConfig(BaseConfigModel):
    finnhub_token: SecretStr
    finnhub_ws_url: AnyUrl
    max_symbols_per_connection: PositiveInt
    max_total_symbols: PositiveInt
    subscription_delay: NonNegativeFloat
    connection_retry_delay: NonNegativeInt
    rate_limit_delay: NonNegativeInt
    max_retries: NonNegativeInt
    ping_interval: PositiveInt
    ping_timeout: PositiveInt
    close_timeout: PositiveInt
    max_message_size: PositiveInt


class BucketDetailsConfig(BaseConfigModel):
    seaweedfs_s3_endpoint: AnyUrl
    seaweedfs_access_key: SecretStr
    seaweedfs_secret_key: SecretStr
    tickers_s3_path: str
    seaweedfs_s3_buckets: List[str] = Field(default_factory=list)

    @field_validator("tickers_s3_path")
    @classmethod
    def validate_s3_path(cls, value: str) -> str:
        if not value.startswith(("s3://", "s3a://")):
            raise ValueError("tickers_s3_path must start with s3:// or s3a://")
        return value

    @field_validator("seaweedfs_s3_buckets", mode="before")
    @classmethod
    def parse_bucket_list(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            buckets = [b.strip().strip("'\"") for b in raw.split(",") if b.strip()]
            return buckets
        return []


class KafkaConnectionConfig(BaseConfigModel):
    kafka_servers: List[str] = Field(default_factory=list)
    kafka_topic: str
    tickers_csv: str
    producer_acks: str = "all"
    producer_retries: NonNegativeInt = 3
    producer_max_in_flight: PositiveInt = 1
    consumer_group_id: str
    starting_offsets: str = "latest"
    fail_on_data_loss: bool = False
    max_offsets_per_trigger: PositiveInt = 50000
    fetch_min_bytes: NonNegativeInt = 1048576
    fetch_max_wait_ms: PositiveInt = 500
    max_partition_fetch_bytes: PositiveInt = 10485760

    @field_validator("kafka_servers", mode="before")
    @classmethod
    def parse_kafka_servers(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            servers = [s.strip().strip("'\"") for s in raw.split(",") if s.strip()]
            return servers
        return []

    @field_validator("kafka_servers")
    @classmethod
    def require_kafka_servers(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("kafka_servers must not be empty")
        return value


class LoadConsumerPerformanceConfig(BaseConfigModel):
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: PositiveInt = 1000
    session_timeout_ms: PositiveInt = 10000
    heartbeat_interval_ms: PositiveInt = 3000
    max_poll_records: PositiveInt = 500
    consumer_timeout_ms: PositiveInt = 5000


class LoadProcessingConfigurationConfig(BaseConfigModel):
    batch_processing: bool = False
    batch_size: PositiveInt = 100
    processing_timeout: PositiveFloat = 30.0


class MonitoringAndMetricsConfig(BaseConfigModel):
    metrics_interval: PositiveInt = 50
    health_check_interval: PositiveFloat = 30.0
    pushgateway_url: str = "prometheus-pushgateway:9091"
    metrics_prefix: str = "polyhorizon_streaming"
    enable_metrics: bool = True


class ErrorHandlingConfig(BaseConfigModel):
    max_consecutive_errors: PositiveInt = 10
    error_backoff_seconds: PositiveFloat = 5.0


class StreamingStorageConfig(BaseConfigModel):
    @model_validator(mode="after")
    def validate_gold_source(self):
        from polyhorizon.core.dataset_release import canonical_path
        if canonical_path(self.source_table) != canonical_path(self.gold):
            raise ValueError("source_table must match the Gold output path")
        return self

    bronze: str
    gold: str
    cp_bronze: str
    cp_gold: str
    dead_letter: str
    cp_dead_letter: str
    source_table: str
    feature_output_path: str
    trigger_interval: str = "30 seconds"
    maintenance_batch_interval: PositiveInt
    retention_hours: PositiveInt

    @field_validator(
        "bronze",
        "gold",
        "cp_bronze",
        "cp_gold",
        "dead_letter",
        "cp_dead_letter",
        "source_table",
        "feature_output_path",
    )
    @classmethod
    def validate_storage_path(cls, value: str) -> str:
        if not value:
            raise ValueError("Storage path must not be empty")
        valid_prefixes = ("s3a://", "s3://", "file://")
        if not value.startswith(valid_prefixes):
            raise ValueError(
                f"Storage path must start with one of {valid_prefixes}: {value}"
            )
        return value


class SparkConnectionConfig(BaseConfigModel):
    spark_log_level: str = "WARN"
    spark_master: str
    packages: List[str] = Field(default_factory=list)
    conf: Dict[str, str] = Field(default_factory=dict)


class FeaturesConfig(BaseConfigModel):
    freq: str
    lookback_bars: PositiveInt
    watermark_delay: str = "2 minutes"
    late_data_strategy: str = "dead_letter"
    late_data_threshold: Optional[str] = None
    raw_symbol_col: str = "symbol"
    raw_price_col: str = "price"
    raw_volume_col: str = "volume"
    raw_timestamp_col: str = "timestamp"
    time_col: str
    symbol_col: str
    open_col: str
    high_col: str
    low_col: str
    close_col: str
    volume_col: str

    @field_validator("late_data_strategy")
    @classmethod
    def validate_late_data_strategy(cls, value: str) -> str:
        allowed = {"drop", "dead_letter"}
        if value not in allowed:
            raise ValueError(f"late_data_strategy must be one of {sorted(allowed)}")
        return value

class MarketScheduleConfig(BaseConfigModel):
    enabled: bool = True
    calendar: str = "NYSE"
    timezone: str = "America/New_York"
    shutdown_grace_minutes: PositiveInt = 5
    
    
class LoggingConfig(BaseConfigModel):
    level: str = "INFO"
    format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    log_dir: str = "logs"
    log_to_file: bool = True
    log_json_format: bool = False
    max_file_size: PositiveInt = 10 * 1024 * 1024
    backup_count: PositiveInt = 5
    alert_emails: List[str]


class Config(BaseConfigModel):
    project: ProjectConfig
    finnhub_connection: FinnhubConnectionConfig
    bucket_details: BucketDetailsConfig
    kafka_connection: KafkaConnectionConfig
    load_consumer_performance: LoadConsumerPerformanceConfig
    load_processing_configuration: LoadProcessingConfigurationConfig
    monitoring_and_metrics: MonitoringAndMetricsConfig
    error_handling: ErrorHandlingConfig
    streaming_storage: StreamingStorageConfig
    spark_connection: SparkConnectionConfig
    features: FeaturesConfig
    market_schedule: MarketScheduleConfig
    logging: LoggingConfig

    @model_validator(mode="after")
    def validate_storage_buckets(self) -> "Config":
        buckets = set(self.bucket_details.seaweedfs_s3_buckets)
        if not buckets:
            return self

        storage_paths = [
            self.streaming_storage.bronze,
            self.streaming_storage.gold,
            self.streaming_storage.cp_bronze,
            self.streaming_storage.cp_gold,
            self.streaming_storage.dead_letter,
            self.streaming_storage.cp_dead_letter,
            self.streaming_storage.source_table,
            self.streaming_storage.feature_output_path,
        ]

        for path in storage_paths:
            if path.startswith(("s3a://", "s3://")):
                bucket = path.split("://", 1)[1].split("/", 1)[0]
                if bucket not in buckets:
                    raise ValueError(
                        f"Storage path bucket '{bucket}' is not in seaweedfs_s3_buckets"
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

        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        project_root = Path(__file__).resolve().parents[1]
        default_path = project_root / "configs/spark_features.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)


def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
