from pathlib import Path
from typing import List, Optional
import os
import re
import yaml

from pydantic import BaseModel, Field, SecretStr, AnyUrl, field_validator
from pydantic.config import ConfigDict
from pydantic.types import PositiveInt, NonNegativeInt, PositiveFloat, NonNegativeFloat

from dotenv import load_dotenv
load_dotenv()


class BaseConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ProjectConfig(BaseConfigModel):
    """Project metadata."""

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
    universe_current_key: str = "universe/current.json"
    universe_max_age_hours: PositiveInt = 30

    @field_validator("tickers_s3_path")
    @classmethod
    def validate_s3_path(cls, value: str) -> str:
        if not value.startswith(("s3://", "s3a://")):
            raise ValueError("tickers_s3_path must start with s3:// or s3a://")
        return value


class KafkaConnectionConfig(BaseConfigModel):
    kafka_servers: List[str] = Field(default_factory=list)
    kafka_topic: str
    tickers_csv: str
    producer_acks: str = "all"
    producer_retries: NonNegativeInt = 3
    producer_max_in_flight: PositiveInt = 1
    consumer_group_id: str
    kafka_bootstrap_servers: str

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


class ErrorHandlingConfig(BaseConfigModel):
    max_consecutive_errors: PositiveInt = 10
    error_backoff_seconds: PositiveFloat = 5.0


class LoggingConfig(BaseConfigModel):
    """Logging configuration."""

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
    logging: LoggingConfig

    @classmethod
    def from_yaml(cls, config_path: Path | str) -> "Config":
        config_path = Path(config_path).resolve()
        with open(config_path) as f:
            config_dict = yaml.safe_load(f) or {}

        # Expand env vars
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
            elif isinstance(value, dict):
                for key, nested in value.items():
                    nested_path = f"{path}.{key}" if path else str(key)
                    find_unresolved(nested, nested_path)
            elif isinstance(value, list):
                for index, nested in enumerate(value):
                    find_unresolved(nested, f"{path}[{index}]")

        find_unresolved(config_dict)
        if unresolved:
            missing = ", ".join(f"{path}={value}" for path, value in unresolved)
            raise ValueError(f"Unresolved environment variables in config: {missing}")
        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        project_root = Path(__file__).resolve().parents[1]
        default_path = project_root / "configs/ingestion_features.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)


def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
