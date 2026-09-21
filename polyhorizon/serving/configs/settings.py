from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Literal
import os
import re
import yaml

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.config import ConfigDict
from pydantic.types import PositiveInt

from dotenv import load_dotenv
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS, DEFAULT_SYMBOL
load_dotenv()


class BaseConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ProjectConfig(BaseConfigModel):
    name: str
    description: str
    version: str
    service_name: str = "polyhorizon.model_serving"


class APIConfig(BaseConfigModel):
    host: str = "0.0.0.0"
    port: PositiveInt = 8000
    root_path: str = ""
    version_prefix: str = "/v1"
    docs_enabled: bool = True
    cors_allow_origins: List[str] = Field(default_factory=list)
    cors_allow_methods: List[str] = Field(default_factory=lambda: ["GET", "POST"])
    cors_allow_headers: List[str] = Field(default_factory=lambda: ["*"])
    request_timeout_seconds: PositiveInt = 30

    @field_validator("cors_allow_origins", mode="before")
    @classmethod
    def parse_origins(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw == "*":
                return ["*"]
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [item.strip().strip("'\"") for item in raw.split(",") if item.strip()]
        return []


class LoggingConfig(BaseConfigModel):
    log_level: str = "INFO"
    log_dir: str = "logs"
    log_format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    log_date_format: str = "%Y-%m-%d %H:%M:%S"
    log_to_file: bool = True
    log_json_format: bool = False
    max_file_size: PositiveInt = 10 * 1024 * 1024
    backup_count: PositiveInt = 5
    alert_emails: List[str] = Field(default_factory=list)


class MonitoringConfig(BaseConfigModel):
    enable_metrics: bool = True
    metrics_path: str = "/metrics"


class SecurityConfig(BaseConfigModel):
    require_api_key: bool = False
    api_keys: List[str] = Field(default_factory=list)
    operator_api_keys: List[str] = Field(default_factory=list)
    api_key_header: str = "X-API-Key"
    rate_limit_enabled: bool = False
    rate_limit_per_minute: PositiveInt = 60
    rate_limit_prefix: str = "serving:rate-limit"
    exempt_paths: List[str] = Field(
        default_factory=lambda: [
            "/v1/health",
            "/v1/health/live",
            "/v1/health/ready",
            "/v1/metadata",
            "/metrics",
        ]
    )

    @field_validator("api_keys", "operator_api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, value):
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [item.strip().strip("'\"") for item in raw.split(",") if item.strip()]
        return []

    @model_validator(mode="after")
    def separate_operator_credentials(self) -> "SecurityConfig":
        if set(self.api_keys) & set(self.operator_api_keys):
            raise ValueError("operator_api_keys must be separate from consumer api_keys")
        return self

    @field_validator("exempt_paths", mode="before")
    @classmethod
    def parse_exempt_paths(cls, value):
        if isinstance(value, list):
            return [str(v).strip() for v in value if str(v).strip()]
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [item.strip().strip("'\"") for item in raw.split(",") if item.strip()]
        return [
            "/v1/health",
            "/v1/health/live",
            "/v1/health/ready",
            "/v1/metadata",
            "/metrics",
        ]


class MLflowConfig(BaseConfigModel):
    tracking_uri: str


class ModelRegistryConfig(BaseConfigModel):
    name: str
    champion_alias: str
    staging_alias: str
    artifact_path: str
    bootstrap_champion_alias: bool = True


class FeastConfig(BaseConfigModel):
    repo_path: str
    feature_store_yaml: str
    project_name: str
    feature_view: str
    feature_refs: List[str] = Field(default_factory=list)

    @field_validator("feature_refs", mode="before")
    @classmethod
    def parse_refs(cls, value):
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [item.strip().strip("'\"") for item in raw.split(",") if item.strip()]
        return []


class RedisConfig(BaseConfigModel):
    enabled: bool = True
    host: str = "localhost"
    port: PositiveInt = 6379
    db: int = 0
    password: Optional[str] = None
    key_prefix: str = "forecast"
    ttl_seconds: PositiveInt = 300


class ForecastConfig(BaseConfigModel):
    horizon: PositiveInt = 3
    quantiles: List[float] = Field(default_factory=lambda: [0.1, 0.5, 0.9])
    target_type: Literal["price", "return"] = "return"
    base_price_feature: str = "close"
    return_to_price_method: Literal["simple", "log"] = "log"
    output_decimals: int = 4

    @field_validator("quantiles", mode="before")
    @classmethod
    def parse_quantiles(cls, value):
        if isinstance(value, list):
            return [float(v) for v in value]
        if isinstance(value, str):
            raw = value.strip()
            if raw.startswith("[") and raw.endswith("]"):
                raw = raw[1:-1]
            return [float(v.strip()) for v in raw.split(",") if v.strip()]
        return [0.1, 0.5, 0.9]


class ClientMetadataConfig(BaseConfigModel):
    supported_symbols: List[str] = Field(default_factory=lambda: list(PRODUCT_SYMBOLS))
    bars_per_day: PositiveInt = 13
    horizon_days: List[PositiveInt] = Field(default_factory=lambda: [1, 2, 3])
    default_symbol: str = DEFAULT_SYMBOL
    currency: str = "USD"
    market_timezone: str = "America/New_York"
    feature_debug_enabled: bool = False
    model_reload_enabled: bool = False

    @model_validator(mode="after")
    def validate_defaults(self) -> "ClientMetadataConfig":
        normalized = [symbol.upper() for symbol in self.supported_symbols]
        if normalized != list(PRODUCT_SYMBOLS):
            raise ValueError("client_metadata.supported_symbols must match the shared product allowlist")
        if not normalized:
            raise ValueError("client_metadata.supported_symbols cannot be empty")
        self.supported_symbols = normalized
        self.default_symbol = self.default_symbol.upper()
        if self.default_symbol not in normalized:
            raise ValueError("client_metadata.default_symbol must be supported")
        if not self.horizon_days:
            raise ValueError("client_metadata.horizon_days cannot be empty")
        return self


class InferenceConfig(BaseConfigModel):
    max_encoder_length: PositiveInt
    max_prediction_length: PositiveInt
    time_idx_field: str = "time_idx"
    time_field: str = "event_timestamp"
    group_id_field: str = "symbol"
    freq: str = "30min"
    target: str = "target"
    static_categoricals: List[str] = Field(default_factory=lambda: ["symbol"])
    time_varying_known_categoricals: List[str] = Field(default_factory=list)
    time_varying_known_reals: List[str] = Field(default_factory=list)
    time_varying_unknown_reals: List[str] = Field(default_factory=list)


class OfflineStoreConfig(BaseConfigModel):
    db_schema: str
    offline_table_name: str
    snapshot_table_name: str
    history_rows: PositiveInt = 256


class ConnectionParams(BaseConfigModel):
    host: str
    port: PositiveInt
    database: str
    user: str
    password: str
    connect_timeout: PositiveInt


class PublicationConfig(BaseConfigModel):
    forecast_mode: Literal["post_close"] = "post_close"
    publication_delay_minutes: int = Field(default=60, ge=1, le=240)


class Config(BaseConfigModel):
    publication: PublicationConfig = Field(default_factory=PublicationConfig)
    project: ProjectConfig
    api: APIConfig
    logging: LoggingConfig
    monitoring: MonitoringConfig
    security: SecurityConfig
    mlflow: MLflowConfig
    model_registry: ModelRegistryConfig
    feast: FeastConfig
    redis: RedisConfig
    forecast: ForecastConfig
    client_metadata: ClientMetadataConfig = Field(default_factory=ClientMetadataConfig)
    inference: InferenceConfig
    offline_store: OfflineStoreConfig
    connection_parameters: ConnectionParams
    warmup_enabled: bool = True

    @model_validator(mode="after")
    def validate_forecast_quantiles(self) -> "Config":
        if 0.5 not in self.forecast.quantiles:
            raise ValueError("forecast.quantiles must include 0.5 for median output")
        if self.forecast.quantiles != sorted(self.forecast.quantiles):
            raise ValueError("forecast.quantiles must be sorted ascending")
        if self.offline_store.history_rows < self.inference.max_encoder_length:
            raise ValueError("offline_store.history_rows must cover max_encoder_length")
        if self.forecast.horizon > self.inference.max_prediction_length:
            raise ValueError("forecast.horizon cannot exceed max_prediction_length")
        max_client_horizon = max(self.client_metadata.horizon_days) * self.client_metadata.bars_per_day
        if max_client_horizon > self.inference.max_prediction_length:
            raise ValueError("client metadata horizons exceed max_prediction_length")
        privileged_ui = (
            self.client_metadata.feature_debug_enabled
            or self.client_metadata.model_reload_enabled
        )
        if privileged_ui and not self.security.require_api_key:
            raise ValueError(
                "feature debug and model reload require API-key authentication"
            )
        if privileged_ui and not self.security.operator_api_keys:
            raise ValueError("feature debug and model reload require separate operator_api_keys")
        return self

    @classmethod
    def from_yaml(cls, config_path: Path | str) -> "Config":
        config_path = Path(config_path).resolve()
        with open(config_path) as f:
            config_dict = yaml.safe_load(f) or {}

        def expand_env(value):
            if isinstance(value, str):
                # Support shell-style defaults in addition to ordinary ${VAR}
                # expansion so optional capabilities do not become mandatory
                # environment variables.
                pattern = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*):-([^}]*)\}")
                value = pattern.sub(lambda match: os.getenv(match.group(1), match.group(2)), value)
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

        # Resolve relative paths for Feast config
        config_dir = config_path.parent
        project_root = config_dir.parent.parent
        feast_cfg = config_dict.get("feast", {})
        for key in ["repo_path", "feature_store_yaml"]:
            if key in feast_cfg and isinstance(feast_cfg[key], str):
                path_value = Path(feast_cfg[key])
                if path_value.is_absolute():
                    feast_cfg[key] = str(path_value)
                else:
                    candidate = (config_dir / path_value).resolve()
                    if candidate.exists():
                        feast_cfg[key] = str(candidate)
                    else:
                        fallback = (project_root / path_value).resolve()
                        feast_cfg[key] = str(fallback)
        config_dict["feast"] = feast_cfg

        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        default_path = Path(__file__).parent / "serving_config.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)


def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
