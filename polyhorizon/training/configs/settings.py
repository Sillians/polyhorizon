import yaml
import os
import re
from pathlib import Path
from typing import List, Optional, Union
from pydantic import BaseModel, Field
from typing import Literal
from pydantic import model_validator

from dotenv import load_dotenv
load_dotenv()

_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(?::-[^}]*)?\}")

# Pydantic model = type definitions
class ProjectConfig(BaseModel):
    """Project metadata."""

    name: str
    version: str


class DataConfig(BaseModel):
    """Data loading and validation configuration."""

    parquet_path: str
    min_days_required: int
    required_symbols: int
    db_schema: str
    offline_table_name: str
    snapshot_table_name: str


class FeatureConfig(BaseModel):
    target: str
    group_ids: List[str]
    static_categoricals: List[str]
    time_varying_known_categoricals: List[str]
    time_varying_known_reals: List[str]
    time_varying_unknown_reals: List[str]
    feature_cols: List[str]
    

class TrainingConfig(BaseModel):
    """Training parameters."""

    max_encoder_length: int
    max_prediction_length: int
    batch_size: int
    num_workers: int
    max_epochs: int
    gradient_clip_val: float
    quantiles: List[float]
    validation_ratio: float = Field(gt=0, lt=1)
    early_stopping_patience: int
    lr_reduce_patience: int


class OptunaConfig(BaseModel):
    """Hyperparameter optimization configuration."""

    n_trials: int
    timeout_hours: int
    direction: Literal["minimize", "maximize"]
    max_epochs: int
    batch_size: int
    num_workers: int
    early_stop_patience: int
    storage_url: str 
    seed: int


class FloatParam(BaseModel):
    low: float
    high: float
    log: bool
    
    @model_validator(mode="after")
    def check_range(self):
        if self.low >= self.high:
            raise ValueError("low must be < high")
        return self

class IntRange(BaseModel):
    min: int
    max: int
    
    @model_validator(mode="after")
    def check_range(self):
        if self.min >= self.max:
            raise ValueError("min must be < max")
        return self


class FloatRange(BaseModel):
    low: float
    high: float

    @model_validator(mode="after")
    def check_range(self):
        if self.low >= self.high:
            raise ValueError("low must be < high")
        return self


class IntParam(BaseModel):
    min: int
    max: int
    step: int
    
    @model_validator(mode="after")
    def check_range(self):
        if self.min >= self.max:
            raise ValueError("min must be < max")
        return self


class HyperparameterRanges(BaseModel):
    learning_rate: FloatParam
    hidden_size: IntParam
    dropout: FloatRange
    attention_head_size: IntParam
    hidden_continuous_size: IntParam

    
# class HyperparameterRanges(BaseModel):
#     """Search space for hyperparameter optimization."""

#     learning_rate: dict
#     hidden_size: dict
#     dropout: dict
#     attention_head_size: dict
#     hidden_continuous_size: dict
    



class MLflowConfig(BaseModel):
    """MLflow tracking and experiment configuration."""
    
    experiment_name: str
    tracking_uri: str
    backend_store: str
    tags: dict[str, str] = Field(default_factory=dict)


class ModelRegistryConfig(BaseModel):
    """Model registry configuration."""

    name: str
    champion_alias: str
    staging_alias: str
    artifact_path: str


class HorizonGate(BaseModel):
    """Per-horizon promotion gates for forecast quality."""

    horizon_max: int
    max_wape: float | None = None
    max_mae: float | None = None
    max_smape: float | None = None
    min_coverage: float | None = None


class GovernanceConfig(BaseModel):
    """Champion-challenger evaluation and promotion thresholds."""

    direction_threshold: float = 0.001
    improvement_threshold: float = 0.02
    max_mae_degradation: float = 0.10
    score_weights: List[float] = Field(default_factory=lambda: [0.7, 0.3])
    stability_max_std: float | None = None
    horizon_gates: List[HorizonGate] = Field(default_factory=list)
    minimum_evaluation_samples: int = Field(default=100, ge=1)
    minimum_baseline_mae_improvement: float = Field(default=0.02, ge=0, lt=1)
    maximum_calibration_error: float = Field(default=0.10, ge=0, lt=1)



class DriftDetection(BaseModel):
    """Drift threshold value"""
    
    reference_days: int    # 6 months
    current_days: int      # Recent 30 days
    threshold: float
    threshold_auc: float
    threshold_wasserstein: float
    ignore_columns: List[str]
    
    @model_validator(mode="after")
    def check_days(self):
        if self.current_days >= self.reference_days:
            raise ValueError("current_days must be smaller than reference_days")
        return self



class LoggingConfig(BaseModel):
    """Logging configuration."""
 
    log_level: str
    log_dir: str
    log_format: str
    log_date_format: str
    log_to_file: bool
    log_json_format: bool
    max_file_size: int
    backup_count: int
    alert_emails: List[str]


class MonitoringConfig(BaseModel):
    """Prometheus Pushgateway configuration for training metrics."""

    enable_metrics: bool = False
    pushgateway_url: str = "prometheus-pushgateway:9091"
    metrics_prefix: str = "training"


class ModelConfig(BaseModel):
    """Default model hyperparameters."""

    learning_rate: float
    hidden_size: int
    attention_head_size: int
    dropout: float
    hidden_continuous_size: int
    output_size: int  # p10, p50, p90
    quantiles: List[float] = Field(default_factory=lambda: [0.1, 0.5, 0.9])
    log_interval: int
    reduce_on_plateau_patience: int

  
  
class ConnectionParams(BaseModel):
    """Postgres connection parameters."""

    host: str
    port: int
    database: str
    user: Union[str, None]
    password: Union[str, None]
    connect_timeout: int

    model_config = {"validate_default": True, "coerce_numbers_to_str": False}



class Config(BaseModel):
    project: ProjectConfig
    data: DataConfig
    features: FeatureConfig
    training: TrainingConfig
    optuna: OptunaConfig
    hyperparameters: HyperparameterRanges
    mlflow: MLflowConfig
    model_registry: ModelRegistryConfig
    governance: GovernanceConfig = Field(default_factory=GovernanceConfig)
    drift: DriftDetection
    logging: LoggingConfig
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    model: ModelConfig = Field(default_factory=ModelConfig)
    connection_parameters: ConnectionParams

    @model_validator(mode="after")
    def validate_model_contract(self) -> "Config":
        if self.model.output_size != len(self.training.quantiles):
            raise ValueError("model.output_size must match training.quantiles length")
        if self.model.quantiles != self.training.quantiles:
            raise ValueError("model.quantiles and training.quantiles must match")
        if sorted(self.training.quantiles) != self.training.quantiles:
            raise ValueError("training.quantiles must be sorted ascending")
        if 0.5 not in self.training.quantiles:
            raise ValueError("training.quantiles must include the median 0.5")
        return self


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

        def find_unresolved(value):
            if isinstance(value, str):
                return [match.group(1) for match in _ENV_PATTERN.finditer(value)]
            if isinstance(value, dict):
                return [name for item in value.values() for name in find_unresolved(item)]
            if isinstance(value, list):
                return [name for item in value for name in find_unresolved(item)]
            return []

        unresolved = sorted(set(find_unresolved(config_dict)))
        if unresolved:
            raise ValueError(
                "Unresolved environment variables in training config: "
                + ", ".join(unresolved)
            )
        
        # Resolve paths RELATIVE to yaml file location
        def resolve_path(path_str: str, relative_to: Path) -> str:
            return str((relative_to / path_str).resolve())
        
        config_path_parent = config_path.parent
        if "paths" in config_dict:
            for key in config_dict["paths"]:
                if key in ["base_dir", "training", "experiments", "src", "utils", "tasks"]:
                    config_dict["paths"][key] = resolve_path(config_dict["paths"][key], config_path_parent)


        # Coerce known int fields (after env expansion)
        int_paths = [
            ("connection_parameters", "port"),
            ("connection_parameters", "connect_timeout"),
            ("logging", "max_file_size"),
            ("logging", "backup_count"),
            # learning_rate: float
            # hidden_size: int
            # attention_head_size: int
            # dropout: float
            # hidden_continuous_size: int
            # output_size: int  # p10, p50, p90
            # log_interval: int
            # reduce_on_plateau_patience: int
            # Add more if needed, e.g., ("training", "batch_size")
        ]
        for section, field in int_paths:
            if section in config_dict and field in config_dict[section]:
                val = config_dict[section][field]
                try:
                    config_dict[section][field] = int(val)
                except (ValueError, TypeError):
                    raise ValueError(f"Config {section}.{field} must be integer: got {val!r}")

        config_dict.setdefault("model", {})

        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        default_path = Path(__file__).parent / "training_config.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)
    
    # @classmethod
    # def load_default(cls) -> "Config":
    #     # Find feast_features.yaml in project root
    #     project_root = Path(__file__).resolve().parents[1]  
    #     default_path = project_root / "configs/training_config.yaml"
    #     if not default_path.exists():
    #         raise FileNotFoundError(f"Config file not found: {default_path}")
    #     return cls.from_yaml(default_path)

def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
