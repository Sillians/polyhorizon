import yaml
import os
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field, model_validator

from dotenv import load_dotenv
load_dotenv()


class ProjectConfig(BaseModel):
    name: str
    description: str
    version: str


class LoggingConfig(BaseModel):
    level: str = "INFO"
    format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    alert_emails: List[str]


class BucketDetails(BaseModel):
    sp500companies_bucket_name: str
    seaweed_s3_endpoint: str
    aws_access_key_id: str
    aws_secret_access_key: str
    sp500_url: str


class UniversePolicyConfig(BaseModel):
    minimum_constituents: int = Field(default=450, ge=1)
    maximum_constituents: int = Field(default=550, ge=1)
    maximum_change_fraction: float = Field(default=0.10, gt=0, le=1)
    request_timeout_seconds: int = Field(default=30, ge=1, le=300)

    @model_validator(mode="after")
    def validate_bounds(self) -> "UniversePolicyConfig":
        if self.maximum_constituents < self.minimum_constituents:
            raise ValueError("maximum_constituents must be >= minimum_constituents")
        return self


class Config(BaseModel):
    project: ProjectConfig
    logging: LoggingConfig
    bucket_details: BucketDetails
    universe_policy: UniversePolicyConfig = Field(default_factory=UniversePolicyConfig)
    

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

        return cls(**config_dict)

    @classmethod
    def load_default(cls) -> "Config":
        project_root = Path(__file__).resolve().parents[1] 
        default_path = project_root / "configs/sp500_variables.yaml"
        if not default_path.exists():
            raise FileNotFoundError(f"Config file not found: {default_path}")
        return cls.from_yaml(default_path)

def load_config(config_path: Optional[Path | str] = None) -> Config:
    if config_path:
        return Config.from_yaml(config_path)
    return Config.load_default()
