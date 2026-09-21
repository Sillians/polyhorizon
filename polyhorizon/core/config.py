# polyhorizon/common/config.py
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    env: str = "local"

    kafka_bootstrap_servers: str
    s3_endpoint: str

    class Config:
        env_file = ".env"

settings = Settings()
