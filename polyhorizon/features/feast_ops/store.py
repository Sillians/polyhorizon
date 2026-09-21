from __future__ import annotations

from feast import FeatureStore

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.utils.logger import get_logger, log_execution_time

logger = get_logger("FeatureStoreInitialization")


@log_execution_time
def init_feature_store(repo_path: str | None = None, config: Config | None = None) -> FeatureStore:
    logger.info("Initialize and return a Feast FeatureStore for the given repo path.")
    config = config or load_config()
    resolved_repo_path = repo_path or config.paths.feast_repo_path
    return FeatureStore(repo_path=resolved_repo_path)
