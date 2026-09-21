import mlflow
from typing import Dict, Optional
from mlflow.tracking import MlflowClient

from polyhorizon.training.configs.settings import ModelRegistryConfig
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.registry")

def register_model(model_uri: str, 
                   registry_config: ModelRegistryConfig, 
                   alias: str = "staging", 
                   tags: Optional[Dict[str, str]] = None,
                   **kwargs) -> str:
    """
    Register a model and set alias+tags.
    Returns the registered version (string).
    """
    client = MlflowClient()
    registered = mlflow.register_model(model_uri, registry_config.name)

    if tags:
        for k, v in tags.items():
            client.set_model_version_tag(registry_config.name, registered.version, k, v)

    # set alias to the model version
    client.set_registered_model_alias(registry_config.name, alias, registered.version)
    logger.info("Model %s registered as version %s and aliased %s", registry_config.name, registered.version, alias)
    return registered.version
