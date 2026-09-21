import mlflow
import mlflow.pytorch
from mlflow.tracking import MlflowClient

from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.model_artifacts")

def log_model_artifacts(model):
    from polyhorizon.core.target_contract import attach_target_contract
    contract = attach_target_contract(model, getattr(model, "dataset_parameters", {}))
    mlflow.pytorch.log_model(model, artifact_path="tft_model", metadata={"target_contract": contract})


def register_model_version(model_uri: str, 
                           registry_name: str, 
                           alias, 
                           tags=None):
    client = MlflowClient()
    registered = mlflow.register_model(model_uri, registry_name)

    if tags:
        for key, val in tags.items():
            client.set_model_version_tag(registry_name, registered.version, key, val)

    client.set_registered_model_alias(registry_name, alias, registered.version)
    return registered.version
