import mlflow
from mlflow.tracking import MlflowClient

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.factory")

# Handles experiment + parent run only.
def init_mlflow(config: Config) -> None:
    """Configure MLflow tracking URI + experiment and set useful experiment tags."""
    mlflow.set_tracking_uri(config.mlflow.tracking_uri)
    mlflow.set_experiment(config.mlflow.experiment_name)

    client = MlflowClient()
    try:
        exp = client.get_experiment_by_name(config.mlflow.experiment_name)
        if exp:
            client.set_experiment_tag(exp.experiment_id, "project", config.project.name)
            client.set_experiment_tag(exp.experiment_id, "version", config.project.version)
    except Exception as exc:
        logger.warning("Could not set experiment tags: %s", exc)

    logger.info("MLflow initialized: %s (experiment=%s)", mlflow.get_tracking_uri(), config.mlflow.experiment_name)


def get_mlflow_client() -> MlflowClient:
    return MlflowClient()
