import mlflow
from mlflow.tracking import MlflowClient
from contextlib import contextmanager

from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.Manager")

class MLflowManager:
    def __init__(self, config):
        self.config = config
        mlflow.set_tracking_uri(self.config.mlflow.tracking_uri)
        mlflow.set_experiment(self.config.mlflow.experiment_name)
        self.client = MlflowClient()

    @contextmanager
    def parent_run(self, run_name: str):
        with mlflow.start_run(run_name=run_name) as run:
            yield run

    @contextmanager
    def child_run(self, run_name: str):
        with mlflow.start_run(run_name=run_name, nested=True) as run:
            yield run
