import mlflow
from typing import Optional, Dict, Any
from contextlib import contextmanager

from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.Service")

class MLflowService:
    """Single point of truth for MLflow operations."""
    
    @staticmethod
    def initialize(tracking_uri: str, 
                   experiment_name: str, 
                   tags: Optional[Dict] = None):
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(experiment_name)
        if tags:
            # Set tags at the experiment level if it's a new run
            client = mlflow.tracking.MlflowClient()
            exp = client.get_experiment_by_name(experiment_name)
            for k, v in tags.items():
                client.set_experiment_tag(exp.experiment_id, k, v)

    @staticmethod
    @contextmanager
    def run(name: str, 
            nested: bool = False, 
            tags: Optional[Dict] = None):
        """Standardized context manager for all runs."""
        with mlflow.start_run(run_name=name, nested=nested) as run:
            if tags:
                mlflow.set_tags(tags)
            yield run

    @staticmethod
    def log_params_from_config(config_obj: Any):
        """Extracts attributes from a config class and logs them."""
        params = {k: v for k, v in vars(config_obj).items() if not k.startswith('_')}
        mlflow.log_params(params)