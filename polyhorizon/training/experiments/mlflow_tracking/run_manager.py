import mlflow
from typing import Optional
from contextlib import contextmanager

from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.run_manager")

# Updated Run Manager
def start_parent_run(name: Optional[str] = None,
                     **kwargs):
    """Start parent MLflow run only once (Prefect-level)."""
    if mlflow.active_run():
        return mlflow.active_run().info.run_id
    
    run = mlflow.start_run(run_name=name)
    return run.info.run_id


@contextmanager
def nested_run(name: str, nested: bool = True):
    """Create a clean nested MLflow run."""
    with mlflow.start_run(run_name=name, nested=nested) as run:
        yield run



