import os
import torch
import mlflow
import tempfile
from typing import Any

from polyhorizon.training.experiments.mlflow_tracking.run_manager import nested_run
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.dataset_logger")

# Handles dataset-specific nested runs.
def log_tft_datasets(training_ds: Any, 
                     validation_ds: Any, 
                     artifact_subpath: str = "tft_datasets", 
                     run_name: str = "tft-dataset-creation"):

    with nested_run(name=run_name, nested=True):
        mlflow.log_param("training_dataset_type", type(training_ds).__name__)
        mlflow.log_param("validation_dataset_type", type(validation_ds).__name__)

        with tempfile.TemporaryDirectory() as tmpdir:
            train_path = os.path.join(tmpdir, "tft_training_dataset.pt")
            val_path = os.path.join(tmpdir, "tft_validation_dataset.pt")

            torch.save(training_ds, train_path)
            torch.save(validation_ds, val_path)

            mlflow.log_artifact(train_path, artifact_path=artifact_subpath)
            mlflow.log_artifact(val_path, artifact_path=artifact_subpath)

        logger.info("TFT datasets logged to MLflow artifacts/%s", artifact_subpath)



