"""MLflow utilities for experiment tracking and model registry."""

import mlflow
from mlflow.tracking import MlflowClient
import logging
import os
import tempfile
from typing import Dict, Optional
import torch

from polyhorizon.training.configs.settings import Config, ModelRegistryConfig

logger = logging.getLogger("mlflow_utils")


def init_mlflow(config: Config) -> None:
    """Initialize MLflow tracking and experiment.

    Args:
        config: Full configuration object with MLflow settings
    """
    mlflow.set_tracking_uri(config.mlflow.tracking_uri)
    mlflow.set_experiment(config.mlflow.experiment_name)

    # Set tags for the entire project
    client = MlflowClient()
    try:
        exp = client.get_experiment_by_name(config.mlflow.experiment_name)
        if exp:
            client.set_experiment_tag(exp.experiment_id, "project", config.project.name)
            client.set_experiment_tag(
                exp.experiment_id, "version", config.project.version
            )
    except Exception as e:
        logger.warning(f"Could not set experiment tags: {e}")

    logger.info(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
    logger.info(f"Experiment: {config.mlflow.experiment_name}")


def register_model(
    model_uri: str,
    registry_config: ModelRegistryConfig,
    alias: str = "staging",
    tags: Optional[Dict[str, str]] = None,
) -> str:
    """Register a model in MLflow Model Registry.

    Args:
        model_uri: URI of the model to register (e.g., runs:/run_id/artifact_path)
        registry_config: Model registry configuration
        alias: Alias to set for the registered version (default: staging)
        tags: Optional tags to set on the model version

    Returns:
        Version number of the registered model
    """
    client = MlflowClient()

    # Register model
    registered = mlflow.register_model(model_uri, registry_config.name)

    # Set tags
    if tags:
        for key, value in tags.items():
            client.set_model_version_tag(
                registry_config.name, registered.version, key, value
            )

    # Set alias
    client.set_registered_model_alias(registry_config.name, alias, registered.version)

    logger.info(f"Model v{registered.version} → @{alias}")
    return registered.version


def log_tft_datasets_mlflow(training_ds, validation_ds, run_name: str):
    with mlflow.start_run(run_name=run_name, nested=True):

        mlflow.log_param("num_training_series", len(training_ds))
        mlflow.log_param("num_validation_series", len(validation_ds))

        # Persist datasets to temp folder for artifact logging
        with tempfile.TemporaryDirectory() as tmpdir:
            train_path = os.path.join(tmpdir, "tft_training_dataset.pt")
            val_path = os.path.join(tmpdir, "tft_validation_dataset.pt")

            torch.save(training_ds, train_path)
            torch.save(validation_ds, val_path)

            mlflow.log_artifact(train_path, artifact_path="datasets")
            mlflow.log_artifact(val_path, artifact_path="datasets")
