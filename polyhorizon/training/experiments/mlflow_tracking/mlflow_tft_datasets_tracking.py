import mlflow
import tempfile
import os
import torch
from typing import Any
from pytorch_forecasting import TimeSeriesDataSet
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("mlflow.dataset_logger")


# Main Functionality used (Edit accordingly)
def log_tft_dataset_metadata(dataset: TimeSeriesDataSet, prefix: str = "ds"):
    """
    Automated extraction and logging of TimeSeriesDataSet parameters to MLflow.
    """
    # 1. Structural Parameters
    params = {
        f"{prefix}.max_encoder_length": dataset.max_encoder_length,
        f"{prefix}.max_prediction_length": dataset.max_prediction_length,
        f"{prefix}.min_encoder_length": dataset.min_encoder_length,
        f"{prefix}.time_idx": dataset.time_idx,
        f"{prefix}.target": dataset.target,
        f"{prefix}.group_ids": dataset.group_ids,
    }

    # 2. Feature Lists (Categoricals and Reals)
    # We join these into strings to avoid MLflow's limit on parameter count
    feature_attrs = [
        "static_categoricals", "static_reals",
        "time_varying_known_categoricals", "time_varying_known_reals",
        "time_varying_unknown_categoricals", "time_varying_unknown_reals"
    ]

    for attr in feature_attrs:
        val = getattr(dataset, attr, [])
        if val:
            params[f"{prefix}.{attr}"] = ", ".join(val)

    # 3. Log everything in one efficient batch call
    mlflow.log_params(params)
    
    # 4. Log high-level stats as metrics
    mlflow.log_metric(f"{prefix}.num_samples", len(dataset))
    





# Remove functionality if un-useful
def log_tft_datasets_mlflow(training_ds: Any, 
                            validation_ds: Any, 
                            artifact_subpath: str = "tft_datasets",
                            run_name: str = "tft-dataset-creation"):
    with mlflow.start_run(run_name=run_name, nested=True):
        
        mlflow.log_param("training_series", training_ds)
        mlflow.log_param("validation_series", validation_ds)
        
        mlflow.log_param("num_training_series", len(training_ds))
        mlflow.log_param("num_validation_series", len(validation_ds))

        # Persist datasets to temp folder for artifact logging
        with tempfile.TemporaryDirectory() as tmpdir:
            train_path = os.path.join(tmpdir, "tft_training_dataset.pt")
            val_path = os.path.join(tmpdir, "tft_validation_dataset.pt")

            torch.save(training_ds, train_path)
            torch.save(validation_ds, val_path)

            mlflow.log_artifact(train_path, artifact_path=artifact_subpath)
            mlflow.log_artifact(val_path, artifact_path=artifact_subpath)
            
        logger.info("TFT datasets logged to MLflow artifacts/%s", artifact_subpath)

        mlflow.end_run()
