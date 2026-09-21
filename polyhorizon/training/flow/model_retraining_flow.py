from typing import Optional, Dict, Any

import mlflow
from prefect import flow, get_run_logger

from polyhorizon.training.configs.settings import load_config, Config
from polyhorizon.training.tasks.model_retraining_tasks import (
    drift_detection_task,
    get_last_best_params,
)
from polyhorizon.training.tasks.model_training_tasks import (
    preprocess_data,
    build_datasets,
    train_tft_model_task,
    tune_tft_model_task,
    compare_and_promote_task,
)
from polyhorizon.training.tasks.model_activation_tasks import activate_champion_task
from polyhorizon.training.utils.feature_lineage import (
    fetch_feature_lineage,
    log_feature_lineage_mlflow,
)
from polyhorizon.training.experiments.mlflow_tracking.factory import init_mlflow

# Drift detection → (if drifted) preprocess latest data → reuse last best params or re‑tune → train → compare + promote + email.
""" 
Drift detection
If drift threshold exceeded:
    Preprocess latest data
    Load last HPO params (or default)
    Train model
    Compare + promote + email
"""
@flow(
    name="TFT Model Retraining Flow",
    retries=1,
    retry_delay_seconds=300,
    timeout_seconds=24 * 60 * 60,
)
def model_retraining_flow(
    config_path: Optional[str] = None,
    run_tuning: bool = False,
) -> Dict[str, Any]:
    logger = get_run_logger()
    config: Config = load_config(config_path)

    logger.info("Starting retraining flow with drift detection.")

    init_mlflow(config)
    is_nested = mlflow.active_run() is not None
    with mlflow.start_run(run_name="tft_model_retraining_flow", nested=is_nested):
        mlflow.set_tag("flow_name", "model_retraining_flow")
        mlflow.set_tag("run_type", "retraining")
        mlflow.set_tag("model_name", config.model_registry.name)
        mlflow.set_tag("run_tuning", str(run_tuning))
        lineage = fetch_feature_lineage(config)
        log_feature_lineage_mlflow(lineage)

        needs_retraining, cur_df, drift_payload = drift_detection_task(config)
        if not needs_retraining:
            logger.info("No retraining needed based on drift thresholds.")
            return {
                "retrained": False,
                "drift": drift_payload,
            }

        processed_df = preprocess_data(cur_df, config)
        training_ds, validation_ds = build_datasets(processed_df, config)

        if run_tuning:
            params = tune_tft_model_task(
                config=config,
                training_ds=training_ds,
                validation_ds=validation_ds,
                save_path="artifacts/best_params.json",
            )
        else:
            params = get_last_best_params(config)

        model_metadata = train_tft_model_task(
            config=config,
            training_ds=training_ds,
            validation_ds=validation_ds,
            params=params,
        )

        promotion_result = compare_and_promote_task(
            config=config,
            challenger_metadata=model_metadata,
            inference_ds=validation_ds,
        )
        activation_result = activate_champion_task(
            promotion_result=promotion_result,
            model_metadata=model_metadata,
        )

        logger.info("Retraining flow complete.")
        return {
            "retrained": True,
            "drift": drift_payload,
            "model_metadata": model_metadata,
            "promotion_result": promotion_result,
            "activation_result": activation_result,
        }


if __name__ == "__main__":
    model_retraining_flow()
