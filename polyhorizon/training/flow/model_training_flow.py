from typing import Optional, Dict, Any
import time

import mlflow
from prefect import flow, get_run_logger
from polyhorizon.training.configs.settings import load_config, Config
from polyhorizon.training.utils.metrics import TrainingMetricsPublisher
from polyhorizon.training.tasks.model_training_tasks import (
    read_data,
    preprocess_data,
    build_datasets,
    tune_tft_model_task,
    train_tft_model_task,
    compare_and_promote_task,
)
from polyhorizon.training.tasks.model_activation_tasks import activate_champion_task
from polyhorizon.training.utils.feature_lineage import (
    fetch_feature_lineage,
    log_feature_lineage_mlflow,
)
from polyhorizon.training.experiments.mlflow_tracking.factory import init_mlflow

# Data ingest → preprocessing → dataset build → optional HPO → training → compare + promote + email.
""" 
Read data
Preprocess
Build datasets
Optional HPO
Train model
Compare + promote + email
"""
@flow(
    name="TFT Model Training Flow",
    retries=1,
    retry_delay_seconds=300,
    timeout_seconds=24 * 60 * 60,
)
def model_training_flow(
    config_path: Optional[str] = None,
    run_tuning: bool = False,
) -> Dict[str, Any]:
    logger = get_run_logger()
    config: Config = load_config(config_path)
    metrics_publisher = TrainingMetricsPublisher(config.monitoring)
    start_time = time.time()
    model_metadata: Dict[str, Any] | None = None
    promotion_result: Dict[str, Any] | None = None
    success = False

    logger.info("Starting training flow.")

    init_mlflow(config)
    is_nested = mlflow.active_run() is not None
    try:
        with mlflow.start_run(run_name="tft_model_training_flow", nested=is_nested):
            mlflow.set_tag("flow_name", "model_training_flow")
            mlflow.set_tag("run_type", "training")
            mlflow.set_tag("model_name", config.model_registry.name)
            mlflow.set_tag("run_tuning", str(run_tuning))
            lineage = fetch_feature_lineage(config)
            log_feature_lineage_mlflow(lineage)

            raw_df = read_data(config)
            processed_df = preprocess_data(raw_df, config)
            training_ds, validation_ds = build_datasets(processed_df, config)

            params = None
            if run_tuning:
                params = tune_tft_model_task(
                    config=config,
                    training_ds=training_ds,
                    validation_ds=validation_ds,
                    save_path="artifacts/best_params.json",
                )

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

            success = True
            logger.info("Training flow complete.")
            return {
                "model_metadata": model_metadata,
                "promotion_result": promotion_result,
                "activation_result": activation_result,
            }
    except Exception:
        logger.exception("Training flow failed.")
        raise
    finally:
        duration = time.time() - start_time
        metrics_publisher.push_run_metrics(duration_seconds=duration, success=success)
        if model_metadata is not None:
            metrics_publisher.push_validation_metrics(model_metadata.get("val_loss"))
        if promotion_result is not None:
            metrics_publisher.push_promotion_metrics(promotion_result)


if __name__ == "__main__":
    model_training_flow()
