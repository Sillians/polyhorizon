import pandas as pd
from prefect import task, get_run_logger
from pytorch_forecasting import TimeSeriesDataSet

from polyhorizon.training.src.training.trainer import TFTTrainer
from polyhorizon.training.src.training.tuner import TFTTuner
from polyhorizon.training.src.governance.champs_challenger_model_judge import ModelJudge
from polyhorizon.training.src.governance.promotion_email_alert import send_promotion_alert

from polyhorizon.training.src.data.generate_training_dataframe import PostgresDataLoader
from polyhorizon.training.src.data.preprocessing import TFTDataPreprocessor
from polyhorizon.training.src.data.dataset import TFTDatasetFactory

from polyhorizon.training.configs.settings import Config

@task(name="Read data from Postgres", retries=3, retry_delay_seconds=[10, 20, 60])
def read_data(config: Config):
    return PostgresDataLoader(config).fetch_data(track_mlflow=True)

@task(name="Process and feature engineer data", retries=3, retry_delay_seconds=[10, 20, 60])
def preprocess_data(df: pd.DataFrame, config: Config):
    return TFTDataPreprocessor(config).process(df)

@task(name="Create TimeSeriesDataset", retries=3, retry_delay_seconds=[10, 20, 60])
def build_datasets(df: pd.DataFrame, config: Config):
    return TFTDatasetFactory(config).create_datasets(df)


@task(name="tune_tft_model", retries=0, retry_delay_seconds=[10, 20, 60])
def tune_tft_model_task(config: Config, 
                        training_ds: TimeSeriesDataSet, 
                        validation_ds: TimeSeriesDataSet, 
                        save_path: str):
    logger = get_run_logger()
    logger.info("Starting TFT hyperparameter tuning...")
    
    assert isinstance(training_ds, TimeSeriesDataSet)
    assert isinstance(validation_ds, TimeSeriesDataSet)

    tuner = TFTTuner(config=config, 
                     training=training_ds, 
                     validation=validation_ds
                     )
    logger.info(f"Tuning complete. Saved to {save_path}")
    return tuner.tune(save_path=save_path)


@task(name="train_tft_model", retries=0, retry_delay_seconds=[10, 20, 60])
def train_tft_model_task(config: Config, 
                         training_ds: TimeSeriesDataSet, 
                         validation_ds: TimeSeriesDataSet, 
                         params: dict | None):
    logger = get_run_logger()
    logger.info("Starting TFT model training...")

    logger.info("Trains the final model using optimized parameters")
    trainer = TFTTrainer(config, training=training_ds, validation=validation_ds)
    return trainer.train(
        model_params=params
    )

@task(name="Compares Challenger vs Champion")
def promote_model_logic_task(config: Config):
    logger = get_run_logger()
    logger.info("Starting model promotion logic")
    return ModelJudge(config)


@task(name="Compare, Promote, and Alert")
def compare_and_promote_task(
    config: Config,
    challenger_metadata: dict,
    inference_ds: TimeSeriesDataSet,
):
    logger = get_run_logger()
    logger.info("Comparing challenger to champion and evaluating promotion.")
    judge = ModelJudge(config)
    result = judge.compare_and_promote_with_metrics(challenger_metadata, inference_ds)

    if result["promoted"]:
        challenger_metrics = result["challenger_metrics"]
        metrics = {
            "challenger_hit_rate": challenger_metrics["hit_rate"],
            "challenger_mae": challenger_metrics["mae"],
            "improvement_pct": float(result["improvement"] or 0.0),
            "stability": challenger_metrics.get("stability"),
        }
        send_promotion_alert(
            model_metadata=challenger_metadata,
            metrics=metrics,
            config=config,
        )

    return result

