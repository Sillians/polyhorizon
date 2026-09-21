"""Hyperparameter optimization workflow for TFT models."""
import json
import optuna
import mlflow
import os
import socket
from pathlib import Path
from polyhorizon.training.src.hpo.parameter_sampler import TFTParameterSampler
from polyhorizon.training.src.hpo.objective import TFTObjective
from pytorch_forecasting import TimeSeriesDataSet

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.experiments.mlflow_tracking.mlflow_tft_datasets_tracking import log_tft_dataset_metadata
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("TFTHPOTuner")

"""
# distributed tuning.
Proper Optuna sampler with seed
Persistent study storage
Timeout handling
Dataset lineage logging
Artifact persistence
Internal state tracking (self.best_params)
"""

# The Orchestrator (TFTTuner) 
class TFTTuner:
    def __init__(self, 
                 config: Config, 
                 training: TimeSeriesDataSet, 
                 validation: TimeSeriesDataSet):
        self.config = config
        self.sampler = TFTParameterSampler(self.config.hyperparameters)
        self.objective = TFTObjective(self.config, 
                                      training, 
                                      validation, 
                                      self.sampler)
        self.best_params: dict | None = None

    def tune(self, save_path: str = "best_params.json") -> dict:
        logger.info("Starting hyperparameter optimization")
        # Ensure we have an active experiment
        mlflow.set_experiment(self.config.mlflow.experiment_name)

        # Start the Parent Run
        with mlflow.start_run(run_name=f"TFT_HPO_Worker_{socket.gethostname()}_{os.getpid()}"):
            # 1. Log Dataset Metadata (Lineage)
            log_tft_dataset_metadata(self.objective.training, prefix="train")
            log_tft_dataset_metadata(self.objective.validation, prefix="val")
            
            def _flatten(d, parent_key=""):
                items = {}
                for k, v in d.items():
                    new_key = f"{parent_key}.{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.update(_flatten(v, new_key))
                    else:
                        items[new_key] = v
                return items

            mlflow.log_params({
                f"search_space.{key}": value
                for key, value in _flatten(self.sampler.search_space()).items()
            })
            
            # 2. Create and Optimize Study   
            # Use a DB backend (storage url)
            sampler = optuna.samplers.TPESampler(seed=self.config.optuna.seed)
            study = optuna.create_study(
                direction=self.config.optuna.direction,
                study_name="tft_tuning",
                storage=self.config.optuna.storage_url, # postgresql://user:pass@host:5432/optuna
                load_if_exists=True,
                sampler=sampler
            )
            try:
                # trace which machine ran which trial
                mlflow.set_tag("worker_host", socket.gethostname())
                mlflow.set_tag("worker_pid", os.getpid())
                mlflow.set_tag("optuna_study_name", study.study_name)
                
                optuna.logging.set_verbosity(optuna.logging.WARNING)
                study.optimize(self.objective, 
                            n_trials=self.config.optuna.n_trials,
                            timeout=(
                                    self.config.optuna.timeout_hours * 3600
                                    if self.config.optuna.timeout_hours
                                    else None
                                    ),
                )
            except Exception as e:
                mlflow.set_tag("tuning_failed", True)
                mlflow.set_tag("failure_reason", str(e)[:500])
                raise
            
            mlflow.log_param("seed", self.config.optuna.seed)


            # 3. Finalize
            best_params = study.best_params
            self.best_params = best_params

            mlflow.log_params(best_params)
            mlflow.log_metric("best_val_loss", study.best_value)
            if study.best_trial:
                mlflow.log_metric("best_trial_number", study.best_trial.number)
                mlflow.set_tag("n_trials_completed", len(study.trials))
            
            # save
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "w") as f:    
                json.dump({
                    "best_params": best_params,
                    "best_value": study.best_value,
                    "best_trial": study.best_trial.number,
                    }, f, indent=2)

            mlflow.log_artifact(str(save_path))
            
        logger.info(f"Best parameters: {best_params}")
        return best_params
    
    
    def get_best_params(self) -> dict:
        if self.best_params is None:
            raise ValueError("No tuning has been run yet.")
        return self.best_params
