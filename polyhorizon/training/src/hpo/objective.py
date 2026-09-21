import optuna
import mlflow
import os
from optuna.integration import PyTorchLightningPruningCallback
from pytorch_lightning import Trainer
from pytorch_forecasting import TimeSeriesDataSet
from pytorch_lightning.callbacks import EarlyStopping

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.src.models import create_tft_model_with_hparams
from polyhorizon.training.src.training.processing_unit import get_accelerator
from polyhorizon.training.src.hpo.parameter_sampler import TFTParameterSampler

from polyhorizon.training.utils.logger import get_logger
logger = get_logger("TFTObjective")

""" 
Fresh model per trial
Fresh Trainer per trial
No global state mutation
Nested MLflow runs per trial
Optuna pruning + EarlyStopping together (great combo)
Uses config-driven batch size / epochs
"""

class TFTObjective:
    """Executes a single training trial as a callable object."""
    def __init__(self, 
                 config: Config, 
                 training: TimeSeriesDataSet, 
                 validation: TimeSeriesDataSet, 
                 sampler: TFTParameterSampler) -> None:
        
        self.config = config
        self.training = training
        self.validation = validation
        self.sampler = sampler

    def __call__(self, trial: optuna.Trial) -> float:
        # 1. Sample Hyperparameters
        hparams = self.sampler.sample(trial)
        
        # CPU worker calculation
        num_cpu = os.cpu_count() or 1
        num_workers = min(self.config.optuna.num_workers, max(1, num_cpu - 1))

        # 2. Setup Dataloaders
        train_loader = self.training.to_dataloader(
            train=True, 
            batch_size=self.config.optuna.batch_size, 
            num_workers=num_workers,
            persistent_workers=True if num_workers > 0 else False,
            pin_memory=True,
        )
        val_loader = self.validation.to_dataloader(
            train=False,
            batch_size=self.config.optuna.batch_size,
            num_workers=num_workers,
            persistent_workers=True if num_workers > 0 else False,
            pin_memory=True,

        )

        # 3. Execution inside a Nested MLflow Run (Child Run)
        with mlflow.start_run(nested=True, run_name=f"trial_{trial.number}"):
            mlflow.log_params(hparams)
            
            model = create_tft_model_with_hparams(
                training_dataset=self.training,
                base_config=self.config.model,
                **hparams
            )
            
            # --- COMBINED CALLBACKS ---
            callbacks = [
                # 1. Pruning: Kills this trial if it's worse than others at the same epoch
                PyTorchLightningPruningCallback(trial, monitor="val_loss"),
                # 2. Early Stopping: Stops this trial if it stops improving (local efficiency)
                EarlyStopping(
                    monitor="val_loss", 
                    patience=self.config.optuna.early_stop_patience, # e.g., 5 epochs
                    mode="min"
                )
            ]

            accelerator_config = get_accelerator()
            trainer = Trainer(
                max_epochs=self.config.optuna.max_epochs,
                accelerator=accelerator_config["accelerator"],
                devices=accelerator_config["devices"],
                callbacks=callbacks,
                logger=False, 
                enable_checkpointing=False
            )

            trainer.fit(model, train_loader, val_loader)
            
            # val_loss = trainer.callback_metrics.get("val_loss", float("inf")).item()
            val_metric = trainer.callback_metrics.get("val_loss")

            if val_metric is None:
                val_loss = float("inf")
            else:
                val_loss = val_metric.item() if hasattr(val_metric, "item") else float(val_metric)

            mlflow.log_metric("val_loss", val_loss)
            return val_loss
