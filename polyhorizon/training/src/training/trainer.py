import mlflow
import torch
import os
import mlflow.pytorch
import mlflow.data
from pytorch_lightning import Trainer
from typing import Optional, Dict, Any
from mlflow.tracking import MlflowClient
from mlflow.models import infer_signature
from pytorch_lightning.loggers import MLFlowLogger
from pytorch_lightning.callbacks import ModelCheckpoint, EarlyStopping, LearningRateMonitor
from pytorch_forecasting import TimeSeriesDataSet

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.src.models.tft import create_tft_model, create_tft_model_with_hparams
from polyhorizon.training.utils.logger import get_logger, log_execution_time, log_function_call
from polyhorizon.training.experiments.mlflow_tracking.mlflow_tft_datasets_tracking import log_tft_dataset_metadata


""" 
Correct MLflow–Lightning logger sync
Proper dataset lineage logging
Model registry with aliasing
Signature + requirements logging
Separation of factory/model creation
Nested run design compatible with HPO
"""

class TFTTrainer:
    """Orchestrates the training of a TFT model with integrated MLflow tracking."""

    def __init__(self, 
                 config: Config, 
                 training_ds: TimeSeriesDataSet, 
                 validation_ds: TimeSeriesDataSet):
        
        self.config = config
        self.training_ds = training_ds
        self.validation_ds = validation_ds
        self.client = MlflowClient()
        self.logger = get_logger("TFT_Trainer")
    
    @log_execution_time
    def train(self, model_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # 1. Use an active run if it exists, otherwise start a child run
        # This prevents the 'double run' issue common with PL + MLflow
        
        if mlflow.active_run():
            run_ctx = mlflow.start_run(run_name="tft_training_execution", nested=True)
        else:
            run_ctx = mlflow.start_run(run_name="tft_training_execution")

        with run_ctx as run:
            
            num_cpu = os.cpu_count() or 1
            num_workers = min(self.config.training.num_workers, max(1, num_cpu - 1))

            # 2. Setup DataLoaders
            train_loader = self.training_ds.to_dataloader(
                train=True, 
                batch_size=self.config.training.batch_size, 
                num_workers=num_workers,
            )
            val_loader = self.validation_ds.to_dataloader(
                train=False, 
                batch_size=self.config.training.batch_size, 
                num_workers=num_workers,
            )
            
            # Convert the underlying pandas dataframes to MLflow Dataset objects
            # This makes them appear in the "Datasets" tab in the UI
            train_mlflow_ds = mlflow.data.from_pandas(
                self.training_ds.data, 
                name="tft_training_set", 
                targets=self.training_ds.target
            )
            val_mlflow_ds = mlflow.data.from_pandas(
                self.validation_ds.data, 
                name="tft_validation_set", 
                targets=self.validation_ds.target
            )
            
            mlflow.log_input(train_mlflow_ds, context="training")
            mlflow.log_input(val_mlflow_ds, context="validation")
            
            # --- AUTO-LOGGING DATASET METADATA ---
            # Now obvious in the UI which features were used
            log_tft_dataset_metadata(self.training_ds, prefix="train")
            log_tft_dataset_metadata(self.validation_ds, prefix="val")

            # 3. Configure Lightning Logger to sync with THIS specific run
            mlf_logger = MLFlowLogger(
                experiment_name=self.config.mlflow.experiment_name,
                run_id=run.info.run_id,
                tracking_uri=mlflow.get_tracking_uri(),
                # tracking_uri=self.config.mlflow.tracking_uri
            )

            # 4. Instantiate Model (Modular Factory approach)
            model = self._init_model(model_params)
            
            callbacks = [
                ModelCheckpoint(monitor="val_loss", 
                                mode="min", 
                                save_top_k=1, 
                                filename="best-tft-{epoch:02d}-{val_loss:.4f}"
                                ),
                EarlyStopping(monitor="val_loss",
                                min_delta=1e-4,
                                patience=self.config.training.early_stopping_patience,
                                mode="min",
                                verbose=False),
                LearningRateMonitor(logging_interval="epoch")
            ]

            # 5. Trainer with automated callbacks
            trainer = Trainer(
                max_epochs=self.config.training.max_epochs,
                accelerator="auto", # Dynamically chooses GPU/MPS/CPU
                logger=mlf_logger,
                callbacks=callbacks,
                gradient_clip_val=self.config.training.gradient_clip_val,
                precision="32-true", # MPS performs best with 32-bit precision
                enable_model_summary=False,
            ) 

            # 6. Fit and Log
            trainer.fit(model,
                        train_dataloaders=train_loader,
                        val_dataloaders=val_loader)
            
            # 7. Model Registration
            # We capture the metadata returned from the registration helper
            self.logger.info("Logging model artifacts and registering...")
            model_metadata = self._log_and_register_model(model, run.info.run_id)
            
            # Add final metrics for convenience in the Prefect Flow
            # best_val_loss = trainer.callback_metrics.get("val_loss", 0.0)
            # # Convert tensor to float if needed
            # if hasattr(best_val_loss, 'item'):
            #     best_val_loss = best_val_loss.item()
                
            # final metrics
            metric = trainer.callback_metrics.get("val_loss")
            best_val_loss = float(metric.cpu().item()) if metric is not None else float("inf")

            self.logger.info(f"Final validation loss: {best_val_loss:.4f}")
            mlflow.log_metric("best_val_loss", best_val_loss)
            model_metadata["val_loss"] = best_val_loss
            
            return model_metadata


    def _init_model(self, model_params):
        """Encapsulates model creation logic."""
        # Note: create_tft_model would be your factory function defined elsewhere
        if model_params:
            return create_tft_model_with_hparams(training_dataset=self.training_ds,
                                                 base_config=self.config.model, 
                                                 **model_params)
        return create_tft_model(training_dataset=self.training_ds, 
                                config=self.config.model)

    @log_function_call
    def _log_and_register_model(self, model, run_id: str) -> Dict[str, Any]:
        """Saves, registers, and aliases the model, returning versioning metadata."""
        
        # 1. Infer Signature
        # sample_batch = next(iter(self.validation_ds.to_dataloader(batch_size=1)))
        # x, _ = sample_batch
        # signature = infer_signature(x, model(x))
        
        sample_batch = next(iter(self.validation_ds.to_dataloader(batch_size=1)))
        x, _ = sample_batch

        model.eval()
        with torch.no_grad():
            preds = model(x)

        signature = infer_signature(x, preds)

        
        ds_params = self.training_ds.get_parameters()
        # Persist the fitted categorical encoders and target normalizer with the
        # model artifact. Serving can then recreate the exact training dataset
        # contract instead of fitting new encoders at request time.
        model.dataset_parameters = ds_params
        from polyhorizon.core.target_contract import attach_target_contract
        target_contract = attach_target_contract(model, ds_params)
        mlflow.set_tag("dataset_parameters", str(ds_params))

        # 2. Log and Register in one step
        # This returns a ModelInfo object containing all registry metadata
        model_info = mlflow.pytorch.log_model(
            pytorch_model=model,
            artifact_path=self.config.model_registry.artifact_path,
            registered_model_name=self.config.model_registry.name,
            signature=signature,
            pip_requirements=["pytorch-forecasting", "pytorch-lightning", "mlflow", "torch", "numpy", "pandas"],
            extra_files=None,
            metadata={"target_contract": target_contract},
            await_registration_for=300  # seconds
        )
        
        # Manually log the params as a JSON artifact for extra safety
        mlflow.log_dict(ds_params, "dataset_parameters.json")

        model_version = model_info.registered_model_version

        # Record forecast semantics for serving
        self.client.set_model_version_tag(
            name=self.config.model_registry.name,
            version=model_version,
            key="forecast_horizon",
            value=str(self.config.training.max_prediction_length),
        )
        self.client.set_model_version_tag(
            name=self.config.model_registry.name,
            version=model_version,
            key="target_definition",
            value="1-step log return",
        )
        self.client.set_model_version_tag(
            name=self.config.model_registry.name,
            version=model_version,
            key="target_column",
            value=self.config.features.target,
        )
        
        # 3. Assign Alias (e.g., "challenger")
        staging_alias = self.config.model_registry.staging_alias
        if staging_alias:
            self.client.set_registered_model_alias(
                name=self.config.model_registry.name,
                alias=staging_alias,
                version=model_version
            )
            self.logger.info(f"Assigned alias '{staging_alias}' to version {model_version}")

        # 4. Construct SOTA URIs
        # registry_uri: used for model-centric operations
        # run_uri: used for artifact-centric operations
        registry_uri = f"models:/{self.config.model_registry.name}@{staging_alias}"
        mlflow.set_tag("deployed_model_uri", registry_uri)
        
        return {
            "run_id": run_id,
            "model_name": self.config.model_registry.name,
            "model_version": model_version,
            "model_uri": registry_uri,  # Downstream tasks use this
            "alias": staging_alias
        }
