import torch
import tempfile
import mlflow.data
import pandas as pd
import numpy as np
from typing import Tuple
from pathlib import Path

from polyhorizon.training.configs.settings import Config
from pytorch_forecasting import TimeSeriesDataSet, GroupNormalizer
from polyhorizon.training.utils.logger import get_logger
from polyhorizon.training.src.data.temporal_split import (
    timestamp_split, validation_window_mask, assert_validation_targets_after_cutoff,
)


# Main Functionality Approach (Adjust parameters from root repo)
class TFTDatasetFactory:
    def __init__(self, config: Config):
        self.config = config
        self.features_cfg = config.features 
        self.train_cfg = config.training 
        self.logger = get_logger("TFTDatasetBuilder")

    def create_datasets(self, df: pd.DataFrame) -> Tuple[TimeSeriesDataSet, TimeSeriesDataSet]:
        """Orchestrates splitting, building, and tracking TFT datasets."""
        
        self.logger.info(
            f"Creating TFT datasets | Total rows: {len(df):,} | Validation ratio: {self.train_cfg.validation_ratio}"
        )
        
        # 1. Track the Input Data Lineage in MLflow
        # This shows exactly which 30m-bar snapshot was used
        dataset_source = mlflow.data.from_pandas(
            df, name="resampled_30m_bars", targets=self.features_cfg.target
        )
        mlflow.log_input(dataset_source, context="tft_dataset_creation")

        # 2. Split Logic
        # 1. Identify your feature columns
        feature_cols = self.config.features.feature_cols
        
        # 2. Drop rows where target or any essential features are NaN
        # This usually removes the first few rows (rolling stats) and last few rows (future target)
        df = df.dropna(subset=feature_cols).copy()

        # 3. Check for infinite values (common if you used log transforms on zero volume)
        df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=feature_cols)

        train_df, val_df, cutoff, boundaries = timestamp_split(
            df, self.features_cfg.group_ids, self.train_cfg.validation_ratio,
            self.train_cfg.max_encoder_length, self.train_cfg.max_prediction_length,
        )
        self.logger.info("Shared UTC training cutoff: %s", cutoff.isoformat())

        self.logger.info(f"Train rows: {len(train_df):,} | Validation rows: {len(val_df):,}")


        # 3. Build the Base Training Dataset
        self.logger.info("Building training TimeSeriesDataSet")
        training_ds = TimeSeriesDataSet(
            train_df,
            time_idx="time_idx", # column name of time of observation
            target=self.features_cfg.target, # column name of target to predict
            group_ids=self.features_cfg.group_ids, # column name(s) for timeseries IDs
            max_encoder_length=self.train_cfg.max_encoder_length, # how much history to use
            min_encoder_length=self.train_cfg.max_encoder_length // 2,
            max_prediction_length=self.train_cfg.max_prediction_length, # how far to predict into future
            min_prediction_length=1,
        
            # covariates static for a timeseries ID
            static_categoricals=self.features_cfg.static_categoricals,
            
            # covariates known and unknown in the future to inform prediction
            time_varying_known_reals=self.features_cfg.time_varying_known_reals,
            time_varying_known_categoricals=self.features_cfg.time_varying_known_categoricals,
            time_varying_unknown_reals=self.features_cfg.time_varying_unknown_reals,
            time_varying_unknown_categoricals=[],
            
            target_normalizer=GroupNormalizer(groups=self.features_cfg.group_ids),
            add_relative_time_idx=True,
            add_target_scales=True,
            add_encoder_length=True,
            allow_missing_timesteps=True
        )

        # 4. Generate Val and Inference sets from Training metadata
        self.logger.info("Building validation TimeSeriesDataSet (reusing training schema)")
        validation_ds = TimeSeriesDataSet.from_dataset(training_ds, 
                                                       val_df, 
                                                       stop_randomization=True)
        validation_ds = validation_ds.filter(
            lambda index: validation_window_mask(index, boundaries, self.features_cfg.group_ids)
        )
        earliest_target = assert_validation_targets_after_cutoff(
            validation_ds.decoded_index, val_df, self.features_cfg.group_ids, cutoff,
        )
        self.split_metadata = {
            "method": "global_distinct_timestamp_holdout_v1",
            "training_cutoff_utc": cutoff.isoformat(),
            "earliest_validation_target_utc": earliest_target.isoformat(),
            "validation_ratio": self.train_cfg.validation_ratio,
            "training_rows": len(train_df), "validation_windows": len(validation_ds),
            "encoder_context_rows": int((val_df.event_timestamp <= cutoff).sum()),
        }
        mlflow.log_dict(self.split_metadata, "temporal_split.json")
        
        # 5. Log Dataset Hyperparameters to MLflow
        self._log_dataset_metadata(datasets=training_ds)
        
        # 6. Serialize and upload datasets to MLflow Artifacts
        self._log_dataset_artifacts(datasets={
            "training": training_ds,
            "validation": validation_ds,
        })

        self.logger.info("TFT datasets created successfully and artifacts logged.")
        return training_ds, validation_ds


    def _log_dataset_metadata(self, datasets: TimeSeriesDataSet):
        """Helper to log the internal structure of the TFT dataset."""
        mlflow.log_params({
            "max_encoder_length": datasets.max_encoder_length,
            "max_prediction_length": datasets.max_prediction_length,
            "num_symbols": len(datasets.get_parameters()["categorical_encoders"]["symbol"].classes_),
            "features_known": self.features_cfg.time_varying_known_categoricals + self.features_cfg.time_varying_known_reals,
            "features_unknown": self.features_cfg.time_varying_unknown_reals
        })
    
    
    def _log_dataset_artifacts(self, datasets: dict[str, TimeSeriesDataSet]):
        """Serializes and uploads dataset objects to the MLflow artifact store."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            for name, ds in datasets.items():
                path = Path(tmp_dir) / f"{name}.pt"
                # PyTorch Forecasting datasets are best saved via torch.save
                torch.save(ds, path)
                mlflow.log_artifact(local_path=str(path), artifact_path="datasets")
            
            self.logger.info(f"Uploaded {len(datasets)} datasets to MLflow artifacts.")
        
