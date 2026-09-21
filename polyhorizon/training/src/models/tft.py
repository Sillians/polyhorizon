from pytorch_forecasting import TemporalFusionTransformer, QuantileLoss
from pytorch_forecasting import TimeSeriesDataSet

from polyhorizon.training.configs.settings import ModelConfig
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("CreateTFTModel")

def create_tft_model(
    training_dataset: TimeSeriesDataSet,
    config: ModelConfig,
) -> TemporalFusionTransformer:
    """Create a Temporal Fusion Transformer model from dataset and config.

    Args:
        training_dataset: PyTorch Forecasting TimeSeriesDataSet for training
        config: Model configuration with hyperparameters

    Returns:
        Configured TFT model ready for training
    """
    return TemporalFusionTransformer.from_dataset(
        training_dataset,
        learning_rate=config.learning_rate,
        hidden_size=config.hidden_size,
        attention_head_size=config.attention_head_size,
        dropout=config.dropout,
        hidden_continuous_size=config.hidden_continuous_size,
        output_size=config.output_size,
        loss=QuantileLoss(quantiles=config.quantiles),
        log_interval=config.log_interval,
        reduce_on_plateau_patience=config.reduce_on_plateau_patience,
    )


def create_tft_model_with_hparams(
    *
    training_dataset: TimeSeriesDataSet, 
    base_config: ModelConfig, 
    **hparams
) -> TemporalFusionTransformer:
    """Create TFT model with custom hyperparameters (for HPO).

    Args:
        training_dataset: PyTorch Forecasting TimeSeriesDataSet
        base_config: Base model configuration
        **hparams: Override hyperparameters (learning_rate, hidden_size, etc.)

    Returns:
        TFT model with custom hyperparameters
    """
    return TemporalFusionTransformer.from_dataset(
        training_dataset,
        learning_rate=hparams.get("learning_rate", base_config.learning_rate),
        hidden_size=hparams.get("hidden_size", base_config.hidden_size),
        attention_head_size=hparams.get(
            "attention_head_size", base_config.attention_head_size
        ),
        dropout=hparams.get("dropout", base_config.dropout),
        hidden_continuous_size=hparams.get(
            "hidden_continuous_size", base_config.hidden_continuous_size
        ),
        output_size=base_config.output_size,
        loss=QuantileLoss(quantiles=base_config.quantiles),
        log_interval=base_config.log_interval,
        reduce_on_plateau_patience=base_config.reduce_on_plateau_patience,
        
    )
