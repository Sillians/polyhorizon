import optuna
from polyhorizon.training.configs.settings import Config
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("TFTParameterSampler")


class TFTParameterSampler:
    """Decouples configuration from Optuna suggestion logic."""

    def __init__(self, config: Config):
        self.config = config

    def sample(self, trial: optuna.Trial) -> dict:
        hp = self.config.hyperparameters
        
        # validation safety
        assert hp.learning_rate.low < hp.learning_rate.high
        assert hp.hidden_size.step > 0
        return {
            "learning_rate": trial.suggest_float(
                "learning_rate",
                hp.learning_rate.low,
                hp.learning_rate.high,
                log=hp.learning_rate.log,
            ),
            "hidden_size": trial.suggest_int(
                "hidden_size",
                hp.hidden_size.min,
                hp.hidden_size.max,
                step=hp.hidden_size.step,
            ),
            "dropout": trial.suggest_float(
                "dropout",
                hp.dropout.low,
                hp.dropout.high,
            ),
            "attention_head_size": trial.suggest_int(
                "attention_head_size",
                hp.attention_head_size.min,
                hp.attention_head_size.max,
                step=hp.attention_head_size.step,
            ),
            "hidden_continuous_size": trial.suggest_int(
                "hidden_continuous_size",
                hp.hidden_continuous_size.min,
                hp.hidden_continuous_size.max,
                step=hp.hidden_continuous_size.step,
            ),
        }

    def search_space(self) -> dict:
        hp = self.config.hyperparameters
        return {
            "learning_rate": {
                "low": hp.learning_rate.low,
                "high": hp.learning_rate.high,
                "log": hp.learning_rate.log,
            },
            "hidden_size": {
                "min": hp.hidden_size.min,
                "max": hp.hidden_size.max,
                "step": hp.hidden_size.step,
            },
            "dropout": {
                "low": hp.dropout.low,
                "high": hp.dropout.high,
            },
            "attention_head_size": {
                "min": hp.attention_head_size.min,
                "max": hp.attention_head_size.max,
                "step": hp.attention_head_size.step,
            },
            "hidden_continuous_size": {
                "min": hp.hidden_continuous_size.min,
                "max": hp.hidden_continuous_size.max,
                "step": hp.hidden_continuous_size.step,
            },
        }


# test (remove)
# config = load_config()
# hp = config.hyperparameters
# assert hp.learning_rate.low < hp.learning_rate.high
# assert hp.hidden_size.step > 0
