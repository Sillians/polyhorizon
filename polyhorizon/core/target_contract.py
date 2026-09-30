"""Versioned forecast semantics serialized inside every trained model."""

from typing import Literal
from hashlib import sha256
import json

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS

from pydantic import BaseModel, ConfigDict, Field


class TargetContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[2] = 2
    target_column: Literal["target"] = "target"
    target_type: Literal["return"] = "return"
    target_definition: Literal["log(close_t / close_t_minus_1)"] = "log(close_t / close_t_minus_1)"
    step_semantics: Literal["one_bar_per_symbol"] = "one_bar_per_symbol"
    base_price_feature: Literal["close"] = "close"
    return_to_price_method: Literal["log"] = "log"
    symbols: tuple[str, ...] = PRODUCT_SYMBOLS
    calendar: Literal["NYSE"] = "NYSE"
    frequency: Literal["30min"] = "30min"
    max_encoder_length: int = Field(gt=0)
    max_prediction_length: int = Field(gt=0)
    quantiles: tuple[float, ...]
    static_categoricals: tuple[str, ...]
    time_varying_known_categoricals: tuple[str, ...]
    time_varying_known_reals: tuple[str, ...]
    time_varying_unknown_reals: tuple[str, ...]
    dataset_fingerprint: str = Field(min_length=64, max_length=64)
    calibration_method: Literal["empirical_cumulative_residuals_v1"] = "empirical_cumulative_residuals_v1"


def _dataset_fingerprint(parameters: dict) -> str:
    """Hash the portable TFT schema; fitted encoders remain in the model artifact."""
    fields = ("target", "group_ids", "max_encoder_length", "max_prediction_length",
              "static_categoricals", "time_varying_known_categoricals",
              "time_varying_known_reals", "time_varying_unknown_reals")
    portable = {key: parameters.get(key) for key in fields}
    return sha256(json.dumps(portable, sort_keys=True, default=str).encode()).hexdigest()


def attach_target_contract(model, dataset_parameters: dict, config) -> dict:
    """Only label datasets matching the implemented training target."""
    if dataset_parameters.get("target") != "target":
        raise ValueError("Training dataset target must be 'target' (one-step log returns)")
    features = config.features
    training = config.training
    contract = TargetContract(
        max_encoder_length=training.max_encoder_length,
        max_prediction_length=training.max_prediction_length,
        quantiles=tuple(training.quantiles),
        static_categoricals=tuple(features.static_categoricals),
        time_varying_known_categoricals=tuple(features.time_varying_known_categoricals),
        time_varying_known_reals=tuple(features.time_varying_known_reals),
        time_varying_unknown_reals=tuple(features.time_varying_unknown_reals),
        dataset_fingerprint=_dataset_fingerprint(dataset_parameters),
    ).model_dump()
    for key in ("max_encoder_length", "max_prediction_length", "static_categoricals",
                "time_varying_known_categoricals", "time_varying_known_reals",
                "time_varying_unknown_reals"):
        if key not in dataset_parameters:
            raise ValueError(f"Training dataset is missing {key}")
        actual = dataset_parameters[key]
        expected = contract[key]
        if isinstance(expected, (list, tuple)):
            actual, expected = list(actual or []), list(expected)
        if actual != expected:
            raise ValueError(f"Training dataset disagrees with target contract for {key}")
    model.target_contract = contract
    return contract


def validate_target_contract(model, config) -> TargetContract:
    raw = getattr(model, "target_contract", None)
    if not isinstance(raw, dict) or set(raw) != set(TargetContract.model_fields):
        raise ValueError("Model artifact is missing a complete target_contract; retrain/re-export the model")
    contract = TargetContract.model_validate(raw)
    expected = {
        "target_column": config.inference.target,
        "target_type": config.forecast.target_type,
        "base_price_feature": config.forecast.base_price_feature,
        "return_to_price_method": config.forecast.return_to_price_method,
        "symbols": tuple(config.client_metadata.supported_symbols),
        "calendar": "NYSE",
        "frequency": config.inference.freq,
        "max_encoder_length": config.inference.max_encoder_length,
        "max_prediction_length": config.inference.max_prediction_length,
        "quantiles": tuple(config.forecast.quantiles),
        "static_categoricals": tuple(config.inference.static_categoricals),
        "time_varying_known_categoricals": tuple(config.inference.time_varying_known_categoricals),
        "time_varying_known_reals": tuple(config.inference.time_varying_known_reals),
        "time_varying_unknown_reals": tuple(config.inference.time_varying_unknown_reals),
    }
    for field, value in expected.items():
        if getattr(contract, field) != value:
            raise ValueError(f"Model target contract mismatch for {field}: artifact={getattr(contract, field)!r}, serving={value!r}")
    parameters = getattr(model, "dataset_parameters", None)
    if not isinstance(parameters, dict) or parameters.get("target") != contract.target_column:
        raise ValueError("Model dataset_parameters target disagrees with target_contract")
    if _dataset_fingerprint(parameters) != contract.dataset_fingerprint:
        raise ValueError("Model dataset_parameters fingerprint disagrees with target_contract")
    calibration = getattr(model, "cumulative_calibration", None)
    if not isinstance(calibration, dict) or calibration.get("method") != contract.calibration_method:
        raise ValueError("Model is missing cumulative forecast calibration")
    if calibration.get("quantiles") != list(contract.quantiles):
        raise ValueError("Model calibration quantiles disagree with target_contract")
    offsets = calibration.get("offsets")
    if (not isinstance(offsets, list) or len(offsets) != contract.max_prediction_length or
            any(not isinstance(row, list) or len(row) != len(contract.quantiles)
                for row in offsets)):
        raise ValueError("Model calibration horizon disagrees with target_contract")
    return contract
