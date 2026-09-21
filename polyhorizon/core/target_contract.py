"""Versioned forecast semantics serialized inside every trained model."""

from typing import Literal

from pydantic import BaseModel, ConfigDict


class TargetContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    target_column: Literal["target"] = "target"
    target_type: Literal["return"] = "return"
    target_definition: Literal["log(close_t / close_t_minus_1)"] = "log(close_t / close_t_minus_1)"
    step_semantics: Literal["one_bar_per_symbol"] = "one_bar_per_symbol"
    base_price_feature: Literal["close"] = "close"
    return_to_price_method: Literal["log"] = "log"


def attach_target_contract(model, dataset_parameters: dict) -> dict:
    """Only label datasets matching the implemented training target."""
    contract = TargetContract().model_dump()
    if dataset_parameters.get("target") != contract["target_column"]:
        raise ValueError("Training dataset target must be 'target' (one-step log returns)")
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
    }
    for field, value in expected.items():
        if getattr(contract, field) != value:
            raise ValueError(f"Model target contract mismatch for {field}: artifact={getattr(contract, field)!r}, serving={value!r}")
    parameters = getattr(model, "dataset_parameters", None)
    if not isinstance(parameters, dict) or parameters.get("target") != contract.target_column:
        raise ValueError("Model dataset_parameters target disagrees with target_contract")
    return contract
