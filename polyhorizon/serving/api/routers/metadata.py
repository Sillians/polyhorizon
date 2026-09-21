from __future__ import annotations

from fastapi import APIRouter, Request

from polyhorizon.serving.api.schemas import ClientMetadataResponse


router = APIRouter()


@router.get("/ops/session")
def operator_session(request: Request):
    """Authorization is enforced by SecurityManager before this handler."""
    config = request.app.state.config
    return {
        "role": "operator",
        "feature_debug_enabled": config.client_metadata.feature_debug_enabled,
        "model_reload_enabled": config.client_metadata.model_reload_enabled,
    }


@router.get("/metadata", response_model=ClientMetadataResponse)
def client_metadata(request: Request):
    config = request.app.state.config
    metadata = config.client_metadata
    return ClientMetadataResponse(
        service=config.project.service_name,
        forecast_mode=config.publication.forecast_mode,
        publication_frequency="once_per_completed_NYSE_session",
        publication_delay_minutes=config.publication.publication_delay_minutes,
        freshness_policy="NYSE closing bar required after close plus publication allowance; prior session allowed before deadline",
        api_version=config.api.version_prefix,
        supported_symbols=metadata.supported_symbols,
        default_symbol=metadata.default_symbol,
        bars_per_day=metadata.bars_per_day,
        horizon_days=metadata.horizon_days,
        max_prediction_length=config.inference.max_prediction_length,
        quantiles=config.forecast.quantiles,
        target_type=config.forecast.target_type,
        return_to_price_method=config.forecast.return_to_price_method,
        currency=metadata.currency,
        market_timezone=metadata.market_timezone,
        frequency=config.inference.freq,
        api_key_required=config.security.require_api_key,
        api_key_header=config.security.api_key_header,
        request_timeout_seconds=config.api.request_timeout_seconds,
        feature_debug_enabled=metadata.feature_debug_enabled,
        model_reload_enabled=metadata.model_reload_enabled,
    )
