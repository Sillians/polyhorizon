from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict


class ClientMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: str
    forecast_mode: str
    publication_frequency: str
    publication_delay_minutes: int
    freshness_policy: str
    api_version: str
    supported_symbols: List[str]
    default_symbol: str
    bars_per_day: int
    horizon_days: List[int]
    max_prediction_length: int
    quantiles: List[float]
    target_type: str
    return_to_price_method: str
    currency: str
    market_timezone: str
    frequency: str
    api_key_required: bool
    api_key_header: str
    request_timeout_seconds: int
    feature_debug_enabled: bool
    model_reload_enabled: bool
