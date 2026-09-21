from __future__ import annotations

from datetime import datetime
from datetime import UTC
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator
from polyhorizon.core.product_symbols import require_product_symbol

from polyhorizon.serving.services.forecast_service import ForecastResult


class ForecastRequest(BaseModel):
    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        return require_product_symbol(value)

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "symbol": "NVDA",
                "horizon": 3,
                "use_cache": True,
            }
        },
    )

    symbol: str = Field(..., min_length=1, description="Ticker symbol, e.g. NVDA")
    horizon: Optional[int] = Field(None, ge=1, description="Prediction horizon override")
    use_cache: bool = Field(True, description="Use cached forecast if available")


class PredictionStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    step: int = Field(..., ge=1)
    timestamp: str
    p10: float
    p50: float
    p90: float


class ForecastResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    horizon: int
    quantiles: List[float]
    predictions: List[PredictionStep]
    base_price: float
    absolute_change: float
    percent_change: float
    model_version: str
    model_uri: str
    features_timestamp: Optional[str]
    cached: bool
    generated_at: str

    @classmethod
    def from_result(cls, result: ForecastResult) -> "ForecastResponse":
        return cls(
            symbol=result.symbol,
            horizon=result.horizon,
            quantiles=result.quantiles,
            predictions=[PredictionStep(**item) for item in result.predictions],
            base_price=result.base_price,
            absolute_change=result.absolute_change,
            percent_change=result.percent_change,
            model_version=result.model_version,
            model_uri=result.model_uri,
            features_timestamp=result.features_timestamp,
            cached=result.cached,
            generated_at=datetime.now(UTC).isoformat(),
        )
