from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from polyhorizon.serving.api.schemas import ForecastRequest, ForecastResponse
from polyhorizon.serving.app.dependencies import get_forecast_service
from polyhorizon.serving.services.forecast_service import ForecastService
from polyhorizon.serving.services.freshness import StaleFeaturesError
from polyhorizon.serving.utils.logger import get_logger


router = APIRouter()
logger = get_logger("ForecastRouter")


@router.post("/forecast", response_model=ForecastResponse)
def forecast(request: ForecastRequest, service: ForecastService = Depends(get_forecast_service)):
    try:
        result = service.predict(
            symbol=request.symbol,
            horizon=request.horizon,
            use_cache=request.use_cache,
        )
        return ForecastResponse.from_result(result)
    except StaleFeaturesError as exc:
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc), "code": "stale_features"},
            headers={"Retry-After": "300"},
        )
    except ValueError as exc:
        logger.warning("Forecast request failed: %s", exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected error during forecast")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Forecast failed",
        ) from exc
