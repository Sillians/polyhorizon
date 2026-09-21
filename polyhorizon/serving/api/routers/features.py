from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status

from polyhorizon.serving.api.schemas import DebugFeaturesResponse, FeaturesResponse
from polyhorizon.serving.app.dependencies import get_container, ServingContainer
from polyhorizon.serving.utils.logger import get_logger
from polyhorizon.core.product_symbols import require_product_symbol


router = APIRouter()
logger = get_logger("FeaturesRouter")


def product_symbol(symbol: str = Query(..., min_length=1)) -> str:
    try:
        return require_product_symbol(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/features", response_model=FeaturesResponse)
def get_features(
    symbol: str = Depends(product_symbol),
    limit: int = Query(50, ge=1, le=500, description="Max rows to return"),
    container: ServingContainer = Depends(get_container),
):
    symbol = symbol.upper()
    try:
        df = container.feature_store.get_feature_window(symbol, container.config.offline_store.history_rows)
        if df.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No features found for symbol {symbol}",
            )

        truncated = False
        if len(df) > limit:
            df = df.tail(limit)
            truncated = True

        time_field = container.config.inference.time_field
        features_timestamp = None
        if time_field in df.columns:
            features_timestamp = str(df[time_field].iloc[-1])

        data = df.to_dict(orient="records")
        return FeaturesResponse(
            symbol=symbol,
            rows=len(data),
            columns=list(df.columns),
            data=data,
            features_timestamp=features_timestamp,
            truncated=truncated,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch feature window")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch feature window",
        ) from exc


@router.get("/features/debug", response_model=DebugFeaturesResponse)
def get_features_debug(
    symbol: str = Depends(product_symbol),
    limit: int = Query(50, ge=1, le=500, description="Max rows to return"),
    container: ServingContainer = Depends(get_container),
):
    if not container.config.client_metadata.feature_debug_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    symbol = symbol.upper()
    try:
        raw_df = container.feature_store.get_feature_window(symbol, container.config.offline_store.history_rows)
        if raw_df.empty:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No features found for symbol {symbol}",
            )

        derived_df = container.forecast_service.preprocessor.enrich(raw_df)

        truncated = False
        if len(raw_df) > limit:
            raw_df = raw_df.tail(limit)
            derived_df = derived_df.tail(limit)
            truncated = True

        time_field = container.config.inference.time_field
        features_timestamp = None
        if time_field in raw_df.columns:
            features_timestamp = str(raw_df[time_field].iloc[-1])

        return DebugFeaturesResponse(
            symbol=symbol,
            raw_rows=len(raw_df),
            derived_rows=len(derived_df),
            raw_columns=list(raw_df.columns),
            derived_columns=list(derived_df.columns),
            raw=raw_df.to_dict(orient="records"),
            derived=derived_df.to_dict(orient="records"),
            features_timestamp=features_timestamp,
            truncated=truncated,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch debug feature window")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch debug feature window",
        ) from exc
