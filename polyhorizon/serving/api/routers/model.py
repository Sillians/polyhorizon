from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from polyhorizon.serving.api.schemas import ModelMetadataResponse, ModelReloadResponse
from polyhorizon.serving.app.dependencies import get_container, ServingContainer
from polyhorizon.serving.utils.logger import get_logger


router = APIRouter()
logger = get_logger("ModelRouter")


@router.get("/model", response_model=ModelMetadataResponse)
def get_model_metadata(container: ServingContainer = Depends(get_container)):
    try:
        metadata = container.model_registry.get_champion_metadata()
        return ModelMetadataResponse(**metadata)
    except Exception as exc:
        logger.exception("Failed to fetch model metadata")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch model metadata",
        ) from exc


@router.post("/model/reload", response_model=ModelReloadResponse)
def reload_champion(container: ServingContainer = Depends(get_container)):
    """Reload the current champion alias without restarting the API process."""
    if not container.config.client_metadata.model_reload_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found")
    try:
        handle = container.reload_champion()
        return ModelReloadResponse(
            model_version=handle.model_version,
            model_uri=handle.model_uri,
            loaded_at=handle.loaded_at.isoformat(),
        )
    except Exception as exc:
        logger.exception("Failed to reload champion model")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Champion model reload failed",
        ) from exc
