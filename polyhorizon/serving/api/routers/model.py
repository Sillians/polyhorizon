from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from polyhorizon.serving.api.schemas import ModelMetadataResponse, ModelReloadResponse
from polyhorizon.serving.app.dependencies import get_container, ServingContainer
from polyhorizon.serving.utils.logger import get_logger


router = APIRouter()
logger = get_logger("ModelRouter")


class VersionRequest(BaseModel):
    model_version: str


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


@router.post("/model/prepare", response_model=ModelReloadResponse)
def prepare_candidate(body: VersionRequest, container: ServingContainer = Depends(get_container)):
    """Preload an approved candidate without changing live predictions."""
    if not container.config.client_metadata.model_reload_enabled:
        raise HTTPException(status_code=404, detail="Not found")
    try:
        version = body.model_version
        registered = container.model_registry.client.get_model_version(
            container.config.model_registry.name, version
        )
        if ((registered.tags or {}).get("governance_qualification") != "passed-v1" or
                (registered.tags or {}).get("session_qualification") != "nyse-full-session-v1"):
            raise ValueError("Candidate has not passed governance qualification")
        handle = container.prepare_version(version)
        return ModelReloadResponse(model_version=handle.model_version,
                                   model_uri=handle.model_uri,
                                   loaded_at=handle.loaded_at.isoformat())
    except Exception as exc:
        logger.exception("Failed to prepare candidate")
        raise HTTPException(status_code=503, detail="Candidate preparation failed") from exc


@router.post("/model/activate", response_model=ModelReloadResponse)
def activate_candidate(body: VersionRequest, container: ServingContainer = Depends(get_container)):
    if not container.config.client_metadata.model_reload_enabled:
        raise HTTPException(status_code=404, detail="Not found")
    try:
        handle = container.activate_prepared(body.model_version)
        return ModelReloadResponse(model_version=handle.model_version,
                                   model_uri=handle.model_uri,
                                   loaded_at=handle.loaded_at.isoformat())
    except Exception as exc:
        logger.exception("Failed to activate candidate")
        raise HTTPException(status_code=503, detail="Candidate activation failed") from exc
