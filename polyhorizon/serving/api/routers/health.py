from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from polyhorizon.serving.api.schemas import HealthResponse
from polyhorizon.serving.app.dependencies import get_container, ServingContainer


router = APIRouter()


@router.get("/health/live")
def liveness():
    return {"status": "ok"}


@router.get("/health/ready")
def readiness(request: Request):
    container = getattr(request.app.state, "container", None)
    if container is None or container.model_handle.model is None:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "model_not_loaded"},
        )

    loaded_version = str(container.model_handle.model_version)
    try:
        champion_version = container.model_registry.get_champion_version()
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "reason": "champion_unresolvable",
                "model_version": loaded_version,
            },
        )

    if loaded_version != champion_version:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "reason": "champion_not_active",
                "model_version": loaded_version,
                "champion_version": champion_version,
            },
        )
    return {
        "status": "ready",
        "model_version": loaded_version,
        "champion_version": champion_version,
    }


@router.get("/health", response_model=HealthResponse)
def health(container: ServingContainer = Depends(get_container)):
    return HealthResponse(
        status="ok",
        service=container.config.project.service_name,
        version=container.config.project.version,
        model_version=container.model_handle.model_version,
        model_loaded_at=container.model_handle.loaded_at.isoformat(),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
