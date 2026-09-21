from fastapi import APIRouter

from polyhorizon.serving.api.routers import forecast, health, features, metadata, model

api_router = APIRouter()
api_router.include_router(health.router, tags=["health"])
api_router.include_router(forecast.router, tags=["forecast"])
api_router.include_router(features.router, tags=["features"])
api_router.include_router(model.router, tags=["model"])
api_router.include_router(metadata.router, tags=["metadata"])

__all__ = ["api_router"]
