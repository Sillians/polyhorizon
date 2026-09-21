from __future__ import annotations

from dataclasses import dataclass
from threading import Lock
from typing import Optional

from fastapi import Request

from polyhorizon.serving.configs.settings import Config, load_config
from polyhorizon.serving.services.cache import RedisCache
from polyhorizon.serving.services.feature_store import FeatureStoreClient
from polyhorizon.serving.services.forecast_service import ForecastService
from polyhorizon.serving.services.model_registry import ModelHandle, ModelRegistryClient
from polyhorizon.serving.utils.logger import get_logger


@dataclass
class ServingContainer:
    config: Config
    model_registry: ModelRegistryClient
    model_handle: ModelHandle
    feature_store: FeatureStoreClient
    cache: RedisCache
    forecast_service: ForecastService
    reload_lock: Lock

    def reload_champion(self) -> ModelHandle:
        """Atomically load the currently aliased champion into the service."""
        with self.reload_lock:
            model_handle = self.model_registry.load_champion()
            self.model_handle = model_handle
            self.forecast_service.model_handle = model_handle
            return model_handle


def build_container(config: Optional[Config] = None, config_path: Optional[str] = None) -> ServingContainer:
    if config is None:
        config = load_config(config_path)

    logger = get_logger("Container")
    registry = ModelRegistryClient(config)
    model_handle = registry.load_champion()
    feature_store = FeatureStoreClient(config)
    cache = RedisCache(config.redis)
    forecast_service = ForecastService(
        config=config,
        model_handle=model_handle,
        feature_store=feature_store,
        cache=cache,
    )
    logger.info("Serving container initialized with model version %s", model_handle.model_version)
    return ServingContainer(
        config=config,
        model_registry=registry,
        model_handle=model_handle,
        feature_store=feature_store,
        cache=cache,
        forecast_service=forecast_service,
        reload_lock=Lock(),
    )


def get_container(request: Request) -> ServingContainer:
    container = getattr(request.app.state, "container", None)
    if container is None:
        raise RuntimeError("Serving container not initialized")
    return container


def get_forecast_service(request: Request) -> ForecastService:
    return get_container(request).forecast_service
