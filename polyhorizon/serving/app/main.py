from __future__ import annotations

import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from polyhorizon.serving.api.routers import api_router
from polyhorizon.serving.app.dependencies import build_container
from polyhorizon.serving.configs.settings import load_config
from polyhorizon.serving.services import metrics
from polyhorizon.serving.services.security import SecurityManager
from polyhorizon.serving.utils.logger import get_logger


def create_app(config_path: Optional[str] = None, init_container: bool = True) -> FastAPI:
    config = load_config(config_path)
    logger = get_logger("App")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if init_container:
            container = build_container(config=config)
            app.state.container = container
            app.state.security = SecurityManager(config=config, redis_client=container.cache.client)
            if config.warmup_enabled:
                app.state.warmup_symbol = None
            logger.info("Model serving startup complete")
            yield
            container.cache.close()
            logger.info("Model serving shutdown complete")
        else:
            logger.info("Model serving startup skipped container initialization")
            yield

    app = FastAPI(
        title=config.project.name,
        description=config.project.description,
        version=config.project.version,
        docs_url="/docs" if config.api.docs_enabled else None,
        redoc_url="/redoc" if config.api.docs_enabled else None,
        openapi_url="/openapi.json" if config.api.docs_enabled else None,
        root_path=config.api.root_path,
        lifespan=lifespan,
    )
    app.state.config = config
    app.state.security = SecurityManager(config=config)

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next):
        start = time.perf_counter()
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        request.state.request_id = request_id
        security = getattr(request.app.state, "security", None)
        if security:
            decision = security.authorize(request)
            if not decision.allowed:
                response = JSONResponse(
                    status_code=decision.status_code,
                    content={"detail": decision.detail or "Unauthorized"},
                )
                duration = time.perf_counter() - start
                if config.monitoring.enable_metrics:
                    metrics.record_request("rejected", request.method, response.status_code, duration)
                response.headers["X-Request-ID"] = request_id
                return response
        try:
            response = await call_next(request)
        except Exception:
            duration = time.perf_counter() - start
            if config.monitoring.enable_metrics:
                metrics.record_request(_route_template(request), request.method, 500, duration)
            logger.exception(
                "Request failed",
                extra={"extra_fields": _request_log_context(request, request_id, 500, duration)},
            )
            raise
        duration = time.perf_counter() - start
        if config.monitoring.enable_metrics:
            metrics.record_request(
                _route_template(request), request.method, response.status_code, duration
            )
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "Request completed",
            extra={
                "extra_fields": _request_log_context(
                    request, request_id, response.status_code, duration
                )
            },
        )
        return response

    if config.monitoring.enable_metrics:
        @app.get(config.monitoring.metrics_path, include_in_schema=False)
        async def metrics_endpoint():
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/v1/warmup", include_in_schema=False)
    async def warmup(request: Request):
        container = getattr(request.app.state, "container", None)
        if not container:
            return JSONResponse(status_code=503, content={"detail": "Container not ready"})
        if not container.config.warmup_enabled:
            return JSONResponse(status_code=403, content={"detail": "Warmup disabled"})
        symbol = os.getenv("SERVING_WARMUP_SYMBOL", "NVDA")
        try:
            container.forecast_service.predict(symbol=symbol, use_cache=False)
        except Exception as exc:
            logger.warning("Warmup failed for symbol %s: %s", symbol, exc)
            return JSONResponse(status_code=500, content={"detail": "Warmup failed"})
        return JSONResponse(status_code=200, content={"status": "ok", "symbol": symbol})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception while processing request")
        return JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request_id": getattr(request.state, "request_id", None),
            },
        )

    app.include_router(api_router, prefix=config.api.version_prefix)

    # CORS must wrap auth so preflights and denied responses remain readable
    # to an explicitly allowed browser origin. Actual requests still require auth.
    if config.api.cors_allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.api.cors_allow_origins,
            allow_methods=config.api.cors_allow_methods,
            allow_headers=config.api.cors_allow_headers,
            allow_credentials=True,
        )

    return app


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", "unmatched")


def _request_log_context(
    request: Request, request_id: str, status_code: int, duration: float
) -> dict[str, object]:
    return {
        "request_id": request_id,
        "method": request.method,
        "route": _route_template(request),
        "status": status_code,
        "duration_seconds": round(duration, 6),
    }


def app_factory() -> FastAPI:
    """Build the ASGI app after the runtime environment has been loaded."""
    return create_app(config_path=os.getenv("SERVING_CONFIG_PATH"))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "polyhorizon.serving.app.main:app_factory",
        host=os.getenv("SERVING_HOST", "0.0.0.0"),
        port=int(os.getenv("SERVING_PORT", "8000")),
        reload=False,
        factory=True,
    )
