from __future__ import annotations

from prometheus_client import Counter, Histogram


REQUEST_LATENCY = Histogram(
    "serving_request_latency_seconds",
    "Request latency in seconds",
    ["endpoint", "method"],
)
REQUEST_COUNT = Counter(
    "serving_requests_total",
    "Total number of requests",
    ["endpoint", "method", "status"],
)
CACHE_HIT = Counter(
    "serving_cache_hits_total",
    "Number of cache hits",
)
CACHE_MISS = Counter(
    "serving_cache_misses_total",
    "Number of cache misses",
)
MODEL_LOAD = Counter(
    "serving_model_loads_total",
    "Number of model loads",
)


def record_request(endpoint: str, method: str, status: int, duration: float) -> None:
    REQUEST_COUNT.labels(endpoint=endpoint, method=method, status=str(status)).inc()
    REQUEST_LATENCY.labels(endpoint=endpoint, method=method).observe(duration)


def record_cache(hit: bool) -> None:
    if hit:
        CACHE_HIT.inc()
    else:
        CACHE_MISS.inc()


def record_model_load() -> None:
    MODEL_LOAD.inc()
