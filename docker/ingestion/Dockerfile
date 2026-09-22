FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_CACHE=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    ca-certificates \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install uv

COPY pyproject.toml uv.lock /app/
RUN uv sync --frozen --extra ingestion --extra sp500_data

COPY polyhorizon /app/polyhorizon
COPY prefect.yaml /app/prefect.yaml
COPY scripts/ci/smoke_universe_entrypoint.py /app/scripts/ci/smoke_universe_entrypoint.py

ENV PYTHONPATH=/app PATH="/app/.venv/bin:${PATH}" \
    LOG_LEVEL=INFO LOG_TO_FILE=false

# Fail the build if the deployed interpreter cannot load the universe job.
RUN python scripts/ci/smoke_universe_entrypoint.py
