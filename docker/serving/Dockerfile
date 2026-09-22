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
RUN uv sync --frozen --extra serving

COPY . /app

ENV PYTHONPATH=/app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://localhost:8000/v1/health/ready || exit 1

CMD ["uv", "run", "uvicorn", "polyhorizon.serving.app.main:app_factory", "--factory", "--host", "0.0.0.0", "--port", "8000"]
