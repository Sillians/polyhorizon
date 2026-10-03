FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_CACHE=1 \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md /app/
RUN uv sync --frozen --all-extras \
    && uv pip install --python /app/.venv/bin/python "prefect-docker>=0.6,<0.7"

COPY polyhorizon /app/polyhorizon
COPY prefect.yaml /app/prefect.yaml

RUN uv run --frozen python -c "from prefect_docker.worker import DockerWorker; assert DockerWorker.type == 'docker'" \
    && uv run --frozen prefect --help > /dev/null

# Deployment overrides this command with `uv run prefect deploy ...`.
CMD ["uv", "run", "--frozen", "prefect", "--help"]
