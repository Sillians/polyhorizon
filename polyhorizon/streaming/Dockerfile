# adjust as development progresses
FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv && uv sync --frozen

COPY polyhorizon ./polyhorizon

CMD ["uv", "run", "python", "-m", "polyhorizon.streaming.kafka_job"]
