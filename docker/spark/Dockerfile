FROM apache/spark:3.5.7-python3

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_CACHE=1 \
    PYTHONPATH=/opt/app

# Create JMX directory
RUN mkdir -p /opt/spark/jmx

# Install curl for fetching jars
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# --------------------
# Install PolyHorizon streaming dependencies (uv)
# --------------------
WORKDIR /opt/app
RUN python3 -m pip install --upgrade pip \
    && pip install uv

COPY pyproject.toml uv.lock README.md /opt/app/
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install 3.11 \
    && uv sync --frozen --extra streaming --python 3.11 \
    && chmod -R a+rX /opt/python

ENV PATH="/opt/app/.venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/app/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/app/.venv/bin/python

COPY docker/spark/versions.env /opt/app/docker/spark/versions.env
COPY docker/spark/resolve-jars.sh docker/spark/smoke_dependencies.py /opt/app/docker/spark/
COPY polyhorizon /opt/app/polyhorizon

# --------------------
# Add required JARs (versions sourced from docker/spark/versions.env)
# --------------------
RUN bash /opt/app/docker/spark/resolve-jars.sh

RUN set -a \
    && . /opt/app/docker/spark/versions.env \
    && set +a \
    && curl -fsSL "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROMETHEUS_VERSION}/jmx_prometheus_javaagent-${JMX_PROMETHEUS_VERSION}.jar" \
    -o /opt/spark/jmx/jmx_prometheus_javaagent.jar

# --------------------
# Spark configuration
# --------------------
COPY docker/spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY docker/spark/conf/jmx-spark.yaml /opt/spark/conf/jmx-spark.yaml
COPY docker/spark/jobs /opt/spark/jobs

RUN chown -R spark:spark /opt/spark /opt/app

USER spark
RUN /opt/app/.venv/bin/python -c \
    "import sys, pyspark, boto3; assert sys.version_info[:2] == (3, 11)"
RUN SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit --master 'local[1]' \
    --conf spark.jars.packages= --conf spark.ui.enabled=false \
    /opt/app/docker/spark/smoke_dependencies.py
