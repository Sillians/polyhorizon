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
RUN uv sync --frozen --extra streaming

ENV PATH="/opt/app/.venv/bin:${PATH}" \
    PYSPARK_PYTHON=/opt/app/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/app/.venv/bin/python

COPY docker/spark/versions.env /opt/app/docker/spark/versions.env
COPY polyhorizon /opt/app/polyhorizon

# --------------------
# Add required JARs (versions sourced from docker/spark/versions.env)
# --------------------
RUN set -a \
    && . /opt/app/docker/spark/versions.env \
    && set +a \
    && curl -fsSL "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_SPARK_VERSION}/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar" \
        -o /opt/spark/jars/delta-spark_2.12-${DELTA_SPARK_VERSION}.jar \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_SQL_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_SQL_KAFKA_VERSION}.jar" \
        -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_SQL_KAFKA_VERSION}.jar \
    && curl -fsSL "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
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
