#!/usr/bin/env bash
set -euo pipefail
source /opt/app/docker/spark/versions.env
packages="io.delta:delta-spark_2.12:${DELTA_SPARK_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_SQL_KAFKA_VERSION},org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_BUNDLE_VERSION}"
# Spark's bundled Ivy resolves Maven POMs, including runtime transitive dependencies.
SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit --master 'local[1]' \
    --conf spark.jars.ivy=/tmp/polyhorizon-ivy --conf spark.ui.enabled=false \
    --packages "$packages" /opt/app/docker/spark/smoke_dependencies.py
python - <<'PY'
from pathlib import Path
import hashlib
import re
import shutil

destination = Path('/opt/spark/jars')
def artifact(name):
    return re.split(r'-(?=\d)', name, maxsplit=1)[0]
provided = {artifact(p.name) for p in destination.glob('*.jar')}
for jar in sorted(Path('/tmp/polyhorizon-ivy/jars').glob('*.jar')):
    # Ivy retrieval prefixes the filename with the Maven group ID.
    name = jar.name.split('_', 1)[1]
    if artifact(name) not in provided:
        shutil.copy2(jar, destination / name)
manifest = []
for jar in sorted(destination.glob('*.jar')):
    manifest.append(f'{hashlib.sha256(jar.read_bytes()).hexdigest()}  {jar.name}')
(destination / 'dependency-checksums.txt').write_text('\n'.join(manifest) + '\n')
PY
