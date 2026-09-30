# Spark runtime dependencies

`resolve-jars.sh` uses Spark's bundled Ivy Maven resolver during image build.
Root coordinates come from `versions.env`; their transitive runtime dependencies
are copied into `/opt/spark/jars`. Artifacts already supplied by the Spark base
image retain the base image's version to avoid duplicate core/Scala libraries.
`dependency-checksums.txt` inventories the resulting JAR classpath.

The build exercises a local Delta write/read and loads Kafka client, Spark Kafka
token-provider, pool and S3 classes. A second smoke check runs as `spark` without
`--packages`, ensuring the installed classpath works without dependency downloads.
This checks JVM dependencies, not Kafka/S3 connectivity or live streaming.

Build: `docker compose build spark-master`. Recreate master, workers and submission
after rebuilding so drivers and executors use the same image. No checkpoint or
data-volume deletion is required.
