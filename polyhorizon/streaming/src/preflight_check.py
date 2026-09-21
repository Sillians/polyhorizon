from __future__ import annotations

from typing import Iterable

import os
import boto3
from botocore.config import Config as BotoConfig
from kafka import KafkaConsumer

from polyhorizon.streaming.configs.settings import Config
from polyhorizon.streaming.utils.logger import get_logger


def _check_seaweedfs(config: Config, buckets: Iterable[str]) -> bool:
    logger = get_logger("Preflight.SeaweedFS")
    if not buckets:
        logger.warning("SeaweedFS bucket list is empty; skipping bucket validation")
        return True

    s3_custom_config = BotoConfig(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 3},
    )

    s3 = boto3.client(
        "s3",
        endpoint_url=str(config.bucket_details.seaweedfs_s3_endpoint),
        aws_access_key_id=config.bucket_details.seaweedfs_access_key.get_secret_value(),
        aws_secret_access_key=config.bucket_details.seaweedfs_secret_key.get_secret_value(),
        region_name="us-east-1",
        config=s3_custom_config,
    )

    all_ok = True
    try:
        s3.list_buckets()
        logger.info("SeaweedFS credentials validated via list_buckets")
    except Exception:
        logger.error("SeaweedFS credentials validation failed", exc_info=True)
        return False
    for bucket in buckets:
        try:
            s3.head_bucket(Bucket=bucket)
            logger.info("SeaweedFS bucket available: %s", bucket)
        except s3.exceptions.NoSuchBucket:
            logger.warning("SeaweedFS bucket does not exist: %s", bucket)
            all_ok = False
        except Exception:
            logger.error("SeaweedFS connectivity check failed for bucket '%s'", bucket, exc_info=True)
            all_ok = False
    return all_ok


def _check_kafka(config: Config) -> bool:
    logger = get_logger("Preflight.Kafka")
    kafka_topic = config.kafka_connection.kafka_topic
    consumer = None
    try:
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=config.kafka_connection.kafka_servers,
            request_timeout_ms=5000,
        )
        topics = consumer.topics()
        if kafka_topic in topics:
            logger.info("Kafka topic available: %s", kafka_topic)
            return True
        logger.warning("Kafka connected, but topic '%s' not found", kafka_topic)
        return False
    except Exception:
        logger.error("Kafka connection failed. Check bootstrap servers.", exc_info=True)
        return False
    finally:
        if consumer is not None:
            consumer.close()

def _check_spark_package_versions(config: Config) -> bool:
    logger = get_logger("Preflight.SparkVersions")
    packages = config.spark_connection.packages
    if not packages:
        logger.error("spark_connection.packages is empty; cannot validate Spark package versions")
        return False

    required = {
        "DELTA_SPARK_VERSION",
        "SPARK_SQL_KAFKA_VERSION",
        "HADOOP_AWS_VERSION",
        "AWS_SDK_BUNDLE_VERSION",
    }

    env_missing = [name for name in required if not _env_value_present(name)]
    if env_missing:
        logger.error("Missing Spark version env vars: %s", ", ".join(env_missing))
        return False

    unresolved = [pkg for pkg in packages if "${" in pkg and "}" in pkg]
    if unresolved:
        logger.error("Unresolved Spark package versions in config: %s", unresolved)
        return False

    logger.info("Spark package versions resolved: %s", ", ".join(packages))
    return True


def _env_value_present(key: str) -> bool:
    value = os.getenv(key)
    return value is not None and value.strip() != ""


def check_infrastructure(config: Config) -> bool:
    logger = get_logger("Preflight")
    logger.info("Starting pre-flight infrastructure check")

    seaweed_ok = _check_seaweedfs(config, config.bucket_details.seaweedfs_s3_buckets)
    kafka_ok = _check_kafka(config)
    spark_versions_ok = _check_spark_package_versions(config)

    if seaweed_ok and kafka_ok and spark_versions_ok:
        logger.info("Pre-flight checks passed")
    else:
        logger.warning("Pre-flight checks failed")

    return seaweed_ok and kafka_ok and spark_versions_ok



# test (remove later)
# from polyhorizon.streaming.configs.settings import load_config
# config = load_config()

# if __name__ == "__main__":
#     if check_infrastructure(config=config):
#         print("\nAll systems GO. You can safely start the Spark Job.")
#     else:
#         print("\nPre-flight checks FAILED. Fix the issues above before starting Spark.")
