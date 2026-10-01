import copy
from types import SimpleNamespace

import pytest

from polyhorizon.streaming.configs.low_memory import apply_low_memory
from scripts.deploy.register_local import specifications
from scripts.ops.migrate_kafka_two_brokers import assignments, verified


def test_profile_preserves_source_checkpoints():
    original = SimpleNamespace(streaming_storage=SimpleNamespace(gold="s3a://b/gold", cp_gold="s3a://b/cp/gold",
        source_table="s3a://b/gold", bronze="s3a://b/bronze", cp_bronze="s3a://b/cp/bronze", cp_dead_letter="s3a://b/cp/dl"),
        spark_connection=SimpleNamespace(conf={}), kafka_connection=SimpleNamespace(max_offsets_per_trigger=50000))
    original.model_copy = lambda deep: copy.deepcopy(original)
    low = apply_low_memory(original)
    assert low.streaming_storage.cp_bronze == original.streaming_storage.cp_bronze
    assert low.streaming_storage.cp_dead_letter == original.streaming_storage.cp_dead_letter
    assert low.streaming_storage.bronze == original.streaming_storage.bronze
    assert low.streaming_storage.gold.endswith("-p8-v1")
    assert low.streaming_storage.cp_gold.endswith("-p8-v1")
    assert low.streaming_storage.source_table == low.streaming_storage.gold
    assert low.spark_connection.conf["spark.sql.shuffle.partitions"] == "8"
    assert original.streaming_storage.gold == "s3a://b/gold"


def test_bounded_prefect_jobs_and_two_brokers():
    specs = specifications("/repo/.env", "network", low_memory=True)
    for spec in specs:
        assert "kafka-broker-3" not in spec["job_variables"]["env"]["KAFKA_SERVERS"]
        assert "mem_limit" in spec["job_variables"]
    spark = next(s for s in specs if s["name"] == "streaming-market-hours")
    assert spark["job_variables"]["image"] == "spark-custom:low-memory-local"
    assert spark["parameters"]["qualification_only"]


def test_replica_migration_preserves_topics_and_partition_numbers():
    metadata = [{"topic": "__consumer_offsets", "partitions": [{"partition": 0, "leader": 3,
        "replicas": [3, 2, 1], "isr": [1, 2, 3]}]}]
    before, after = assignments(metadata)
    assert before["partitions"][0]["replicas"] == [3, 2, 1]
    assert after["partitions"] == [{"topic": "__consumer_offsets", "partition": 0, "replicas": [1, 2]}]
    assert not verified(metadata)
    metadata[0]["partitions"][0].update(leader=1, replicas=[1, 2], isr=[1, 2])
    assert verified(metadata)


@pytest.mark.parametrize("low_memory", [False, True])
def test_ingestion_deployment_requires_manual_commits(low_memory):
    specs = specifications("/repo/.env", "network", low_memory=low_memory)
    ingestion = next(s for s in specs if s["name"] == "ingestion-market-hours")
    assert ingestion["job_variables"]["env"]["KAFKA_ENABLE_AUTO_COMMIT"] == "false"


def test_unsynchronized_migration_is_refused():
    with pytest.raises(RuntimeError, match="in sync"):
        assignments([{"topic": "t", "partitions": [{"partition": 0, "leader": 1, "replicas": [1, 2, 3], "isr": [1, 2]}]}])
