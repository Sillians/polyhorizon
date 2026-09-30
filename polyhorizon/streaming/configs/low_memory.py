"""Opt-in local profile. Stateless Bronze checkpoints are intentionally unchanged."""
SUFFIX = "-p8-v1"


def apply_low_memory(config):
    config = config.model_copy(deep=True)
    storage = config.streaming_storage
    for name in ("gold", "cp_gold"):
        value = getattr(storage, name).rstrip("/")
        if not value.endswith(SUFFIX):
            setattr(storage, name, value + SUFFIX)
    storage.source_table = storage.gold
    config.spark_connection.conf.update({
        "spark.sql.shuffle.partitions": "8",
        "spark.default.parallelism": "2",
        "spark.cores.max": "1",
        "spark.executor.cores": "1",
        "spark.executor.memory": "768m",
        "spark.driver.maxResultSize": "128m",
        "spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage": "true",
        "spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB": "128",
        "spark.sql.streaming.stateStore.rocksdb.writeBufferCacheRatio": "0.5",
    })
    # Smaller batches bound pressure during initial Gold replay and live Kafka reads.
    config.kafka_connection.max_offsets_per_trigger = 10000
    return config
