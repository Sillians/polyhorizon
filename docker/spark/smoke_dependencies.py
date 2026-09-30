"""No Kafka/S3 services required: exercise JVM dependencies and a local Delta write."""
import tempfile
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
         .getOrCreate())
try:
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    for name in (
        'io.delta.storage.LogStore',
        'org.apache.kafka.common.serialization.ByteArraySerializer',
        'org.apache.spark.kafka010.KafkaConfigUpdater',
        'org.apache.commons.pool2.impl.GenericKeyedObjectPool',
        'org.apache.hadoop.fs.s3a.S3AFileSystem',
    ):
        loader.loadClass(name)
    with tempfile.TemporaryDirectory(prefix='delta-smoke-') as directory:
        spark.range(2).write.format('delta').save(directory + '/table')
        assert spark.read.format('delta').load(directory + '/table').count() == 2
    print('Spark JVM dependency smoke test passed')
finally:
    spark.stop()
