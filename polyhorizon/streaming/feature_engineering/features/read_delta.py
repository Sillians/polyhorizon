from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession


def read_from_delta(
    spark: SparkSession,
    delta_path: str,
    *,
    columns: Optional[Iterable[str]] = None,
) -> DataFrame:
    if not delta_path:
        raise ValueError("delta_path must not be empty")

    df = spark.read.format("delta").load(delta_path)
    if columns:
        df = df.select(*columns)
    return df
