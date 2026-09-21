from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import DataFrame

from polyhorizon.streaming.utils.logger import get_logger

logger = get_logger("WriteFeatures")


def write_features(
    df: DataFrame,
    path: str,
    *,
    mode: str = "overwrite",
    single_file: bool = False,
    partition_by: Optional[Iterable[str]] = None,
) -> None:
    """Write computed features to Delta storage."""
    if not path:
        raise ValueError("path must not be empty")

    logger.info("Writing features to %s", path)

    writer_df = df.coalesce(1) if single_file else df
    writer = writer_df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    logger.info("Features successfully written to %s", path)
