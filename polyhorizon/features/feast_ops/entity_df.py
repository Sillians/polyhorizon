from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional
import os

import boto3
import pandas as pd
from botocore.client import Config as BotoConfig

from polyhorizon.features.configs.settings import Config
from polyhorizon.features.utils.logger import get_logger
from polyhorizon.sp500_data.reader import load_governed_universe

logger = get_logger("EntityDF")


def build_entity_df(
    symbols: Iterable[str],
    timestamps: Optional[Iterable[datetime]] = None,
) -> pd.DataFrame:
    symbols = list(symbols)

    if timestamps is None:
        timestamps = [datetime.now()] * len(symbols)
    else:
        timestamps = list(timestamps)
        if len(timestamps) != len(symbols):
            raise ValueError("Length of timestamps must match length of symbols.")

    return pd.DataFrame({"symbol": symbols, "event_timestamp": timestamps})


def build_entity_freq_df(
    symbols: Iterable[str],
    start_date: str,
    end_date: str,
    freq: str = "30min",
) -> pd.DataFrame:
    timestamps = pd.date_range(start=start_date, end=end_date, freq=freq)
    records = [{"symbol": symbol, "event_timestamp": ts} for symbol in symbols for ts in timestamps]
    return pd.DataFrame(records)


def build_entity_training_data(
    symbols: Iterable[str],
    start_date: str,
    end_date: str,
    freq: str = "30min",
) -> pd.DataFrame:
    timestamps = pd.date_range(start=start_date, end=end_date, freq=freq)
    entity_records = [{"symbol": s, "event_timestamp": ts} for s in symbols for ts in timestamps]
    return pd.DataFrame(entity_records)


def load_sp500_symbols(config: Config, bucket: str, key: str) -> list[str]:
    """Fetch symbols list from SeaweedFS."""
    try:
        clean_bucket = bucket.replace("s3://", "").strip("/")
        s3_resource = boto3.resource(
            "s3",
            endpoint_url=str(config.bucket_details.seaweedfs_s3_endpoint),
            aws_access_key_id=config.bucket_details.seaweedfs_access_key,
            aws_secret_access_key=config.bucket_details.seaweedfs_secret_key,
            config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1",
        )

        snapshot = load_governed_universe(
            s3_resource.meta.client,
            clean_bucket,
            current_key=os.getenv("UNIVERSE_CURRENT_KEY", "universe/current.json"),
            maximum_age_hours=int(os.getenv("UNIVERSE_MAX_AGE_HOURS", "30")),
        )
        logger.info(
            "Loaded %s symbols from governed universe %s",
            len(snapshot.symbols),
            snapshot.run_id,
        )
        return snapshot.symbols
    except Exception:
        logger.error("Failed to load S&P500 symbols from SeaweedFS", exc_info=True)
        raise
