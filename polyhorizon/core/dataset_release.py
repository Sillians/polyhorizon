"""Explicit immutable handoff from the Spark driver to feature publication."""

from datetime import datetime
from typing import Literal
from uuid import UUID
import re

from pydantic import BaseModel, ConfigDict, Field, model_validator


def canonical_path(path: str) -> str:
    return path.replace("s3a://", "s3://", 1).rstrip("/")


def publication_source_query(schema: str, table: str) -> str:
    for name in (schema, table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("Unsafe publication source identifier")
    return (
        f"SELECT * FROM {schema}.{table} WHERE dataset_version = "
        f"(SELECT dataset_id FROM {schema}.dataset_publications "
        "ORDER BY max_event_time DESC, updated_at DESC LIMIT 1)"
    )


class DatasetRelease(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1] = 1
    dataset_id: UUID
    status: Literal["features_complete"] = "features_complete"
    bronze_path: str
    bronze_version: int = Field(ge=0)
    gold_path: str
    gold_version: int = Field(ge=0)
    feature_path: str
    feature_version: int = Field(ge=0)
    feature_definition: Literal["ohlcv-rolling-v1"] = "ohlcv-rolling-v1"
    frequency: str = Field(min_length=1)
    lookback_bars: int = Field(gt=0)
    image_tag: str
    market_close: datetime
    completed_at: datetime
    min_event_time: datetime
    max_event_time: datetime
    row_count: int = Field(gt=0)

    @model_validator(mode="after")
    def validate_times(self):
        times = (self.market_close, self.completed_at, self.min_event_time, self.max_event_time)
        if any(value.tzinfo is None for value in times):
            raise ValueError("Dataset timestamps must have timezones")
        if not self.min_event_time <= self.max_event_time <= self.market_close <= self.completed_at:
            raise ValueError("Invalid dataset completion/time bounds")
        return self

    def validate_root(self, feature_root: str) -> None:
        expected = f"{canonical_path(feature_root)}/datasets/{self.dataset_id}"
        if canonical_path(self.feature_path) != expected:
            raise ValueError("Feature release path does not match configured FEATURE_OUTPUT_PATH")


def validate_feature_frame(frame, release: DatasetRelease | None = None) -> None:
    import numpy as np
    import pandas as pd
    from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS

    numeric = ["open", "high", "low", "close", "total_volume", "rolling_avg_close", "rolling_volatility_close"]
    required = ["symbol", "window_start", "window_end", "event_timestamp", *numeric]
    if set(required) - set(frame.columns):
        raise ValueError("Computed features are missing required columns")
    if frame.empty or frame[required].isna().any().any():
        raise ValueError("Computed features are empty or contain nulls")
    if set(frame.symbol) != set(PRODUCT_SYMBOLS):
        raise ValueError("Computed features must cover exactly the product allowlist")
    if frame.duplicated(["symbol", "window_start"]).any():
        raise ValueError("Duplicate feature keys")
    if not np.isfinite(frame[numeric].to_numpy(dtype=float)).all():
        raise ValueError("Non-finite feature values")
    if (frame[["open", "high", "low", "close", "rolling_avg_close"]] <= 0).any().any():
        raise ValueError("Feature prices must be positive")
    if (frame[["total_volume", "rolling_volatility_close"]] < 0).any().any():
        raise ValueError("Volume and volatility must be nonnegative")
    if ((frame.total_volume % 1 != 0) | (frame.total_volume >= 2**63)).any():
        raise ValueError("Volume must be an integer representable as PostgreSQL BIGINT")
    if ((frame.high < frame[["open", "close", "low"]].max(axis=1)) |
            (frame.low > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("Invalid OHLC bounds")
    start = pd.to_datetime(frame.window_start, utc=True)
    end = pd.to_datetime(frame.window_end, utc=True)
    event = pd.to_datetime(frame.event_timestamp, utc=True)
    if not ((start < end) & (end == event)).all():
        raise ValueError("Feature event_timestamp must equal window_end")
    if release and (len(frame) != release.row_count or event.min() != release.min_event_time
                    or event.max() != release.max_event_time):
        raise ValueError("Feature rows do not match the completion manifest")
    if release and not (event.groupby(frame.symbol).max() == release.market_close).all():
        raise ValueError("Every product symbol must have a closing bar")
