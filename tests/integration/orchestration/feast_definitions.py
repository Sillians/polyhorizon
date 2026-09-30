"""Copied into a disposable Feast repository by the live rehearsal."""
from datetime import timedelta
from pathlib import Path

import yaml
from feast import Entity, FeatureView, Field, ValueType
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float32, Int64

from polyhorizon.core.dataset_release import publication_source_query

settings = yaml.safe_load((Path(__file__).parent / "feature_store.yaml").read_text())
symbol = Entity(name="symbol", join_keys=["symbol"], value_type=ValueType.STRING)
source = PostgreSQLSource(
    name="rehearsal_ohlcv",
    query=publication_source_query(settings["offline_store"]["db_schema"], "ohlcv"),
    timestamp_field="event_timestamp", created_timestamp_column="created_at",
)
stock_ohlcv_features = FeatureView(
    name="stock_ohlcv_features", entities=[symbol], ttl=timedelta(days=7),
    schema=[Field(name="close", dtype=Float32), Field(name="total_volume", dtype=Int64)],
    source=source, online=True,
)
