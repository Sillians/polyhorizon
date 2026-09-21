from __future__ import annotations

from feast import PushSource
from polyhorizon.core.dataset_release import publication_source_query
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.utils.logger import get_logger

logger = get_logger("FeatureSource")
config = load_config()


class Sources:
    """Defines and manages data sources used in Feast FeatureViews."""

    STOCK_OHLCV_SOURCE = PostgreSQLSource(
        name="stock_ohlcv_source",
        # Publication commits rows and the ledger entry together. Both historical
        # reads and materialization select one release, including a pending
        # release being retried after an online-store failure.
        query=publication_source_query(config.data.db_schema, config.data.offline_table_name),
        timestamp_field="event_timestamp",
        created_timestamp_column="created_at",
    )

    stock_ohlcv_push_source = PushSource(
        name="stock_ohlcv_features_push_source",
        batch_source=STOCK_OHLCV_SOURCE,
    )
