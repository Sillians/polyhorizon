from __future__ import annotations

from polyhorizon.features.feature_repo.feature_view_definitions.ohlcv_feature_def import (
    ohlcv_feature_view,
)
from polyhorizon.features.feature_repo.features.entities import Entities
from polyhorizon.features.feature_repo.features.sources import Sources

entities = [Entities.symbol]
feature_views = [ohlcv_feature_view]
data_sources = [Sources.STOCK_OHLCV_SOURCE]
