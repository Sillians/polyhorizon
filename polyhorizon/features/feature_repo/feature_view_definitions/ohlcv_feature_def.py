from __future__ import annotations

from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float32, Int64, String

from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.feature_repo.feature_view_definitions.base_feature_def import BaseFeatureDefinition
from polyhorizon.features.feature_repo.features.entities import Entities
from polyhorizon.features.feature_repo.features.sources import Sources
from polyhorizon.features.utils.logger import get_logger

logger = get_logger("FeatureDefinition")


class OHLCVFeatureDefinition(BaseFeatureDefinition):
    """Defines OHLCV rolling statistics and related features dynamically."""

    def build_feature_view(self) -> FeatureView:
        schema = [Field(name="symbol", dtype=String)]

        skip_fields = {"symbol", "event_timestamp", "window_start", "window_end"}
        for feature_name in self.config.features.stock_ohlcv_features:
            if feature_name.lower() in skip_fields:
                continue

            dtype = Int64 if feature_name == "total_volume" else Float32
            schema.append(Field(name=feature_name, dtype=dtype))

        return FeatureView(
            name=self.config.project.feature_view,
            entities=[Entities.symbol],
            ttl=timedelta(days=self.config.feast_parameters.ttl_days),
            source=Sources.STOCK_OHLCV_SOURCE,
            schema=schema,
            online=True,
            tags={"team": "rolling_OHLCV_indicators"},
        )


config = load_config()

ohlcv_feature_view = OHLCVFeatureDefinition(config=config).build_feature_view()
logger.info("Registered FeatureView: %s", ohlcv_feature_view.name)
