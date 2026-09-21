from __future__ import annotations

import argparse
from typing import List, Optional

from polyhorizon.streaming.configs.settings import load_config
from polyhorizon.streaming.feature_engineering.features.compute_features import (
    resample_and_compute_rolling_features,
)
from polyhorizon.streaming.feature_engineering.features.read_delta import read_from_delta
from polyhorizon.streaming.feature_engineering.features.write_features import write_features
from polyhorizon.streaming.feature_engineering.utils.spark_session import get_spark_session
from polyhorizon.streaming.utils.logger import get_logger

logger = get_logger("FeatureEngineering")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute streaming features")
    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Path to streaming config YAML",
    )
    parser.add_argument(
        "--rolling-features",
        default=None,
        help="Comma-separated list of rolling features",
    )
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="Write output as a single file",
    )
    parser.add_argument(
        "--mode",
        default="overwrite",
        help="Write mode for Delta output",
    )
    return parser.parse_args()


def _parse_feature_list(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    return [item.strip() for item in value.split(",") if item.strip()]


def main() -> None:
    args = _parse_args()
    config = load_config(args.config_path)

    spark = None
    try:
        logger.info("Starting feature engineering pipeline")
        spark = get_spark_session("StockFeatureComputation", config)

        logger.info("Reading source table: %s", config.streaming_storage.source_table)
        df = read_from_delta(spark, config.streaming_storage.source_table)

        rolling_features = _parse_feature_list(args.rolling_features)
        feature_df = resample_and_compute_rolling_features(
            df,
            features_config=config.features,
            rolling_features=rolling_features,
        )

        logger.info("Writing features to %s", config.streaming_storage.feature_output_path)
        write_features(
            feature_df,
            config.streaming_storage.feature_output_path,
            mode=args.mode,
            single_file=args.single_file,
        )

        logger.info("Feature pipeline completed successfully")
    except Exception:
        logger.error("Feature pipeline failed", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
