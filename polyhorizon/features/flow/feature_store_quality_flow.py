from __future__ import annotations

from prefect import flow, get_run_logger

from polyhorizon.features.tasks.feast_tasks import validate_snapshot
from polyhorizon.features.feast_ops.feature_manager import FeatureManager
from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.utils.metrics import FeatureMetricsPublisher


@flow(
    name="feature-store-quality-flow",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=60 * 60,
)
def feature_store_quality_flow(run_drift_check: bool = True) -> dict:
    logger = get_run_logger()
    validate_snapshot()

    config = load_config()
    fm = FeatureManager(config)
    metrics = FeatureMetricsPublisher(config.monitoring)

    freshness = fm.get_snapshot_freshness()
    logger.info("Feature freshness: %s", freshness)
    metrics.push_freshness(freshness.get("staleness_seconds"))

    if run_drift_check:
        drift_report = fm.compute_snapshot_drift()
        if drift_report.empty:
            logger.info("Drift report: no data or no drift detected")
            metrics.push_drift_count(0)
        else:
            detected = drift_report[drift_report["drift_detected"]]
            logger.info(
                "Drift report: %s features checked, %s drifted",
                len(drift_report),
                len(detected),
            )
            metrics.push_drift_count(len(detected))

    return {"status": "success"}
