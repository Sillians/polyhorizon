from __future__ import annotations

from prefect import flow, get_run_logger

from polyhorizon.features.tasks.feast_tasks import (
    publish_feature_release,
)
from polyhorizon.features.feast_ops.feature_manager import FeatureManager
from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.utils.metrics import FeatureMetricsPublisher


@flow(
    name="feature-store-flow",
    retries=2,
    retry_delay_seconds=120,
    timeout_seconds=2 * 60 * 60,
)
def feature_store_flow(
    dataset_release: dict | None = None,
    apply_changes: bool = True,
    materialize: bool = True,
    run_observability: bool = True,
    run_drift_check: bool = True,
) -> dict:
    logger = get_run_logger()

    if dataset_release is None:
        raise ValueError("A completed, version-pinned dataset_release is required")
    if not materialize:
        raise ValueError("Versioned publication must finish the PostgreSQL to Feast handoff")
    result = publish_feature_release(dataset_release, apply_changes)

    if run_observability:
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

    logger.info("Feature store flow completed successfully")
    return result
