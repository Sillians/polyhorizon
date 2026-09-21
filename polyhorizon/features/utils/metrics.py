from __future__ import annotations

from typing import Dict

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from polyhorizon.features.configs.settings import MonitoringConfig
from polyhorizon.features.utils.logger import get_logger


class FeatureMetricsPublisher:
    def __init__(self, config: MonitoringConfig) -> None:
        self.config = config
        self.logger = get_logger("FeatureMetrics")

    def _push_gauge(self, name: str, value: float, labels: Dict[str, str]) -> None:
        if not self.config.enable_metrics:
            return
        try:
            registry = CollectorRegistry()
            gauge = Gauge(name, name, list(labels.keys()), registry=registry)
            if labels:
                gauge.labels(**labels).set(value)
            else:
                gauge.set(value)
            push_to_gateway(self.config.pushgateway_url, job="feature-store", registry=registry)
        except Exception:
            self.logger.warning("Failed to push metric %s to Pushgateway", name, exc_info=True)

    def push_materialization_metrics(self, duration_seconds: float, incremental: bool, success: bool) -> None:
        prefix = self.config.metrics_prefix
        labels = {"incremental": str(incremental).lower()}
        self._push_gauge(f"{prefix}_materialization_duration_seconds", duration_seconds, labels)
        self._push_gauge(f"{prefix}_materialization_success", 1.0 if success else 0.0, labels)

    def push_freshness(self, staleness_seconds: float | None) -> None:
        if staleness_seconds is None:
            return
        prefix = self.config.metrics_prefix
        self._push_gauge(f"{prefix}_snapshot_staleness_seconds", float(staleness_seconds), {})

    def push_drift_count(self, drifted_count: int) -> None:
        prefix = self.config.metrics_prefix
        self._push_gauge(f"{prefix}_drifted_features", float(drifted_count), {})
