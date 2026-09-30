from __future__ import annotations

from typing import Dict

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

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
            pushadd_to_gateway(self.config.pushgateway_url, job="feature-store", registry=registry)
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

    def push_publication_metrics(self, *, success: bool, event_timestamp=None,
                                 interior_session_gaps: dict[str, int] | None = None) -> None:
        prefix = self.config.metrics_prefix
        self._push_gauge(f"{prefix}_publication_success", 1.0 if success else 0.0, {})
        if success and event_timestamp is not None:
            self._push_gauge(f"{prefix}_last_published_event_timestamp_seconds",
                             float(event_timestamp.timestamp()), {})
        if interior_session_gaps and self.config.enable_metrics:
            try:
                registry = CollectorRegistry()
                gauge = Gauge(f"{prefix}_interior_session_gaps", "Interior gaps in the latest completed session",
                              ["symbol"], registry=registry)
                for symbol, count in interior_session_gaps.items():
                    gauge.labels(symbol=symbol).set(float(count))
                pushadd_to_gateway(self.config.pushgateway_url, job="feature-store", registry=registry)
            except Exception:
                self.logger.warning("Failed to push interior session gaps", exc_info=True)
