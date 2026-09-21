from __future__ import annotations

import math
import time
from typing import Dict, Optional

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from polyhorizon.training.configs.settings import MonitoringConfig
from polyhorizon.training.utils.logger import get_logger


class TrainingMetricsPublisher:
    def __init__(self, config: MonitoringConfig) -> None:
        self.config = config
        self.logger = get_logger("TrainingMetrics")

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
            push_to_gateway(self.config.pushgateway_url, job="training", registry=registry)
        except Exception:
            self.logger.warning("Failed to push metric %s to Pushgateway", name, exc_info=True)

    def push_run_metrics(self, duration_seconds: float, success: bool, timestamp: Optional[float] = None) -> None:
        if not self.config.enable_metrics:
            return
        prefix = self.config.metrics_prefix
        ts = float(timestamp if timestamp is not None else time.time())
        self._push_gauge(f"{prefix}_last_run_duration_seconds", float(duration_seconds), {})
        self._push_gauge(f"{prefix}_last_run_success", 1.0 if success else 0.0, {})
        self._push_gauge(f"{prefix}_last_run_timestamp", ts, {})

    def push_validation_metrics(self, val_loss: Optional[float]) -> None:
        if not self.config.enable_metrics:
            return
        if not _is_valid_number(val_loss):
            return
        prefix = self.config.metrics_prefix
        self._push_gauge(f"{prefix}_val_loss", float(val_loss), {})

    def push_promotion_metrics(self, promotion_result: Optional[Dict]) -> None:
        if not self.config.enable_metrics or not promotion_result:
            return
        prefix = self.config.metrics_prefix
        promoted = promotion_result.get("promoted")
        if promoted is not None:
            self._push_gauge(f"{prefix}_promotion_success", 1.0 if promoted else 0.0, {})

        improvement = promotion_result.get("improvement")
        if _is_valid_number(improvement):
            self._push_gauge(f"{prefix}_improvement_pct", float(improvement), {})

        challenger_metrics = promotion_result.get("challenger_metrics") or {}
        hit_rate = challenger_metrics.get("hit_rate")
        if _is_valid_number(hit_rate):
            self._push_gauge(f"{prefix}_challenger_hit_rate", float(hit_rate), {})
        mae = challenger_metrics.get("mae")
        if _is_valid_number(mae):
            self._push_gauge(f"{prefix}_challenger_mae", float(mae), {})
        score = challenger_metrics.get("composite_score")
        if _is_valid_number(score):
            self._push_gauge(f"{prefix}_challenger_score", float(score), {})
        stability = challenger_metrics.get("stability")
        if _is_valid_number(stability):
            self._push_gauge(f"{prefix}_challenger_stability", float(stability), {})


def _is_valid_number(value: Optional[float]) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return False
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False
