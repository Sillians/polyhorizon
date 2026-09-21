from __future__ import annotations

import json
from typing import Dict, Optional

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pyspark.sql.streaming import StreamingQueryListener

from polyhorizon.streaming.configs.settings import MonitoringAndMetricsConfig
from polyhorizon.streaming.utils.logger import get_logger


class StreamingMetricsPublisher:
    def __init__(self, config: MonitoringAndMetricsConfig) -> None:
        self.config = config
        self.logger = get_logger("StreamingMetrics")

    def _push_gauge(self, name: str, value: float, labels: Dict[str, str]) -> None:
        if not self.config.enable_metrics:
            return
        try:
            registry = CollectorRegistry()
            gauge = Gauge(name, name, list(labels.keys()), registry=registry)
            gauge.labels(**labels).set(value)
            push_to_gateway(self.config.pushgateway_url, job="streaming-job", registry=registry)
        except Exception:
            self.logger.warning("Failed to push metric %s to Pushgateway", name, exc_info=True)

    def push_batch_metrics(self, query_name: str, progress: Dict) -> None:
        if not self.config.enable_metrics:
            return

        num_input_rows = float(progress.get("numInputRows", 0))
        processed_rows_per_sec = float(progress.get("processedRowsPerSecond", 0.0) or 0.0)
        input_rows_per_sec = float(progress.get("inputRowsPerSecond", 0.0) or 0.0)
        duration_ms = float(progress.get("durationMs", {}).get("triggerExecution", 0.0))

        prefix = self.config.metrics_prefix
        labels = {"query": query_name}

        self._push_gauge(f"{prefix}_batch_input_rows", num_input_rows, labels)
        self._push_gauge(f"{prefix}_batch_duration_ms", duration_ms, labels)
        self._push_gauge(f"{prefix}_processed_rows_per_second", processed_rows_per_sec, labels)
        self._push_gauge(f"{prefix}_input_rows_per_second", input_rows_per_sec, labels)

        kafka_lag = _extract_kafka_lag(progress)
        if kafka_lag is not None:
            self._push_gauge(f"{prefix}_kafka_lag", float(kafka_lag), labels)

    def push_dead_letter_counts(self, reason: str, count: int) -> None:
        if not self.config.enable_metrics:
            return
        prefix = self.config.metrics_prefix
        labels = {"reason": reason}
        self._push_gauge(f"{prefix}_dead_letter_rows", float(count), labels)


class StreamingMetricsListener(StreamingQueryListener):
    def __init__(self, config: MonitoringAndMetricsConfig) -> None:
        super().__init__()
        self.publisher = StreamingMetricsPublisher(config)
        self.logger = get_logger("StreamingMetricsListener")

    def onQueryStarted(self, event) -> None:
        self.logger.info("Streaming query started: %s", event.name or event.id)

    def onQueryProgress(self, event) -> None:
        try:
            progress = json.loads(event.progress.json)
            query_name = progress.get("name") or event.progress.name or event.progress.id
            self.publisher.push_batch_metrics(query_name, progress)
        except Exception:
            self.logger.error("Failed to emit streaming metrics", exc_info=True)

    def onQueryTerminated(self, event) -> None:
        self.logger.info("Streaming query terminated: %s", event.id)


def _extract_kafka_lag(progress: Dict) -> Optional[int]:
    sources = progress.get("sources", [])
    if not sources:
        return None

    source = sources[0]
    latest = _parse_offsets(source.get("latestOffset"))
    end = _parse_offsets(source.get("endOffset"))

    if latest is None or end is None:
        return None

    lag = 0
    for topic, partitions in latest.items():
        end_partitions = end.get(topic, {})
        for partition, latest_offset in partitions.items():
            end_offset = end_partitions.get(partition, latest_offset)
            lag += max(0, latest_offset - end_offset)

    return lag


def _parse_offsets(offsets) -> Optional[Dict[str, Dict[str, int]]]:
    if offsets is None:
        return None
    if isinstance(offsets, str):
        try:
            offsets = json.loads(offsets)
        except json.JSONDecodeError:
            return None
    if not isinstance(offsets, dict):
        return None

    parsed: Dict[str, Dict[str, int]] = {}
    for topic, parts in offsets.items():
        if isinstance(parts, dict):
            parsed[topic] = {str(p): int(v) for p, v in parts.items()}
    return parsed
