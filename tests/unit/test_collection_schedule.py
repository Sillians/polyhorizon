from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
import asyncio
import pytest

from polyhorizon.core.session_window import collection_session
from scripts.deploy.register_local import specifications, COLLECTION_DEPLOYMENTS


def utc(value):
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)


@pytest.mark.parametrize("now,eligible", [
    ("2026-09-30T13:25:00", True), ("2026-09-30T13:29:00", True),
    ("2026-09-30T13:30:00", True), ("2026-09-30T20:00:00", False),
    ("2026-09-30T10:00:00", False), ("2026-09-26T13:30:00", False),
    ("2026-12-25T14:30:00", False),
    ("2026-11-27T17:59:00", True), ("2026-11-27T18:00:00", False),
    ("2026-11-02T14:25:00", True), ("2026-11-02T13:25:00", False),
])
def test_calendar_windows(now, eligible):
    assert (collection_session(utc(now)) is not None) == eligible


def test_previous_day_backlog_is_not_replayed():
    assert collection_session(utc("2026-09-30T13:30:00"), utc("2026-09-29T13:30:00")) is None


def test_scheduled_start_without_a_flow_context():
    from polyhorizon.core.session_window import scheduled_start
    assert scheduled_start() is None


def test_only_collection_is_activated_and_spark_is_prepared_first():
    specs = {d["name"]: d for d in specifications("/repo/.env", "test-network")}
    assert COLLECTION_DEPLOYMENTS == {"sp500-universe-weekdays", "ingestion-market-hours", "streaming-market-hours"}
    assert specs["streaming-market-hours"]["cron"] == "25 9 * * 1-5"
    assert specs["ingestion-market-hours"]["cron"] == "29 9 * * 1-5"
    assert specs["streaming-market-hours"]["parameters"] == {"qualification_only": True}
    assert specs["streaming-market-hours"]["job_variables"]["env"]["SPARK_MASTER_URL"] == "spark://spark-master:7077"


def test_task_retry_after_close_does_not_reopen_connections(monkeypatch):
    from polyhorizon.ingestion.tasks import producer_consumer_streaming_tasks as tasks
    producer, consumer = Mock(), Mock()
    monkeypatch.setattr(tasks, "FinnhubProducer", producer)
    monkeypatch.setattr(tasks, "FinnhubConsumer", consumer)
    monkeypatch.setattr(tasks, "get_run_logger", Mock())
    cfg = SimpleNamespace(monitoring_and_metrics=SimpleNamespace(health_check_interval=30))
    async def check():
        await tasks.run_producer_task.fn(cfg, 600, stop_at="2000-01-01T20:00:00+00:00")
        await tasks.run_consumer_task.fn(cfg, 600, stop_at="2000-01-01T20:00:00+00:00")
    asyncio.run(check())
    producer.assert_not_called()
    consumer.assert_not_called()
