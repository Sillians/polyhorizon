import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from polyhorizon.ingestion.finnhub_producer.producer import FinnhubProducer


def producer():
    p = object.__new__(FinnhubProducer)
    p.config = SimpleNamespace(max_retries=3, connection_retry_delay=0, rate_limit_delay=0,
                               kafka_topic="trades", trade_stale_seconds=120, recovery_seconds=60)
    p.logger = Mock()
    p._is_running = True
    p._websocket = None
    p._subscribed_symbols = set()
    p._symbols = ["NVDA", "AAPL", "MSFT"]
    p._session_started = time.monotonic()
    p._session_healthy = False
    p._last_trades = {}
    p._messages_sent = 0
    p._producer = Mock()
    return p


def test_exhaustion_raises_and_closes_every_session():
    p = producer()
    ws = SimpleNamespace(close=AsyncMock())
    async def connect():
        p._websocket = ws
    p._connect_websocket = AsyncMock(side_effect=connect)
    p._subscribe_to_symbols = AsyncMock()
    p._run_websocket_loop = AsyncMock(side_effect=RuntimeError("disconnected"))
    with pytest.raises(RuntimeError, match="budget exhausted"):
        asyncio.run(p._handle_connection_with_retries())
    assert p._connect_websocket.await_count == 3
    assert ws.close.await_count == 3


def test_healthy_session_resets_failure_budget():
    p = producer()
    p._connect_websocket = AsyncMock()
    p._subscribe_to_symbols = AsyncMock()
    calls = []
    async def loop():
        calls.append(1)
        p._session_healthy = len(calls) == 3
        raise RuntimeError("disconnect")
    p._run_websocket_loop = loop
    with pytest.raises(RuntimeError, match="budget exhausted"):
        asyncio.run(p._handle_connection_with_retries())
    assert len(calls) == 5


def test_delivery_only_counts_acknowledgements():
    p = producer()
    asyncio.run(p._send_to_kafka("NVDA", {}))
    assert p._messages_sent == 1
    p._producer.send.return_value.get.side_effect = TimeoutError()
    with pytest.raises(RuntimeError, match="delivery failed"):
        asyncio.run(p._send_to_kafka("NVDA", {}))
    assert p._messages_sent == 1


def test_missing_symbol_stales_despite_other_activity():
    p = producer()
    p._session_started -= 121
    p._last_trades = {"NVDA": time.monotonic(), "MSFT": time.monotonic()}
    with pytest.raises(RuntimeError, match="AAPL"):
        p._check_trade_freshness()


def test_provider_error_is_not_ignored_or_echoed():
    p = producer()
    p._messages_received = 0
    with pytest.raises(RuntimeError, match="provider error") as e:
        asyncio.run(p._handle_websocket_message('{"type":"error","msg":"secret"}'))
    assert "secret" not in str(e.value)


def test_task_propagates_exhaustion(monkeypatch):
    from polyhorizon.ingestion.tasks import producer_consumer_streaming_tasks as tasks
    p = Mock()
    p.start = AsyncMock(side_effect=RuntimeError("budget exhausted"))
    p.stop = AsyncMock()
    monkeypatch.setattr(tasks, "FinnhubProducer", Mock(return_value=p))
    monkeypatch.setattr(tasks, "get_run_logger", Mock())
    monkeypatch.setattr(tasks.producer_health_check_task, "fn", Mock())
    cfg = SimpleNamespace(monitoring_and_metrics=SimpleNamespace(health_check_interval=30))
    with pytest.raises(RuntimeError, match="budget exhausted"):
        asyncio.run(tasks.run_producer_task.fn(cfg, 60))
    p.stop.assert_awaited_once()


def test_metrics_push_has_registry_and_timeout(monkeypatch):
    from polyhorizon.ingestion.tasks import producer_consumer_streaming_tasks as tasks
    push = Mock()
    monkeypatch.setattr(tasks, "push_to_gateway", push)
    monkeypatch.setattr(tasks, "get_run_logger", Mock())
    tasks.producer_health_check_task.fn({"symbol_trade_age_seconds": {"NVDA": 2}})
    tasks.consumer_health_check_task.fn({})
    assert push.call_count == 2
    for call in push.call_args_list:
        assert call.kwargs["registry"] is tasks.REGISTRY
        assert call.kwargs["timeout"] == 5


def test_silent_socket_triggers_watchdog(monkeypatch):
    p = producer()
    p._websocket = SimpleNamespace(recv=AsyncMock(side_effect=asyncio.TimeoutError))
    times = iter([p._session_started, p._session_started + 121])
    # Only the watchdog clock advances, avoiding changes to asyncio's clock.
    original = p._check_trade_freshness
    def check():
        now = next(times)
        with monkeypatch.context() as patch:
            patch.setattr("polyhorizon.ingestion.finnhub_producer.producer.time.monotonic", lambda: now)
            original()
    p._check_trade_freshness = check
    with pytest.raises(RuntimeError, match="freshness limit"):
        asyncio.run(p._run_websocket_loop())
