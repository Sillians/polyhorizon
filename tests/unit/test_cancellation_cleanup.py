import asyncio
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock, AsyncMock

import pytest

from polyhorizon.core.subprocess_lifecycle import run_owned_process


def test_owned_process_preserves_failure():
    with pytest.raises(subprocess.CalledProcessError):
        run_owned_process([sys.executable, "-c", "raise SystemExit(3)"])


@pytest.mark.parametrize("ignore_term", [False, True])
def test_sigterm_reaps_child(ignore_term):
    child_code = "import os,time,signal; " + ("signal.signal(signal.SIGTERM,signal.SIG_IGN); " if ignore_term else "")
    child_code += "print(os.getpid(),flush=True); time.sleep(120)"
    code = f"from polyhorizon.core.subprocess_lifecycle import run_owned_process; import sys; run_owned_process([sys.executable, '-u', '-c', {child_code!r}], grace_seconds=0.2)"
    wrapper = subprocess.Popen([sys.executable, "-u", "-c", code], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        child = int(wrapper.stdout.readline())
        wrapper.terminate()
        wrapper.wait(timeout=10)
        assert wrapper.returncode != 0
        with pytest.raises(ProcessLookupError):
            os.kill(child, 0)
    finally:
        if wrapper.poll() is None:
            wrapper.kill()
        wrapper.wait()


def test_pipeline_cancellation_awaits_both_tasks(monkeypatch):
    from polyhorizon.ingestion.flow import producer_consumer_streaming_flow as flow
    stopped = []
    async def task(cfg, seconds):
        try:
            await asyncio.sleep(120)
        finally:
            stopped.append(True)
    monkeypatch.setattr(flow, "run_producer_task", task)
    monkeypatch.setattr(flow, "run_consumer_task", task)
    async def check():
        running = asyncio.create_task(flow.run_pipeline(None, 120))
        await asyncio.sleep(0.02)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert len(stopped) == 2
    asyncio.run(check())


def test_consumer_cancellation_closes_context_without_task_submission(monkeypatch):
    from polyhorizon.ingestion.tasks import producer_consumer_streaming_tasks as tasks
    consumer = Mock(is_running=True, metrics={}, thread_error=None)
    consumer.__enter__ = Mock(return_value=consumer)
    consumer.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(tasks, "FinnhubConsumer", Mock(return_value=consumer))
    monkeypatch.setattr(tasks, "get_run_logger", Mock())
    monkeypatch.setattr(tasks.consumer_health_check_task, "fn", Mock())
    submit = Mock(side_effect=AssertionError("must not use Prefect task runner"))
    monkeypatch.setattr(tasks.consumer_health_check_task, "submit", submit)
    cfg = SimpleNamespace(monitoring_and_metrics=SimpleNamespace(health_check_interval=30))
    async def check():
        running = asyncio.create_task(tasks.run_consumer_task.fn(cfg, 120))
        await asyncio.sleep(0.02)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    asyncio.run(check())
    consumer.__exit__.assert_called_once()
    submit.assert_not_called()


def test_producer_stops_even_if_health_loop_fails(monkeypatch):
    from polyhorizon.ingestion.tasks import producer_consumer_streaming_tasks as tasks
    producer = Mock(start=AsyncMock(), stop=AsyncMock(), metrics={})
    async def start():
        await asyncio.sleep(0.05)
    producer.start.side_effect = start
    monkeypatch.setattr(tasks, "FinnhubProducer", Mock(return_value=producer))
    monkeypatch.setattr(tasks, "get_run_logger", Mock())
    monkeypatch.setattr(tasks.producer_health_check_task, "fn", Mock(side_effect=RuntimeError("health failed")))
    cfg = SimpleNamespace(monitoring_and_metrics=SimpleNamespace(health_check_interval=30))
    with pytest.raises(RuntimeError, match="health failed"):
        asyncio.run(tasks.run_producer_task.fn(cfg, 10))
    producer.stop.assert_awaited_once()
