"""Opt-in real scheduler → process worker → Kafka → Spark → publication rehearsal."""
import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.getenv("ORCHESTRATION_E2E") != "1", reason="Requires isolated orchestration Compose stack")
ROOT = Path(__file__).resolve().parents[3]


def stop_process(process):
    if process is None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def test_scheduler_to_kafka_to_feast(tmp_path):
    import httpx
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.actions import DeploymentScheduleCreate, WorkPoolCreate
    from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterDeploymentId
    from prefect.client.schemas.schedules import RRuleSchedule
    from prefect.settings import PREFECT_API_URL, PREFECT_API_KEY, temporary_settings
    from prefect.workers.process import ProcessWorker

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    api = f"http://127.0.0.1:{port}/api"
    # Do not inherit application endpoints, secrets, Prefect profiles, or .env.
    env = {key: os.environ[key] for key in ("PATH", "HOME", "JAVA_HOME", "TMPDIR", "LANG") if key in os.environ}
    env.update({
        "PATH": str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", ""),
        "PYTHONPATH": str(ROOT), "PYTHON_DOTENV_DISABLED": "1", "PYSPARK_PYTHON": sys.executable,
        "PREFECT_HOME": str(tmp_path / "prefect-home"), "PREFECT_API_URL": api,
        "PREFECT_SERVER_ANALYTICS_ENABLED": "false", "PREFECT_SERVER_EPHEMERAL_ENABLED": "false",
        "PREFECT_SERVER_SERVICES_SCHEDULER_LOOP_SECONDS": "1",
        "PREFECT_SERVER_SERVICES_SCHEDULER_RECENT_DEPLOYMENTS_LOOP_SECONDS": "1",
        "PREFECT_WORKER_QUERY_SECONDS": "1", "PREFECT_WORKER_PREFETCH_SECONDS": "0",
        "ORCHESTRATION_REHEARSAL_ROOT": str(tmp_path),
    })
    cli = str(Path(sys.executable).with_name("prefect"))
    server = worker = None
    server_log = tmp_path / "server.log"
    worker_log = tmp_path / "worker.log"
    print(f"Rehearsal logs: {tmp_path}", flush=True)
    with server_log.open("w") as server_output, worker_log.open("w") as worker_output:
        try:
            server = subprocess.Popen([cli, "server", "start", "--host", "127.0.0.1", "--port", str(port),
                "--no-ui", "--analytics-off"], cwd=tmp_path, env=env, stdout=server_output,
                stderr=subprocess.STDOUT, start_new_session=True)
            deadline = time.monotonic() + 90
            while True:
                assert server.poll() is None, f"Prefect server exited; see {server_log}"
                try:
                    if httpx.get(f"{api}/health", timeout=2).status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                assert time.monotonic() < deadline, f"Server startup timed out; see {server_log}"
                time.sleep(1)
            with temporary_settings({PREFECT_API_URL: api, PREFECT_API_KEY: None}), get_client(sync_client=True) as client:
                pool = "isolated-rehearsal-pool"
                client.create_work_pool(WorkPoolCreate(name=pool, type="process",
                    base_job_template=ProcessWorker.get_default_base_job_template()))
                child_id = client.create_flow_from_name("feature-store-flow")
                child = client.create_deployment(flow_id=child_id, name="feature-store-after-close", schedules=[],
                    work_pool_name=pool, path=str(ROOT),
                    pull_steps=[{"prefect.deployments.steps.set_working_directory": {"directory": str(ROOT)}}],
                    entrypoint="tests/integration/orchestration/rehearsal.py:publish_rehearsal",
                    job_variables={"env": env}, concurrency_limit=1)
                parent_id = client.create_flow_from_name("kafka-publication-rehearsal")
                due = datetime.now(timezone.utc) + timedelta(seconds=15)
                # COUNT=1 proves a genuine scheduler-created run without leaving
                # a recurring job behind or manually creating a flow run.
                schedule = RRuleSchedule(rrule=f"DTSTART:{due.strftime('%Y%m%dT%H%M%SZ')}\nRRULE:FREQ=DAILY;COUNT=1", timezone="UTC")
                parent = client.create_deployment(flow_id=parent_id, name="scheduled-once", work_pool_name=pool,
                    path=str(ROOT), entrypoint="tests/integration/orchestration/rehearsal.py:scheduled_rehearsal",
                    pull_steps=[{"prefect.deployments.steps.set_working_directory": {"directory": str(ROOT)}}],
                    schedules=[DeploymentScheduleCreate(schedule=schedule)], job_variables={"env": env}, concurrency_limit=1)
                assert not client.read_deployment(child).schedules
                worker = subprocess.Popen([cli, "worker", "start", "--pool", pool, "--type", "process", "--limit", "2"],
                    cwd=tmp_path, env=env, stdout=worker_output, stderr=subprocess.STDOUT, start_new_session=True)
                deadline = time.monotonic() + 660
                while True:
                    assert worker.poll() is None, f"Worker exited; see {worker_log}"
                    runs = client.read_flow_runs(flow_run_filter=FlowRunFilter(deployment_id=FlowRunFilterDeploymentId(any_=[parent])))
                    assert len(runs) <= 1, "One-shot schedule unexpectedly produced duplicate runs"
                    if runs and runs[0].state and runs[0].state.is_final():
                        run = runs[0]
                        assert run.state.is_completed(), f"Scheduled run {run.state.name}: {run.state.message}; see {worker_log}"
                        assert run.auto_scheduled
                        assert run.start_time >= due - timedelta(seconds=1)
                        break
                    assert time.monotonic() < deadline, f"Scheduled rehearsal timed out; see {worker_log}"
                    time.sleep(2)
                result = json.loads((tmp_path / "result.json").read_text())
                downstream = client.read_flow_run(result["feature_flow_run_id"])
                assert downstream.deployment_id == child
                assert downstream.state.is_completed()
                assert not downstream.auto_scheduled
                assert downstream.parameters["dataset_release"]["dataset_id"] == result["dataset_id"]
                assert result["sent"] == 10 and result["bronze_rows"] == 9 and result["dead_letter_rows"] == 1
                assert result["feature_rows"] == 6 and result["checkpoint_restart"] == "no_duplicates"
                print(json.dumps({"scheduled_flow_run_id": str(run.id), **result}), flush=True)
                client.delete_deployment(parent)
                client.delete_deployment(child)
        finally:
            stop_process(worker)
            stop_process(server)
