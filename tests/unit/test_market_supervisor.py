from datetime import datetime
from unittest.mock import Mock

import pytest

from scripts.ops import market_supervisor as mod


def dt(value):
    return datetime.fromisoformat(value)


@pytest.mark.parametrize("day,opening,close", [
    ("2026-09-30T10:00:00-04:00", "09:30", "16:00"),
    ("2026-11-27T10:00:00-05:00", "09:30", "13:00"),
    ("2026-11-02T10:00:00-05:00", "09:30", "16:00"),
])
def test_calendar(day, opening, close):
    start, op, cl, stop = mod.window(dt(day))
    assert op.strftime("%H:%M") == opening
    assert cl.strftime("%H:%M") == close
    assert (op-start).total_seconds() == 1200
    assert (stop-cl).total_seconds() == 1200


@pytest.mark.parametrize("day", ["2026-12-25T10:00:00-05:00", "2026-10-03T10:00:00-04:00"])
def test_no_weekend_or_holiday(day):
    assert mod.window(dt(day)) is None


@pytest.fixture
def supervisor(tmp_path, monkeypatch):
    obj = mod.Supervisor(tmp_path)
    obj.keep_awake = Mock()
    obj.notify = Mock()
    monkeypatch.setattr(mod, "run", Mock(return_value=""))
    return obj


def test_preopen_starts_once(supervisor):
    def start():
        supervisor.state["started"] = True
    supervisor.startup = Mock(side_effect=start)
    supervisor.tick(dt("2026-09-30T09:09:00-04:00"))
    supervisor.startup.assert_not_called()
    supervisor.tick(dt("2026-09-30T09:10:00-04:00"))
    supervisor.tick(dt("2026-09-30T09:11:00-04:00"))
    supervisor.startup.assert_called_once()


def test_missed_session_does_not_start(supervisor):
    supervisor.startup = Mock()
    supervisor.tick(dt("2026-09-30T17:00:00-04:00"))
    supervisor.startup.assert_not_called()


def test_retry_budget(supervisor):
    supervisor.state["startup_attempts"] = 3
    with pytest.raises(RuntimeError, match="budget"):
        supervisor.startup()
    mod.run.assert_not_called()


def test_waits_for_qualification(supervisor, monkeypatch):
    supervisor.state = {"day": "2026-09-30"}
    monkeypatch.setattr(mod, "flow_runs", lambda _: ([{"state_type": "RUNNING"}], {}))
    supervisor.finish(dt("2026-09-30T16:20:00-04:00"), dt("2026-09-30T16:00:00-04:00"), None)
    mod.run.assert_not_called()


def test_only_matching_container_is_owned(monkeypatch):
    import json
    calls = Mock(side_effect=["a\nb", json.dumps([
        {"Id": "a", "Config": {"Labels": {"io.prefect.flow-run-id": "owned"}}},
        {"Id": "b", "Config": {"Labels": {"io.prefect.flow-run-id": "unrelated"}}}])])
    monkeypatch.setattr(mod, "run", calls)
    assert mod.owned_containers({"owned"}) == ["a"]


def completed(monkeypatch):
    runs = [{"id": str(i), "deployment_id": str(i), "state_type": "COMPLETED"} for i in range(3)]
    monkeypatch.setattr(mod, "flow_runs", lambda _: (runs, dict(zip(["0", "1", "2"], mod.DEPLOYMENTS))))
    monkeypatch.setattr(mod, "owned_containers", lambda _: [])
    monkeypatch.setattr(mod, "api_spark", lambda **kw: [])
    return runs


def test_unrelated_work_blocks_stop(supervisor, monkeypatch):
    supervisor.state = {"day": "2026-09-30"}
    completed(monkeypatch)
    monkeypatch.setattr(mod, "api", lambda *a: [{"id": "unrelated"}])
    with pytest.raises(RuntimeError, match="Unrelated"):
        supervisor.finish(dt("2026-09-30T16:20:00-04:00"), dt("2026-09-30T16:00:00-04:00"), None)
    mod.run.assert_not_called()


def test_stop_preserves_data_and_saves_report(supervisor, monkeypatch):
    import json
    supervisor.state = {"day": "2026-09-30"}
    completed(monkeypatch)
    monkeypatch.setattr(mod, "api", lambda *a: [])
    mod.run.return_value = '{"status":"qualified"}'
    supervisor.finish(dt("2026-09-30T16:20:00-04:00"), dt("2026-09-30T16:00:00-04:00"), None)
    assert supervisor.state["finished"]
    assert json.loads((supervisor.directory / "2026-09-30.json").read_text())["status"] == "qualified"
    commands = [call.args[0] for call in mod.run.call_args_list]
    assert len([c for c in commands if "stop" in c]) == 2
    assert not any("down" in c or "rm" in c or "--volumes" in c for c in commands)


def test_report_failure_still_recorded_before_stop(supervisor, monkeypatch):
    import json
    supervisor.state = {"day": "2026-09-30"}
    completed(monkeypatch)
    monkeypatch.setattr(mod, "api", lambda *a: [])
    mod.run.side_effect = [TimeoutError(), "", ""]
    supervisor.finish(dt("2026-09-30T16:20:00-04:00"), dt("2026-09-30T16:00:00-04:00"), None)
    assert json.loads((supervisor.directory / "2026-09-30.json").read_text())["status"] == "operational_failure"


def test_restart_persists_state(supervisor):
    supervisor.state = {"day": "2026-09-30", "startup_attempts": 2}
    supervisor.persist()
    assert mod.Supervisor(supervisor.directory).state == supervisor.state


def test_launchagent_uses_absolute_paths():
    from scripts.ops.install_market_supervisor import definition
    conf = definition()
    assert conf["ProgramArguments"][0].startswith("/")
    assert conf["KeepAlive"] and conf["RunAtLoad"]
