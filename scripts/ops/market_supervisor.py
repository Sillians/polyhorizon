"""Local NYSE supervisor. No publication, training, volume deletion or job replay.

Run via the supplied LaunchAgent. State is private, persistent and atomically saved.
Only the explicit collection Compose services and this session's Prefect containers
are eligible for shutdown. API failures prevent shutdown (fail safe).
"""
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from polyhorizon.streaming.src.market_schedule import get_market_session

ROOT = Path(__file__).resolve().parents[2]
NY = ZoneInfo("America/New_York")
SERVICES = ["postgres", "zookeeper", "kafka-broker-1", "kafka-broker-2",
            "master", "volume", "filer", "seaweedfs-s3",
            "prometheus-pushgateway", "spark-master", "spark-worker", "prefect-server"]
WORKERS = ["local-worker-ingestion", "local-worker-streaming"]
DEPLOYMENTS = ["sp500-universe-weekdays", "streaming-market-hours", "ingestion-market-hours"]
TERMINAL = {"COMPLETED", "FAILED", "CRASHED", "CANCELLED"}
COMPOSE = ["docker", "compose", "-f", str(ROOT / "docker-compose.yaml"),
           "-f", str(ROOT / "docker-compose.local-workers.yaml"),
           "-f", str(ROOT / "docker-compose.low-memory.yaml")]


def run(command, timeout=60):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        # Do not persist stderr: Docker/HTTP errors can contain credentials.
        raise RuntimeError(f"{command[0]} command failed (exit {result.returncode})")
    return result.stdout


def api(path, body=None):
    req = Request("http://localhost:4200/api/" + path,
                  data=None if body is None else json.dumps(body).encode(),
                  headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=15) as response:
        return json.load(response)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temp.chmod(0o600)
    temp.replace(path)


def window(now):
    now = now.astimezone(NY)
    session = get_market_session(now, "NYSE", "America/New_York")
    if session is None:
        return None
    opening, close = session
    return opening - timedelta(minutes=20), opening, close, close + timedelta(minutes=20)


def flow_runs(day):
    deployments = api("deployments/filter", {"deployments": {"name": {"any_": DEPLOYMENTS}}, "limit": 100})
    if {d["name"] for d in deployments} != set(DEPLOYMENTS):
        raise RuntimeError("Collection deployments missing")
    start = datetime.fromisoformat(day).replace(tzinfo=NY)
    runs = api("flow_runs/filter", {
        "deployments": {"id": {"any_": [d["id"] for d in deployments]}},
        "flow_runs": {"expected_start_time": {"after_": start.isoformat(),
                         "before_": (start + timedelta(days=1)).isoformat()}},
        "limit": 200,
    })
    if len(runs) >= 200:
        raise RuntimeError("Session flow query exceeded safe bound")
    return runs, {d["id"]: d["name"] for d in deployments}


def active_runs(runs):
    return [r for r in runs if r["state_type"] not in TERMINAL | {"SCHEDULED"}]


def owned_containers(run_ids):
    ids = run(["docker", "ps", "-q", "--filter", "label=io.prefect.flow-run-id"]).split()
    if not ids:
        return []
    inspected = json.loads(run(["docker", "inspect", *ids]))
    return [c["Id"] for c in inspected
            if c["Config"].get("Labels", {}).get("io.prefect.flow-run-id") in run_ids]


class Supervisor:
    def __init__(self, directory):
        self.directory = directory
        self.path = directory / "state.json"
        self.state = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.awake = None

    def persist(self):
        save(self.path, self.state)

    def notify(self, key, message):
        if self.state.get("notice") == key:
            return
        print(message, flush=True)
        self.state["incidents"] = (self.state.get("incidents", []) + [
            {"at": datetime.now(timezone.utc).isoformat(), "message": message}])[-200:]
        # Fixed, non-sensitive local notification; report has the detailed evidence.
        if sys.platform == "darwin":
            try:
                run(["osascript", "-e", 'display notification "Check the local session report or supervisor log." with title "Polyhorizon session update"'])
            except Exception:
                pass  # Log remains authoritative; notification permissions may be denied.
        self.state["notice"] = key
        self.persist()

    def keep_awake(self, enabled):
        if enabled and (self.awake is None or self.awake.poll() is not None) and sys.platform == "darwin":
            self.awake = subprocess.Popen(["/usr/bin/caffeinate", "-is", "-w", str(os.getpid())])
        elif not enabled and self.awake is not None:
            self.awake.terminate()
            self.awake.wait(timeout=5)
            self.awake = None

    def startup(self):
        tries = self.state.get("startup_attempts", 0)
        if tries >= 3:
            raise RuntimeError("Startup retry budget exhausted; operator intervention required")
        if not (ROOT / "artifacts/low-memory/kafka-verified.json").exists():
            raise RuntimeError("Two-broker migration must be verified before low-memory startup")
        self.state["startup_attempts"] = tries + 1
        self.persist()
        try:
            run(["docker", "info", "--format", "{{.ServerVersion}}"])
        except Exception:
            if sys.platform == "darwin":
                context = run(["docker", "context", "show"]).strip()
                run(["open", "-a", "OrbStack" if context == "orbstack" else "Docker"])
            raise RuntimeError("Docker unavailable; requested local engine startup")
        # Persist before mutation: a partially successful compose start also needs cleanup.
        self.state["infrastructure_started"] = True
        self.persist()
        run(COMPOSE + ["up", "-d", "--no-build", "--scale", "spark-worker=1", "--wait", "--wait-timeout", "180", *SERVICES], 240)
        api("health")
        # Registration is idempotent and only enables collection, never downstream ML.
        with (ROOT / "scripts/deploy/register_local.py").open() as source:
            result = subprocess.run(COMPOSE + ["exec", "-T", "prefect-server", "python", "-",
                "--env-file", str(ROOT / ".env"), "--activate", "--low-memory"], stdin=source,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=ROOT, timeout=90)
        if result.returncode:
            raise RuntimeError("Deployment registration failed")
        run(COMPOSE + ["up", "-d", "--no-build", *WORKERS], 90)
        self.state["started"] = True
        self.persist()

    def observe(self, now, opening):
        runs, names = flow_runs(self.state["day"])
        problems = []
        containers = run(COMPOSE + ["ps", "--format", "json"])
        present = set()
        for line in containers.splitlines():
            item = json.loads(line)
            if item.get("State") == "running":
                present.add(item.get("Service"))
            if item.get("Service") in SERVICES + WORKERS and item.get("Health") == "unhealthy":
                problems.append(item["Service"] + " is unhealthy")
        for missing in sorted(set(SERVICES + WORKERS) - present):
            problems.append(missing + " is not running")
        summary = [{"id": r["id"], "deployment": names[r["deployment_id"]],
                    "state": r["state_type"]} for r in runs]
        for name in DEPLOYMENTS:
            matching = [r for r in summary if r["deployment"] == name]
            if now > opening + timedelta(minutes=2) and not any(r["state"] in {"RUNNING", "COMPLETED"} for r in matching):
                problems.append(name + " has not started successfully")
        apps = api_spark()
        if now > opening + timedelta(minutes=2) and not apps:
            problems.append("Spark application absent")
        offsets = run(COMPOSE + ["exec", "-T", "kafka-broker-1", "kafka-run-class",
            "kafka.tools.GetOffsetShell", "--broker-list", "kafka-broker-1:29092",
            "--topic", "stock-trades", "--time", "-1"], 30)
        offsets = {line.split(":")[1]: int(line.split(":")[2]) for line in offsets.splitlines()
                   if line.startswith("stock-trades:")}
        if not offsets:
            problems.append("Kafka offset probe returned no partitions")
        previous = self.state.get("probe", {})
        if offsets != previous.get("offsets"):
            self.state["kafka_changed"] = now.isoformat()
        changed = datetime.fromisoformat(self.state.get("kafka_changed", now.isoformat()))
        if (now - changed).total_seconds() > 180:
            problems.append("Kafka offsets stalled for over 180 seconds")
        # Read-only Delta commit probe runs isolated so a slow object store is bounded.
        delta = json.loads(run([sys.executable, "-m", "scripts.ops.session_report", "--probe"], 45))
        if delta.get("bronze_version") != previous.get("delta", {}).get("bronze_version"):
            self.state["delta_changed"] = now.isoformat()
        changed = datetime.fromisoformat(self.state.get("delta_changed", now.isoformat()))
        if (now - changed).total_seconds() > 300:
            problems.append("Bronze commits stalled for over 300 seconds")
        if delta.get("event_age_seconds", 0) > 300:
            problems.append("Bronze committed event time is over 300 seconds behind")
        if shutil.disk_usage(ROOT).free < 10 * 1024**3:
            problems.append("Host disk has less than 10 GiB free; inspect Docker disk allocation too")
        disk = run(COMPOSE + ["exec", "-T", "kafka-broker-1", "df", "-Pk", "/var/lib/kafka/data"])
        if int(disk.splitlines()[-1].split()[3]) < 10 * 1024**2:
            problems.append("Docker data disk has less than 10 GiB free")
        self.state["probe"] = {"at": now.isoformat(), "offsets": offsets, "delta": delta, "runs": summary}
        self.state["problems"] = problems
        self.persist()
        if problems:
            self.notify("|".join(problems), "; ".join(problems))
        elif self.state.get("notice"):
            self.notify("", "Session checks recovered")

    def finish(self, now, close, stop):
        runs, names = flow_runs(self.state["day"])
        running = active_runs(runs)
        if running and now < close + timedelta(minutes=60):
            self.notify("draining", "Waiting for session jobs and qualification before shutdown")
            return
        if running:
            # Cancel only today's explicitly selected deployments. Re-check next tick.
            for item in running:
                api(f"flow_runs/{item['id']}/set_state", {"state": {"type": "CANCELLING", "name": "Cancelling"}})
            self.state["forced_cleanup"] = True
            self.persist()
        containers = owned_containers({r["id"] for r in runs})
        if containers:
            run(["docker", "stop", "--time", "30", *containers], 120)
        if owned_containers({r["id"] for r in runs}):
            raise RuntimeError("Session containers remain; refusing infrastructure shutdown")
        # Shared infrastructure must not be stopped beneath unrelated work.
        other = api("flow_runs/filter", {"flow_runs": {"state": {"type": {"any_": ["RUNNING", "PENDING", "CANCELLING"]}}}, "limit": 200})
        if any(r["id"] not in {s["id"] for s in runs} for r in other) or len(other) >= 200:
            raise RuntimeError("Unrelated Prefect work active; refusing infrastructure shutdown")
        if api_spark(all_apps=True):
            raise RuntimeError("Spark application still active; refusing infrastructure shutdown")
        try:
            report = json.loads(run([sys.executable, "-m", "scripts.ops.session_report", self.state["day"]], 180))
        except Exception as exc:
            report = {"session": self.state["day"], "status": "operational_failure", "error_type": type(exc).__name__,
                      "published": False, "trained": False, "promoted": False}
        report["runs"] = [{"id": r["id"], "deployment": names[r["deployment_id"]], "state": r["state_type"]} for r in runs]
        report["incidents"] = self.state.get("incidents", [])
        report["last_probe"] = self.state.get("probe")
        report["data_status"] = report["status"]
        if self.state.get("forced_cleanup") or any(r["state_type"] != "COMPLETED" for r in runs):
            report["status"] = "operational_failure"
        if not all(any(names[r["deployment_id"]] == name and r["state_type"] == "COMPLETED" for r in runs) for name in DEPLOYMENTS):
            report["status"] = "operational_failure"
        save(self.directory / (self.state["day"] + ".json"), report)
        # Never compose down: no volumes, networks or unrelated services are removed.
        run(COMPOSE + ["stop", "--timeout", "30", *WORKERS], 90)
        run(COMPOSE + ["stop", "--timeout", "60", *SERVICES], 180)
        self.state["finished"] = True
        self.persist()
        self.keep_awake(False)
        self.notify("finished", f"Session {self.state['day']}: {report['status']}; collection containers stopped")

    def tick(self, now):
        now = now.astimezone(NY)
        # A wake/restart after midnight still reconciles the prior owned session.
        if self.state.get("infrastructure_started") and not self.state.get("finished") and self.state.get("day") != str(now.date()):
            old = window(datetime.fromisoformat(self.state["day"]).replace(tzinfo=NY))
            self.keep_awake(True)
            self.finish(now, old[2], old[3])
            return
        bounds = window(now)
        if bounds is None:
            self.keep_awake(False)
            return
        start, opening, close, stop = bounds
        if now < start:
            self.keep_awake(False)
            return
        if self.state.get("day") != str(now.date()):
            self.state = {"day": str(now.date())}
            self.persist()
        if self.state.get("finished"):
            self.keep_awake(False)
            return
        if not self.state.get("started") and now >= close and not self.state.get("infrastructure_started"):
            return  # Do not boot infrastructure for a session we missed entirely.
        self.keep_awake(True)
        if now >= stop:
            self.finish(now, close, stop)
            return
        if not self.state.get("started") and now < close:
            self.startup()
        if opening <= now < close:
            self.observe(now, opening)


def api_spark(all_apps=False):
    with urlopen("http://localhost:18080/json/", timeout=10) as response:
        return [a for a in json.load(response).get("activeapps", [])
                if all_apps or (a.get("name") == "Polyhorizon_streaming_job" and a.get("state") == "RUNNING")]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--plan", action="store_true", help="Read-only calendar preview; no Docker changes")
    parser.add_argument("--state-dir", type=Path, default=ROOT / "artifacts/market-supervisor")
    args = parser.parse_args()
    if args.plan:
        print(json.dumps({"window": window(datetime.now(timezone.utc)), "services": SERVICES + WORKERS}, default=str, indent=2))
        return
    args.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (args.state_dir / "supervisor.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        supervisor = Supervisor(args.state_dir)
        def stop(*_):
            raise KeyboardInterrupt
        signal.signal(signal.SIGTERM, stop)
        try:
            while True:
                try:
                    supervisor.tick(datetime.now(timezone.utc))
                except Exception as exc:
                    # All explicit RuntimeErrors in this module are sanitized diagnostics.
                    detail = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                    supervisor.state["last_error"] = {"at": datetime.now(timezone.utc).isoformat(), "detail": detail}
                    supervisor.persist()
                    supervisor.notify(detail, "Supervisor check failed: " + detail)
                if args.once:
                    break
                time.sleep(60)
        finally:
            supervisor.keep_awake(False)


if __name__ == "__main__":
    main()
