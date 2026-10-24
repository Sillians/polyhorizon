"""Real local backup/restore and Alertmanager delivery, never production proof."""
import json
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest
import requests

pytestmark = pytest.mark.skipif(os.getenv("PUBLICATION_E2E") != "1", reason="Requires isolated publication Compose stack")
COMPOSE = ["docker", "compose", "-f", "tests/integration/features/compose.yaml", "exec", "-T", "postgres"]


def command(*args, input=None):
    return subprocess.run(COMPOSE + list(args), input=input, check=True, capture_output=True).stdout


def test_postgres_restore_preserves_ml_lineage():
    source, restored = "drill_" + uuid4().hex, "restore_" + uuid4().hex
    command("createdb", "-U", "postgres", source)
    try:
        command("createdb", "-U", "postgres", restored)
        try:
            command("psql", "-U", "postgres", "-d", source, "-v", "ON_ERROR_STOP=1", "-c",
                    "CREATE TABLE dataset_publications(dataset_id text primary key, manifest jsonb, status text); "
                    "INSERT INTO dataset_publications VALUES ('fixture', '{\"policy\":\"historical\"}', 'complete'); "
                    "CREATE TABLE features(symbol text, dataset_version text references dataset_publications(dataset_id)); "
                    "INSERT INTO features VALUES ('NVDA','fixture'),('AAPL','fixture'),('MSFT','fixture');")
            dump = command("pg_dump", "-U", "postgres", "-Fc", source)
            command("pg_restore", "-U", "postgres", "--exit-on-error", "--no-owner", "--no-privileges", "-d", restored, input=dump)
            query = "SELECT symbol,dataset_version,status,manifest FROM features JOIN dataset_publications ON dataset_version=dataset_id ORDER BY symbol"
            original = command("psql", "-U", "postgres", "-d", source, "-At", "-c", query)
            recovered = command("psql", "-U", "postgres", "-d", restored, "-At", "-c", query)
            assert original == recovered and recovered.count(b"complete") == 3
        finally:
            command("dropdb", "-U", "postgres", restored)
    finally:
        command("dropdb", "-U", "postgres", source)


def test_alertmanager_delivers_firing_and_resolved_webhooks():
    receipts = []
    drill_id = uuid4().hex

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if any(a["labels"].get("drill_id") == drill_id for a in payload.get("alerts", [])):
                receipts.append(payload)
            self.send_response(200)
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 18081), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    alert = {"labels": {"alertname": "PolyHorizonLocalReadinessDrill", "drill_id": drill_id},
             "startsAt": (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat(),
             "endsAt": (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
             "annotations": {"summary": "Isolated local drill, not a production incident"}}
    try:
        requests.post("http://127.0.0.1:19093/api/v2/alerts", json=[alert], timeout=5).raise_for_status()
        deadline = time.monotonic() + 30
        while not any(r["status"] == "firing" for r in receipts) and time.monotonic() < deadline:
            time.sleep(0.25)
        assert any(r["status"] == "firing" for r in receipts), "No actual receiver receipt"
        alert["endsAt"] = datetime.now(timezone.utc).isoformat()
        requests.post("http://127.0.0.1:19093/api/v2/alerts", json=[alert], timeout=5).raise_for_status()
        deadline = time.monotonic() + 30
        while not any(r["status"] == "resolved" for r in receipts) and time.monotonic() < deadline:
            time.sleep(0.25)
        assert any(r["status"] == "resolved" for r in receipts)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
