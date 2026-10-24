"""Isolated HTTP rehearsal with a genuinely trained tiny model and synthetic bars.

NOT real-market qualification: fixture timestamps deliberately bypass freshness.
Uses the actual serving routes, artifact loading, and forecast computation.
"""
import runpy
import tempfile
from pathlib import Path

import pytest
import uvicorn


def main():
    root = Path(__file__).resolve().parents[2]
    builder = runpy.run_path(str(root / "tests/integration/forecast/test_forecast_release_gate.py"))["build_trained_fixture"]
    with tempfile.TemporaryDirectory(prefix="polyhorizon-browser-") as work, pytest.MonkeyPatch.context() as patch:
        app, _ = builder(Path(work), patch)
        from polyhorizon.serving.services.security import SecurityManager
        app.state.config.security.operator_api_keys = ["fixture-operator"]
        app.state.config.security.require_api_key = True
        app.state.config.security.api_keys = ["fixture-client"]
        app.state.config.connection_parameters.port = 1
        patch.setenv("SERVING_ALERTMANAGER_URL", "http://127.0.0.1:1")
        app.state.security = SecurityManager(app.state.config)
        # Bind loopback ONLY; never expose this fixture as a deployment.
        uvicorn.run(app, host="127.0.0.1", port=38180)


if __name__ == "__main__":
    main()
