"""Offline smoke check using the same interpreter as the Prefect image job.

Load the entrypoint from prefect.yaml and exercise CLI argument parsing. Never
invoke the publication flow: this check needs no credentials or infrastructure.
"""

import importlib
from pathlib import Path
import subprocess
import sys

import yaml
from prefect import Flow


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    deployment_config = yaml.safe_load((root / "prefect.yaml").read_text())
    deployment = next(
        item for item in deployment_config["deployments"]
        if item["name"] == "sp500-universe-weekdays"
    )
    module_path, function_name = deployment["entrypoint"].split(":")
    module_name = module_path.removesuffix(".py").replace("/", ".")
    flow = getattr(importlib.import_module(module_name), function_name)
    if not isinstance(flow, Flow):
        raise RuntimeError(f"{deployment['entrypoint']} is not a Prefect flow")

    result = subprocess.run(
        [sys.executable, "-m", "polyhorizon.sp500_data", "--help"],
        cwd=root, check=True, capture_output=True, text=True, timeout=30,
    )
    if "--config" not in result.stdout:
        raise RuntimeError("Universe CLI did not expose its configuration argument")
    print(f"Universe entrypoint OK: {deployment['entrypoint']} ({sys.executable})")
    print("Universe CLI --help OK; no publication performed.")


if __name__ == "__main__":
    main()
