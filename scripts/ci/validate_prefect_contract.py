"""Validate production invariants for Prefect deployments and workers."""

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_DEPLOYMENTS = {
    "sp500-universe-weekdays",
    "ingestion-market-hours",
    "streaming-market-hours",
    "streaming-health-hourly",
    "feature-store-after-close",
    "feature-store-quality-nightly",
    "training-quarterly",
    "retraining-drift",
}


def main() -> None:
    config = yaml.safe_load((ROOT / "prefect.yaml").read_text())
    deployments = config.get("deployments", [])
    names = {deployment.get("name") for deployment in deployments}
    if names != EXPECTED_DEPLOYMENTS:
        raise SystemExit(
            f"Prefect deployment set mismatch: expected={sorted(EXPECTED_DEPLOYMENTS)} "
            f"actual={sorted(names)}"
        )

    for deployment in deployments:
        name = deployment["name"]
        concurrency = deployment.get("concurrency_limit", {})
        if concurrency.get("limit") != 1:
            raise SystemExit(f"{name} must prevent overlapping runs")
        if concurrency.get("collision_strategy") not in {"ENQUEUE", "CANCEL_NEW"}:
            raise SystemExit(f"{name} has no valid collision strategy")
        if not deployment.get("version") or not deployment.get("tags"):
            raise SystemExit(f"{name} must have an auditable version and tags")

        variables = deployment.get("work_pool", {}).get("job_variables", {})
        volumes = variables.get("volumes", [])
        if "{{ $PREFECT_JOB_ENV_FILE }}:/app/.env:ro" not in volumes:
            raise SystemExit(f"{name} does not mount the runtime environment read-only")
        if variables.get("image_pull_policy") != "Always":
            raise SystemExit(f"{name} does not enforce immutable image refresh")
        if ":latest" in variables.get("image", ""):
            raise SystemExit(f"{name} uses a mutable image tag")

    compose = (ROOT / "docker-compose.prod.yaml").read_text()
    if compose.count("/var/run/docker.sock:/var/run/docker.sock") < 4:
        raise SystemExit("Every production Docker worker must have Docker socket access")
    if "prefect-docker>=0.6,<0.7" not in (ROOT / "docker/app/Dockerfile").read_text():
        raise SystemExit("The orchestration image must install prefect-docker")


if __name__ == "__main__":
    main()
