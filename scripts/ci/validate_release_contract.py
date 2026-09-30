"""Validate that CI, Compose, and Prefect agree on release-owned images."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RELEASE_IMAGES = {
    "ingestion",
    "spark-streaming",
    "features",
    "training",
    "serving",
    "frontend",
    "orchestration",
}
IMMUTABLE_REF = "ghcr.io/${{GITHUB_REPOSITORY}}/{name}:${{IMAGE_TAG}}"


def main() -> None:
    workflow = (ROOT / ".github/workflows/ci-cd.yml").read_text()
    compose = (ROOT / "docker-compose.prod.yaml").read_text()
    prefect = (ROOT / "prefect.yaml").read_text()

    matrix_images = set(re.findall(r"^\s+- name: ([a-z0-9-]+)$", workflow, re.MULTILINE))
    missing_builds = RELEASE_IMAGES - matrix_images
    if missing_builds:
        raise SystemExit(f"Release matrix is missing images: {sorted(missing_builds)}")

    for name in {"spark-streaming", "serving", "frontend"}:
        expected = IMMUTABLE_REF.format(name=name)
        if expected not in compose:
            raise SystemExit(f"Production Compose does not use {expected}")

    for name in {"ingestion", "spark-streaming", "features", "training"}:
        expected = IMMUTABLE_REF.format(name=name)
        if expected not in prefect:
            raise SystemExit(f"Prefect does not use {expected}")

    deploy_script = (ROOT / "scripts/deploy/deploy_prefect_flows.sh").read_text()
    if IMMUTABLE_REF.format(name="orchestration") not in deploy_script:
        raise SystemExit("Prefect deployment must use the immutable orchestration image")

    if re.search(r"ghcr\.io/[^\s]+:latest", compose + prefect):
        raise SystemExit("Release-owned production images must not use :latest")

    print("Release image contract is consistent.")


if __name__ == "__main__":
    main()
