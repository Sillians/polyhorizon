"""Build, manifest and deployment must agree on lowercase registry paths."""

import json
import os
from pathlib import Path
import shutil
import subprocess

import yaml


ROOT = Path(__file__).resolve().parents[2]


def test_mixed_case_github_repository_produces_consistent_image_names(tmp_path):
    sha = "a" * 40
    output = tmp_path / "outputs"
    capture = tmp_path / "inspected"
    env = os.environ | {
        "GITHUB_REPOSITORY": "Sillians/PolyHorizon",
        "GITHUB_SHA": sha, "RELEASE_TAG": f"sha-{sha}", "IMAGE_TAG": f"sha-{sha}",
        "GITHUB_OUTPUT": str(output), "CAPTURE": str(capture),
    }
    bash = shutil.which("bash")
    assert bash
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci-cd.yml").read_text())
    steps = workflow["jobs"]["build-images"]["steps"]
    normalization = next(s for s in steps if s.get("id") == "image-repository")
    subprocess.run([bash, "-eu", "-c", normalization["run"]], env=env, check=True)
    assert output.read_text().strip() == "repository=sillians/polyhorizon"
    build = next(s for s in steps if s.get("name") == "Build and publish immutable image")
    assert "steps.image-repository.outputs.repository" in build["with"]["tags"]

    release = tmp_path / "release"
    subprocess.run([bash, "scripts/ci/create_release_manifest.sh", str(release)],
                   cwd=ROOT, env=env, check=True)
    manifest = json.loads((release / "release.json").read_text())
    images = manifest["images"]
    assert len(images) == 7
    assert all(image.startswith("ghcr.io/sillians/polyhorizon/") for image in images)
    assert all(image.endswith(f":sha-{sha}") for image in images)
    assert "GITHUB_REPOSITORY=sillians/polyhorizon\n" in (release / ".env.prod.template").read_text()

    deployment = yaml.safe_load((ROOT / ".github/workflows/deploy-prod.yml").read_text())
    inspect = next(s for s in deployment["jobs"]["validate-release"]["steps"]
                   if s.get("name") == "Verify every release image is available")
    # Record the requested image refs without contacting Docker or a registry.
    stub = 'docker() { printf "%s\\n" "$4" >> "$CAPTURE"; };\n'
    subprocess.run([bash, "-eu", "-c", stub + inspect["run"]], env=env, check=True)
    assert capture.read_text().splitlines() == images
