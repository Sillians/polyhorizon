"""Fail CI when the ML serving deployment contract drifts."""

from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def require(text: str, pattern: str, message: str) -> None:
    if re.search(pattern, text, re.MULTILINE | re.DOTALL) is None:
        raise SystemExit(message)


def main() -> None:
    local = (ROOT / "docker-compose.yaml").read_text()
    production = (ROOT / "docker-compose.prod.yaml").read_text()
    readiness = (
        ROOT / "polyhorizon/serving/api/routers/health.py"
    ).read_text()
    activation = (
        ROOT / "polyhorizon/training/tasks/model_activation_tasks.py"
    ).read_text()
    rollout = (ROOT / "scripts/deploy/serving_rolling_update.sh").read_text()
    prod_env = (ROOT / ".env.prod.template").read_text()

    for service in ("model-serving", "frontend", "prefect-server"):
        require(
            local,
            rf"^  {service}:\n(?:(?!^  \S).)*?    build:",
            f"Local {service} must be built from the working tree",
        )

    require(
        production,
        r"^  traefik:\n",
        "Production Compose must include Traefik",
    )
    require(
        production,
        r"^  model-serving:\n"
        r"(?:(?!^  \S).)*?"
        r"image: ghcr\.io/\$\{GITHUB_REPOSITORY\}/serving:\$\{IMAGE_TAG\}",
        "Production serving must use the release image tag",
    )
    model_serving = re.search(
        r"^  model-serving:\n(?P<body>(?:(?!^  \S).)*)",
        production,
        re.MULTILINE | re.DOTALL,
    )
    if model_serving is None:
        raise SystemExit("Production Compose is missing model-serving")
    if "container_name:" in model_serving.group("body"):
        raise SystemExit("Production model-serving must remain horizontally scalable")
    if "/v1/health/ready" not in model_serving.group("body"):
        raise SystemExit("Production serving healthcheck must use /v1/health/ready")

    for marker in (
        "model_not_loaded",
        "champion_unresolvable",
        "champion_not_active",
        "get_champion_version",
    ):
        if marker not in readiness:
            raise SystemExit(f"Readiness does not enforce {marker}")

    for marker in (
        'method="POST"',
        "/v1/model/prepare",
        "/v1/model/activate",
        "/v1/model/reload",
        "socket.getaddrinfo",
        "expected_version",
        'promotion_result.get("approved")',
    ):
        if marker not in activation:
            raise SystemExit(f"Champion activation contract is missing {marker}")

    for marker in (
        "/v1/health/ready",
        "^sha-[0-9a-f]{40}$",
        "--scale",
        "expected_image",
    ):
        if marker not in rollout:
            raise SystemExit(f"Rolling deployment contract is missing {marker}")

    for setting in (
        "SERVING_MODEL_RELOAD_ENABLED=true",
        "SERVING_ACTIVATION_MODE=reload",
        "SERVING_REPLICAS=2",
        "SERVING_ROLLING_REPLICAS=3",
    ):
        if setting not in prod_env:
            raise SystemExit(f"Production environment is missing {setting}")

    print("ML deployment contract is consistent.")


if __name__ == "__main__":
    main()
