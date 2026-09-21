from __future__ import annotations

import json
import os
import socket
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

from prefect import task


@task(name="Activate Promoted Champion", retries=3, retry_delay_seconds=[10, 30, 60])
def activate_champion_task(
    promotion_result: dict,
    model_metadata: dict,
) -> dict:
    """Reload the promoted champion on every serving replica and verify it."""
    if not promotion_result.get("promoted"):
        return {"status": "skipped", "reason": "not_promoted"}

    mode = os.getenv("SERVING_ACTIVATION_MODE", "none").lower()
    if mode == "none":
        return {"status": "deferred", "reason": "activation_disabled"}
    if mode != "reload":
        raise ValueError(f"Unsupported SERVING_ACTIVATION_MODE: {mode}")

    expected_version = str(model_metadata["model_version"])
    base_url = os.getenv(
        "SERVING_RELOAD_URL", "http://model-serving:8000/v1/model/reload"
    )
    api_key = os.getenv("SERVING_RELOAD_API_KEY")
    if not api_key:
        raise RuntimeError("SERVING_RELOAD_API_KEY is required for champion activation")
    api_key_header = os.getenv("SERVING_API_KEY_HEADER", "X-API-Key")

    parts = urlsplit(base_url)
    if not parts.hostname:
        raise ValueError(f"SERVING_RELOAD_URL has no hostname: {base_url}")
    port = parts.port or (443 if parts.scheme == "https" else 80)
    addresses = sorted(
        {
            result[4][0]
            for result in socket.getaddrinfo(
                parts.hostname,
                port,
                type=socket.SOCK_STREAM,
            )
        }
    )
    if not addresses:
        raise RuntimeError(f"No serving replicas resolved for {parts.hostname}")

    activated = []
    for address in addresses:
        host = f"[{address}]" if ":" in address else address
        replica_url = urlunsplit(
            (parts.scheme, f"{host}:{port}", parts.path, parts.query, parts.fragment)
        )
        request = Request(
            replica_url,
            data=b"",
            method="POST",
            headers={api_key_header: api_key, "Content-Type": "application/json"},
        )
        with urlopen(request, timeout=120) as response:
            payload = json.load(response)
        loaded_version = str(payload.get("model_version"))
        if loaded_version != expected_version:
            raise RuntimeError(
                f"Replica {address} loaded model {loaded_version}, expected {expected_version}"
            )
        activated.append({"address": address, "model_version": loaded_version})

    return {
        "status": "activated",
        "model_version": expected_version,
        "replicas": activated,
    }
