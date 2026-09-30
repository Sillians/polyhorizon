from __future__ import annotations

import json
import os
import socket
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, urlopen

from prefect import task


def _replica_urls(base_url: str) -> list[str]:
    parts = urlsplit(base_url)
    if parts.scheme != "http" or not parts.hostname:
        raise ValueError("SERVING_RELOAD_URL must be an internal HTTP service URL")
    port = parts.port or 80
    addresses = sorted({entry[4][0] for entry in socket.getaddrinfo(
        parts.hostname, port, type=socket.SOCK_STREAM
    )})
    if not addresses:
        raise RuntimeError("No serving replicas resolved")
    return [urlunsplit((parts.scheme, f"[{address}]:{port}" if ":" in address else
                      f"{address}:{port}", "", "", "")) for address in addresses]


def _post(url: str, version: str, header: str, key: str) -> dict:
    request = Request(url, data=json.dumps({"model_version": version}).encode(),
                      method="POST", headers={header: key, "Content-Type": "application/json"})
    with urlopen(request, timeout=120) as response:
        payload = json.load(response)
    if str(payload.get("model_version")) != version:
        raise RuntimeError(f"Replica {url} did not load model version {version}")
    return payload


@task(name="Activate Approved Champion", retries=3, retry_delay_seconds=[10, 30, 60])
def activate_champion_task(
    promotion_result: dict,
    model_metadata: dict,
    config=None,
) -> dict:
    """Prepare every replica, commit the alias, activate, and verify."""
    if not promotion_result.get("approved"):
        return {"status": "skipped", "reason": "not_approved"}

    mode = os.getenv("SERVING_ACTIVATION_MODE", "none").lower()
    if mode == "none":
        return {"status": "deferred", "reason": "activation_disabled"}
    if mode != "reload":
        raise ValueError(f"Unsupported SERVING_ACTIVATION_MODE: {mode}")

    if config is None:
        raise ValueError("Registry config is required for champion activation")
    expected_version = str(model_metadata["model_version"])
    base_url = os.getenv(
        "SERVING_RELOAD_URL", "http://model-serving:8000/v1/model/reload"
    )
    api_key = os.getenv("SERVING_RELOAD_API_KEY")
    if not api_key:
        raise RuntimeError("SERVING_RELOAD_API_KEY is required for champion activation")
    api_key_header = os.getenv("SERVING_API_KEY_HEADER", "X-API-Key")

    urls = _replica_urls(base_url)
    from mlflow.tracking import MlflowClient

    client = MlflowClient(tracking_uri=config.mlflow.tracking_uri)
    name = config.model_registry.name
    alias = config.model_registry.champion_alias
    candidate = client.get_model_version(name, expected_version)
    if (candidate.tags or {}).get("session_qualification") != "nyse-full-session-v1":
        raise ValueError("Candidate lacks full-session-qualified training lineage")
    if (candidate.tags or {}).get("governance_qualification") != "passed-v1":
        raise ValueError("Candidate has not passed governance qualification")
    try:
        previous = str(client.get_model_version_by_alias(name, alias).version)
    except Exception:
        previous = None

    for url in urls:
        _post(f"{url}/v1/model/prepare", expected_version, api_key_header, api_key)
    if previous is not None and str(client.get_model_version_by_alias(name, alias).version) != previous:
        raise RuntimeError("Champion changed during candidate preparation")
    if previous != expected_version:
        client.set_registered_model_alias(name=name, alias=alias, version=expected_version)
    try:
        for url in urls:
            _post(f"{url}/v1/model/activate", expected_version, api_key_header, api_key)
        for url in urls:
            with urlopen(f"{url}/v1/health/ready", timeout=10) as response:
                readiness = json.load(response)
            if readiness.get("model_version") != expected_version or readiness.get("transitioning"):
                raise RuntimeError(f"Replica {url} is not ready with champion {expected_version}")
    except Exception:
        if previous != expected_version:
            if previous is None:
                client.delete_registered_model_alias(name=name, alias=alias)
            else:
                client.set_registered_model_alias(name=name, alias=alias, version=previous)
        if previous is not None and previous != expected_version:
            for url in urls:
                try:
                    request = Request(f"{url}/v1/model/reload", data=b"", method="POST",
                                      headers={api_key_header: api_key})
                    with urlopen(request, timeout=120):
                        pass
                except Exception:
                    pass
        raise
    return {"status": "activated", "model_version": expected_version,
            "replicas": [{"address": url} for url in urls]}
