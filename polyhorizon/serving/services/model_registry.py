from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient
from polyhorizon.core.target_contract import validate_target_contract

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.utils.logger import get_logger


@dataclass
class ModelHandle:
    model: object
    model_uri: str
    model_version: str
    loaded_at: datetime
    model_flavor: str


class ModelRegistryClient:
    def __init__(self, config: Config):
        self.config = config
        self.logger = get_logger("ModelRegistry")
        mlflow.set_tracking_uri(config.mlflow.tracking_uri)
        self.client = MlflowClient()

    def _resolve_alias(self, alias: str) -> Optional[str]:
        registry = self.config.model_registry.name
        try:
            version = self.client.get_model_version_by_alias(registry, alias)
            return str(version.version)
        except Exception:
            self.logger.warning("Model alias '%s' is not assigned", alias)
            return None

    def ensure_champion_alias(self) -> str:
        """Return the champion version, bootstrapping the first assignment if needed.

        Bootstrap is deliberately one-way: an existing champion is never moved. When
        a registry already contains versions but has no champion, only a READY
        version with governance qualification may become the initial champion.
        Later promotions remain the responsibility of the governance flow.
        """
        registry = self.config.model_registry.name
        alias = self.config.model_registry.champion_alias
        current = self._resolve_alias(alias)
        if current is not None:
            return current
        if not self.config.model_registry.bootstrap_champion_alias:
            raise RuntimeError(f"Champion alias '{alias}' not found for model '{registry}'")

        versions = list(self.client.search_model_versions(f"name='{registry}'"))
        ready = [
            version
            for version in versions
            if str(getattr(version, "status", "READY")).upper() == "READY"
            and (getattr(version, "tags", {}) or {}).get("governance_qualification") == "passed-v1"
        ]
        if not ready:
            raise RuntimeError(
                f"Cannot bootstrap champion alias '{alias}': model '{registry}' has no governance-qualified READY versions"
            )

        selected = max(
            ready,
            key=lambda item: (
                int(item.version) if str(item.version).isdigit() else -1,
                int(getattr(item, "creation_timestamp", 0) or 0),
            ),
        )
        version = str(selected.version)
        self.client.set_registered_model_alias(name=registry, alias=alias, version=version)
        self.client.set_model_version_tag(
            name=registry,
            version=version,
            key="champion_bootstrapped",
            value="true",
        )
        self.logger.info("Bootstrapped model %s version %s as @%s", registry, version, alias)
        return version

    def get_champion_version(self) -> str:
        """Resolve the governed champion without mutating the registry."""
        registry = self.config.model_registry.name
        alias = self.config.model_registry.champion_alias
        version = self._resolve_alias(alias)
        if version is None:
            raise RuntimeError(f"Champion alias '{alias}' not found for model '{registry}'")
        return version

    def load_champion(self) -> ModelHandle:
        registry = self.config.model_registry.name
        version = self.ensure_champion_alias()

        # Pin the load to the resolved version so an alias move during startup
        # cannot produce mismatched model/version metadata.
        model_uri = f"models:/{registry}/{version}"
        self.logger.info("Loading champion model from %s", model_uri)
        model_flavor = "pytorch"
        try:
            model = mlflow.pytorch.load_model(model_uri)
        except Exception:
            self.logger.warning("Falling back to pyfunc model load", exc_info=True)
            model = mlflow.pyfunc.load_model(model_uri)
            model_flavor = "pyfunc"
        # Fail startup/reload before publishing a handle. Never infer semantics
        # from mutable registry tags or silently accept legacy artifacts.
        validate_target_contract(model, self.config)
        try:
            from polyhorizon.serving.services import metrics
            metrics.record_model_load()
        except Exception:
            pass

        return ModelHandle(
            model=model,
            model_uri=model_uri,
            model_version=version,
            loaded_at=datetime.now(timezone.utc),
            model_flavor=model_flavor,
        )

    def get_champion_metadata(self) -> dict:
        registry = self.config.model_registry.name
        alias = self.config.model_registry.champion_alias
        version = self.get_champion_version()

        mv = self.client.get_model_version(registry, version)
        tags = dict(getattr(mv, "tags", {}) or {})

        def _ts_to_iso(value: Optional[int]) -> Optional[str]:
            if value is None:
                return None
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).isoformat()

        return {
            "name": registry,
            "alias": alias,
            "version": str(version),
            "model_uri": f"models:/{registry}/{version}",
            "model_flavor": "pytorch",
            "run_id": getattr(mv, "run_id", None),
            "status": getattr(mv, "status", None),
            "status_message": getattr(mv, "status_message", None),
            "source": getattr(mv, "source", None),
            "description": getattr(mv, "description", None),
            "creation_time": _ts_to_iso(getattr(mv, "creation_timestamp", None)),
            "last_updated_time": _ts_to_iso(getattr(mv, "last_updated_timestamp", None)),
            "tags": tags,
        }
