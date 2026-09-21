from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.utils.logger import get_logger


class FeatureParityChecker:
    def __init__(self, config: Config, training_config_path: Optional[str] = None) -> None:
        self.config = config
        self.training_config_path = training_config_path or os.getenv(
            "SERVING_TRAINING_CONFIG_PATH",
            "polyhorizon/training/configs/training_config.yaml",
        )
        self.logger = get_logger("FeatureParity")
        self._checked = False

    def check_once(self) -> None:
        if self._checked:
            return
        self._checked = True
        training_cfg = self._load_training_config()
        if not training_cfg:
            self.logger.warning("Training config not found for feature parity check")
            return

        training_features = training_cfg.get("features", {})
        training_known_cats = set(training_features.get("time_varying_known_categoricals", []))
        training_known_reals = set(training_features.get("time_varying_known_reals", []))
        training_unknown_reals = set(training_features.get("time_varying_unknown_reals", []))
        training_static = set(training_features.get("static_categoricals", []))

        serving_known_cats = set(self.config.inference.time_varying_known_categoricals)
        serving_known_reals = set(self.config.inference.time_varying_known_reals)
        serving_unknown_reals = set(self.config.inference.time_varying_unknown_reals)
        serving_static = set(self.config.inference.static_categoricals)

        mismatches: List[str] = []

        mismatches += self._diff_report("known_categoricals", training_known_cats, serving_known_cats)
        mismatches += self._diff_report("known_reals", training_known_reals, serving_known_reals)
        mismatches += self._diff_report("unknown_reals", training_unknown_reals, serving_unknown_reals)
        mismatches += self._diff_report("static_categoricals", training_static, serving_static)

        if mismatches:
            self.logger.warning("Feature parity mismatch detected: %s", "; ".join(mismatches))
        else:
            self.logger.info("Feature parity check passed: serving aligns with training config")

    def _load_training_config(self) -> Optional[Dict]:
        path = Path(self.training_config_path)
        if not path.exists():
            return None
        with open(path) as f:
            config_dict = yaml.safe_load(f) or {}

        config_dict = self._expand_env(config_dict)
        return config_dict

    def _expand_env(self, value):
        if isinstance(value, str):
            return os.path.expandvars(value)
        if isinstance(value, dict):
            return {k: self._expand_env(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._expand_env(v) for v in value]
        return value

    def _diff_report(self, label: str, training: set, serving: set) -> List[str]:
        missing = sorted(training - serving)
        extra = sorted(serving - training)
        report: List[str] = []
        if missing:
            report.append(f"{label} missing in serving: {missing}")
        if extra:
            report.append(f"{label} extra in serving: {extra}")
        return report
