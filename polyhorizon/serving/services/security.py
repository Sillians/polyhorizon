from __future__ import annotations

import time
import secrets
from dataclasses import dataclass
from typing import Optional

from fastapi import Request

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.utils.logger import get_logger


@dataclass
class SecurityDecision:
    allowed: bool
    status_code: int = 200
    detail: Optional[str] = None


class SecurityManager:
    def __init__(self, config: Config, redis_client=None) -> None:
        self.config = config
        self.redis = redis_client
        self.logger = get_logger("SecurityManager")
        self._warned_no_redis = False

    def authorize(self, request: Request) -> SecurityDecision:
        path = request.url.path.rstrip("/")
        prefix = self.config.api.version_prefix.rstrip("/")
        operator_path = path in {f"{prefix}/ops/session", f"{prefix}/features/debug", f"{prefix}/model/reload"}
        key = request.headers.get(self.config.security.api_key_header, "")
        operator = bool(key) and any(
            secrets.compare_digest(key, candidate)
            for candidate in self.config.security.operator_api_keys
        )
        # Privileged checks precede exemptions and remain enforced with public inference.
        if operator_path and not operator:
            return SecurityDecision(allowed=False, status_code=403, detail="Operator credential required")
        if path in self.config.security.exempt_paths:
            return SecurityDecision(allowed=True)

        if self.config.security.require_api_key:
            header_name = self.config.security.api_key_header
            key = request.headers.get(header_name)
            if not operator and (not key or key not in self.config.security.api_keys):
                return SecurityDecision(allowed=False, status_code=401, detail="Unauthorized")

        if self.config.security.rate_limit_enabled:
            if self.redis is None:
                if not self._warned_no_redis:
                    self.logger.warning("Rate limit enabled but Redis unavailable; skipping rate limit")
                    self._warned_no_redis = True
                return SecurityDecision(allowed=True)

            allowed = self._allow_rate_limit(request)
            if not allowed:
                return SecurityDecision(allowed=False, status_code=429, detail="Rate limit exceeded")

        return SecurityDecision(allowed=True)

    def _allow_rate_limit(self, request: Request) -> bool:
        limit = self.config.security.rate_limit_per_minute
        prefix = self.config.security.rate_limit_prefix
        identity = self._get_client_identity(request)
        window = int(time.time() // 60)
        key = f"{prefix}:{identity}:{window}"

        try:
            count = self.redis.incr(key)
            if count == 1:
                self.redis.expire(key, 65)
            return count <= limit
        except Exception:
            self.logger.warning("Rate limit check failed; allowing request", exc_info=True)
            return True

    def _get_client_identity(self, request: Request) -> str:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"
