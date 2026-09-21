from __future__ import annotations

import json
from typing import Any, Optional
import redis

from polyhorizon.serving.configs.settings import RedisConfig
from polyhorizon.serving.utils.logger import get_logger


class RedisCache:
    def __init__(self, config: RedisConfig):
        self.config = config
        self.logger = get_logger("RedisCache")
        self.client: Optional[redis.Redis] = None

        if self.config.enabled:
            self.client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
            )
            try:
                self.client.ping()
            except Exception:
                self.logger.warning("Redis cache unavailable; disabling cache", exc_info=True)
                self.client = None

    def _key(self, key: str) -> str:
        return f"{self.config.key_prefix}:{key}"

    def get(self, key: str) -> Optional[Any]:
        if not self.client:
            return None
        try:
            payload = self.client.get(self._key(key))
            return json.loads(payload) if payload else None
        except Exception:
            self.logger.warning("Failed to read cache key %s", key, exc_info=True)
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        if not self.client:
            return
        try:
            ttl = ttl_seconds or self.config.ttl_seconds
            self.client.set(self._key(key), json.dumps(value), ex=ttl)
        except Exception:
            self.logger.warning("Failed to set cache key %s", key, exc_info=True)

    def close(self) -> None:
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
