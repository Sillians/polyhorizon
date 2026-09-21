from __future__ import annotations


from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str
    service: str
    version: str
    model_version: str
    model_loaded_at: str
    timestamp: str
