from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict


class ModelMetadataResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    alias: str
    version: str
    model_uri: str
    model_flavor: str
    run_id: Optional[str]
    status: Optional[str]
    status_message: Optional[str]
    source: Optional[str]
    description: Optional[str]
    creation_time: Optional[str]
    last_updated_time: Optional[str]
    tags: Dict[str, str]


class ModelReloadResponse(BaseModel):
    model_version: str
    model_uri: str
    loaded_at: str
