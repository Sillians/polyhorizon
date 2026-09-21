from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


class FeaturesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    rows: int
    columns: List[str]
    data: List[Dict[str, Any]]
    features_timestamp: Optional[str] = None
    truncated: bool = Field(default=False, description="True if data was truncated by limit")
