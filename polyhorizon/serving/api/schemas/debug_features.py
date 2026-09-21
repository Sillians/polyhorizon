from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ConfigDict


class DebugFeaturesResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str
    raw_rows: int
    derived_rows: int
    raw_columns: List[str]
    derived_columns: List[str]
    raw: List[Dict[str, Any]]
    derived: List[Dict[str, Any]]
    features_timestamp: Optional[str] = None
    truncated: bool = Field(default=False, description="True if data was truncated by limit")
