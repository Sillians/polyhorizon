from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


@dataclass(frozen=True)
class UniverseSnapshot:
    run_id: str
    fetched_at: datetime
    symbols: list[str]
    manifest: dict


def load_governed_universe(
    s3_client,
    bucket: str,
    *,
    current_key: str = "universe/current.json",
    maximum_age_hours: int = 30,
    now: Callable[[], datetime] | None = None,
) -> UniverseSnapshot:
    """Resolve and verify the immutable artifact referenced by the current pointer."""
    clock = now or (lambda: datetime.now(timezone.utc))
    pointer_response = s3_client.get_object(Bucket=bucket, Key=current_key)
    pointer = json.loads(pointer_response["Body"].read())

    if pointer.get("schema_version") != "1.0":
        raise ValueError(f"Unsupported universe schema: {pointer.get('schema_version')}")
    fetched_at = datetime.fromisoformat(pointer["fetched_at"]).astimezone(timezone.utc)
    age_hours = (clock().astimezone(timezone.utc) - fetched_at).total_seconds() / 3600
    if age_hours < 0:
        raise ValueError("Universe snapshot timestamp is in the future")
    if age_hours > maximum_age_hours:
        raise ValueError(
            f"Universe snapshot is stale: {age_hours:.1f}h old, "
            f"maximum is {maximum_age_hours}h"
        )

    artifact = pointer["artifacts"]["companies.csv"]
    response = s3_client.get_object(Bucket=bucket, Key=artifact["key"])
    payload = response["Body"].read()
    if len(payload) != artifact["bytes"]:
        raise ValueError("Universe artifact size does not match its manifest")
    actual_hash = hashlib.sha256(payload).hexdigest()
    if actual_hash != artifact["sha256"]:
        raise ValueError("Universe artifact checksum does not match its manifest")

    reader = csv.DictReader(io.StringIO(payload.decode("utf-8")))
    symbols = []
    for row in reader:
        symbol = next(
            (value for column, value in row.items() if column.casefold() == "symbol"),
            None,
        )
        if symbol and symbol.strip():
            symbols.append(symbol.strip().upper())

    if len(symbols) != pointer["constituent_count"]:
        raise ValueError("Universe artifact count does not match its manifest")
    if symbols != pointer["symbols"]:
        raise ValueError("Universe artifact symbols do not match its manifest")
    if len(symbols) != len(set(symbols)):
        raise ValueError("Universe artifact contains duplicate symbols")

    return UniverseSnapshot(
        run_id=pointer["run_id"],
        fetched_at=fetched_at,
        symbols=symbols,
        manifest=pointer,
    )
