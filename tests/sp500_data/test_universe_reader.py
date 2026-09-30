from __future__ import annotations

import hashlib
import io
import json
from datetime import datetime, timezone

import pytest

from polyhorizon.sp500_data.reader import load_governed_universe


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, *, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}


def _objects(csv_payload: bytes) -> dict[str, bytes]:
    pointer = {
        "schema_version": "1.0",
        "run_id": "run-123",
        "fetched_at": "2026-07-26T06:00:00+00:00",
        "constituent_count": 2,
        "symbols": ["AAA", "BBB"],
        "artifacts": {
            "companies.csv": {
                "key": "universe/snapshots/run-123/companies.csv",
                "sha256": hashlib.sha256(csv_payload).hexdigest(),
                "bytes": len(csv_payload),
            }
        },
    }
    return {
        "universe/current.json": json.dumps(pointer).encode(),
        "universe/snapshots/run-123/companies.csv": csv_payload,
    }


def test_reader_verifies_and_returns_governed_snapshot():
    payload = b"company_name,symbol\nCompany A,AAA\nCompany B,BBB\n"
    snapshot = load_governed_universe(
        FakeS3(_objects(payload)),
        "universe",
        now=lambda: datetime(2026, 7, 26, 7, 0, tzinfo=timezone.utc),
    )

    assert snapshot.run_id == "run-123"
    assert snapshot.symbols == ["AAA", "BBB"]


def test_reader_rejects_tampered_artifact():
    expected = b"company_name,symbol\nCompany A,AAA\nCompany B,BBB\n"
    objects = _objects(expected)
    objects["universe/snapshots/run-123/companies.csv"] = b"tampered"

    with pytest.raises(ValueError, match="size|checksum"):
        load_governed_universe(
            FakeS3(objects),
            "universe",
            now=lambda: datetime(2026, 7, 26, 7, 0, tzinfo=timezone.utc),
        )


def test_reader_rejects_stale_snapshot():
    payload = b"company_name,symbol\nCompany A,AAA\nCompany B,BBB\n"

    with pytest.raises(ValueError, match="stale"):
        load_governed_universe(
            FakeS3(_objects(payload)),
            "universe",
            maximum_age_hours=24,
            now=lambda: datetime(2026, 7, 28, 7, 0, tzinfo=timezone.utc),
        )
