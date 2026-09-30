from datetime import datetime, timezone
import math

import pytest

from scripts.ops.recover_kafka import validate_trade, coverage
from scripts.deploy.register_local import specifications


def test_valid_millisecond_trade():
    row = validate_trade({"symbol": "NVDA", "price": 10, "volume": 1, "timestamp": 1790343153621})
    assert row["event_time"].microsecond == 621000


@pytest.mark.parametrize("field,value", [("symbol", "BAD"), ("price", 0), ("volume", -1),
                                         ("price", math.nan), ("timestamp", 1790343153),
                                         ("timestamp", True)])
def test_rejects_bad_records(field, value):
    row = {"symbol": "NVDA", "price": 10, "volume": 1, "timestamp": 1790343153621}
    row[field] = value
    with pytest.raises(ValueError):
        validate_trade(row)


def test_incomplete_session_is_explicit():
    report = coverage([{"symbol": "NVDA", "event_time": datetime(2026, 9, 25, 13, 32, tzinfo=timezone.utc)}])
    assert len(report["NVDA"]["sessions"][0]["missing_bars"]) == 12
    assert report["MSFT"]["rows"] == 0


def test_local_jobs_use_internal_endpoints_and_real_network():
    specs = specifications("/repo/.env", "polyhorizon_ml-platform")
    assert len(specs) == 5
    for d in specs:
        job = d["job_variables"]
        assert job["image_pull_policy"] == "Never"
        assert job["networks"] == ["polyhorizon_ml-platform"]
        assert job["env"]["POSTGRES_HOST"] == "postgres"
        assert "AWS_SECRET_ACCESS_KEY" not in job["env"]
    assert next(s for s in specs if s["name"] == "feature-store-after-close")["cron"] is None


def test_publication_deadline_handles_weekend_and_early_close():
    from polyhorizon.ingestion.flow.publication_watchdog import expected_close
    assert expected_close(datetime(2026, 9, 26, 12, tzinfo=timezone.utc)).isoformat() == "2026-09-25T20:00:00+00:00"
    assert expected_close(datetime(2026, 11, 27, 19, tzinfo=timezone.utc)).isoformat() == "2026-11-27T18:00:00+00:00"
