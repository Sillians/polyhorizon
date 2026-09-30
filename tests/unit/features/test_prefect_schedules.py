import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from prefect.server.schemas.schedules import CronSchedule

from polyhorizon.streaming.src.market_schedule import get_market_session


@pytest.mark.parametrize("name", ["ingestion-market-hours", "streaming-market-hours"])
def test_market_open_cron_preserves_new_york_time_across_dst(name):
    deployments = yaml.safe_load((Path(__file__).resolve().parents[3] / "prefect.yaml").read_text())["deployments"]
    deployment = next(item for item in deployments if item["name"] == name)
    schedule = CronSchedule(**deployment["schedule"])
    dates = asyncio.run(schedule.get_dates(n=2, start=datetime(2026, 10, 30, 13, tzinfo=timezone.utc)))
    assert [date.astimezone(timezone.utc).isoformat() for date in dates] == [
        "2026-10-30T13:30:00+00:00", "2026-11-02T14:30:00+00:00",
    ]


def test_market_calendar_skips_holiday_and_honors_early_close():
    assert get_market_session(datetime(2026, 11, 26), "NYSE", "America/New_York") is None
    _, close = get_market_session(datetime(2026, 11, 27), "NYSE", "America/New_York")
    assert close.hour == 13
    assert close.astimezone(timezone.utc).hour == 18


def test_feature_publication_has_no_independent_schedule():
    deployments = yaml.safe_load((Path(__file__).resolve().parents[3] / "prefect.yaml").read_text())["deployments"]
    deployment = next(item for item in deployments if item["name"] == "feature-store-after-close")
    assert deployment["schedules"] == [] and "schedule" not in deployment
