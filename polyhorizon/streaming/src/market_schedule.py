from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def get_market_session(
    now: datetime,
    calendar_name: str,
    timezone: str,
) -> tuple[datetime, datetime] | None:
    try:
        import pandas_market_calendars as mcal
    except ImportError as exc:
        raise RuntimeError("pandas_market_calendars is required for market schedule checks") from exc

    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        return None

    tz = ZoneInfo(timezone)
    market_open = schedule.iloc[0]["market_open"].tz_convert(tz)
    market_close = schedule.iloc[0]["market_close"].tz_convert(tz)
    return market_open, market_close


def is_market_open(now: datetime, calendar_name: str, timezone: str) -> bool:
    session = get_market_session(now, calendar_name, timezone)
    if session is None:
        return False
    market_open, market_close = session
    return market_open <= now <= market_close
