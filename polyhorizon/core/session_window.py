"""Calendar window shared by scheduled collection jobs."""
from datetime import timedelta
from zoneinfo import ZoneInfo
from polyhorizon.streaming.src.market_schedule import get_market_session


def collection_session(now, scheduled_at=None):
    now = now.astimezone(ZoneInfo("America/New_York"))
    if scheduled_at is not None and scheduled_at.astimezone(now.tzinfo).date() != now.date():
        return None  # Never replay yesterday's queued market run today.
    session = get_market_session(now, "NYSE", "America/New_York")
    if session is None:
        return None
    opening, close = session
    if now < opening - timedelta(minutes=10) or now >= close:
        return None
    return opening, close


def scheduled_start():
    from prefect.context import FlowRunContext
    context = FlowRunContext.get()
    return context.flow_run.expected_start_time if context else None
