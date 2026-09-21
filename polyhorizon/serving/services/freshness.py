"""Post-close product contract; feature timestamps label the end of a bar."""

import pandas as pd
import pandas_market_calendars as mcal


class StaleFeaturesError(RuntimeError):
    pass


def require_post_close_features(timestamp, publication_delay_minutes=60, *, now=None):
    now = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    now = now.tz_convert("UTC")
    try:
        cutoff = pd.Timestamp(timestamp)
        if pd.isna(cutoff) or cutoff.tzinfo is None:
            raise ValueError("Missing timezone")
        cutoff = cutoff.tz_convert("UTC")
    except (ValueError, TypeError) as exc:
        raise StaleFeaturesError("Feature cutoff is missing or invalid") from exc
    schedule = mcal.get_calendar("NYSE").schedule(
        start_date=(now - pd.Timedelta(days=32)).date(), end_date=now.date()
    )
    closes = schedule.market_close
    due = closes[closes + pd.Timedelta(minutes=publication_delay_minutes) <= now]
    if due.empty or not closes.eq(cutoff).any() or cutoff > now or cutoff < due.iloc[-1]:
        raise StaleFeaturesError(
            "Post-close features unavailable: require a completed NYSE session published "
            f"within {publication_delay_minutes} minutes of close"
        )
