"""Historical bars have provider coverage evidence, never live tick evidence."""
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

import pandas as pd
import pandas_market_calendars as mcal
from pydantic import BaseModel, ConfigDict, Field, model_validator

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.core.session_qualification import qualify_session

HISTORICAL_POLICY = "nyse-historical-bars-v1"
QUALIFIED_POLICIES = ("nyse-full-session-v1", HISTORICAL_POLICY, "nyse-qualified-data-v1")
BAR_COLUMNS = ["symbol", "window_start", "window_end", "open", "high", "low", "close", "total_volume"]


class HistoricalSource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: Literal["finnhub", "csv"]
    adjustment: Literal["unadjusted", "split_adjusted"]
    start_date: date
    end_date: date
    retrieved_at: datetime
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def bounds(self):
        if (self.start_date > self.end_date or (self.end_date - self.start_date).days > 730
                or self.retrieved_at.tzinfo is None):
            raise ValueError("Historical request needs ordered dates and an aware retrieval time")
        return self


def bars_digest(frame):
    rows = frame[BAR_COLUMNS].copy()
    for column in ("window_start", "window_end"):
        rows[column] = pd.to_datetime(rows[column], utc=True).map(lambda t: t.isoformat())
    rows = rows.sort_values(["symbol", "window_start"])
    for column in BAR_COLUMNS[3:-1]:
        rows[column] = rows[column].astype(float)
    def integer_volume(value):
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError("Historical checksum requires finite integer volume")
        return int(number)
    rows["total_volume"] = rows.total_volume.map(integer_volume)
    payload = json.dumps(rows.to_dict("records"), sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def qualify_historical_bars(frame, source: HistoricalSource, *, now=None):
    current = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
    if current.tzinfo is None or pd.Timestamp(source.retrieved_at) > current + pd.Timedelta(minutes=5):
        raise ValueError("Historical retrieval time is ahead of the qualification clock")
    if set(BAR_COLUMNS) - set(frame.columns) or frame.empty:
        raise ValueError("Historical bars missing required columns or empty")
    if set(frame.symbol) != set(PRODUCT_SYMBOLS):
        raise ValueError("Historical bars must cover exactly the product symbols")
    if bars_digest(frame) != source.content_sha256:
        raise ValueError("Historical content checksum mismatch")
    schedule = mcal.get_calendar("NYSE").schedule(start_date=source.start_date, end_date=source.end_date)
    if schedule.empty:
        raise ValueError("Historical range contains no trading sessions")
    if schedule.iloc[-1].market_close > min(pd.Timestamp(source.retrieved_at), current):
        raise ValueError("Historical range includes an unfinished session")
    expected = set()
    sessions = []
    for _, session in schedule.iterrows():
        expected.update(pd.date_range(session.market_open, session.market_close, freq="30min", inclusive="left"))
        sessions.append(qualify_session(frame, session.market_close))
    starts = pd.to_datetime(frame.window_start, utc=True)
    if set(starts) != expected or frame.assign(window_start=starts).duplicated(["symbol", "window_start"]).any():
        raise ValueError("Historical bars include duplicate, off-session, or missing keys")
    volume = frame.total_volume
    if ((volume % 1 != 0) | (volume >= 2**63)).any():
        raise ValueError("Historical volume must fit PostgreSQL BIGINT")
    return {"policy": HISTORICAL_POLICY, "status": "passed", "sessions": sessions,
            "session_count": len(sessions), "content_sha256": source.content_sha256,
            "provider": source.provider, "adjustment": source.adjustment,
            "scope": "provider bar coverage; not live tick continuity or model qualification"}
