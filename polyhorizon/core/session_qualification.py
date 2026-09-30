"""Fail-closed NYSE session coverage checks (not proof of lossless ingestion)."""
import pandas as pd
import pandas_market_calendars as mcal
import numpy as np

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def qualify_trades(trades, market_close):
    close = pd.Timestamp(market_close).tz_convert("UTC")
    day = close.tz_convert("America/New_York").date()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=day, end_date=day)
    if schedule.empty or schedule.iloc[0].market_close != close:
        raise ValueError("Close does not match an NYSE session")
    opening = schedule.iloc[0].market_open
    times = pd.to_datetime(trades.event_time, utc=True)
    rows = trades.loc[(times >= opening) & (times < close)].copy()
    rows["event_time"] = pd.to_datetime(rows.event_time, utc=True)
    if set(rows.symbol) != set(PRODUCT_SYMBOLS):
        raise ValueError("Trade coverage requires every product symbol")
    if not np.isfinite(rows[["price", "volume"]].to_numpy(dtype=float)).all() or (rows[["price", "volume"]] <= 0).any().any():
        raise ValueError("Invalid session trade price or volume")
    evidence = {}
    for symbol in PRODUCT_SYMBOLS:
        events = rows.loc[rows.symbol == symbol, "event_time"].sort_values()
        gaps = [float((events.iloc[0] - opening).total_seconds()),
                float((close - events.iloc[-1]).total_seconds()),
                float(events.diff().dt.total_seconds().fillna(0).max())]
        if max(gaps) > 120:
            raise ValueError(f"Trade coverage gap exceeds 120 seconds for {symbol}")
        evidence[symbol] = {"trade_count": len(events), "maximum_gap_seconds": max(gaps)}
    return evidence


def qualify_session(gold, market_close):
    close = pd.Timestamp(market_close)
    if close.tzinfo is None:
        raise ValueError("Session close must be timezone-aware")
    close = close.tz_convert("UTC")
    day = close.tz_convert("America/New_York").date()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=day, end_date=day)
    if schedule.empty or schedule.iloc[0].market_close != close:
        raise ValueError("Close does not match an NYSE session")
    opening = schedule.iloc[0].market_open
    expected = pd.date_range(opening, close, freq="30min", inclusive="left")
    starts = pd.to_datetime(gold.window_start, utc=True)
    rows = gold.loc[(starts >= opening) & (starts < close)].copy()
    rows["window_start"] = pd.to_datetime(rows.window_start, utc=True)
    rows["window_end"] = pd.to_datetime(rows.window_end, utc=True)
    if set(rows.symbol) != set(PRODUCT_SYMBOLS):
        raise ValueError("Session must cover exactly the product symbols")
    numeric = ["open", "high", "low", "close", "total_volume"]
    if not np.isfinite(rows[numeric].to_numpy(dtype=float)).all() or (rows[numeric] <= 0).any().any():
        raise ValueError("Session requires finite positive prices and volumes")
    if ((rows.high < rows[["open", "close", "low"]].max(axis=1)) |
            (rows.low > rows[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("Invalid session OHLC bounds")
    if not (rows.window_end == rows.window_start + pd.Timedelta(minutes=30)).all():
        raise ValueError("Session bars must be exactly 30 minutes")
    counts = {}
    for symbol in PRODUCT_SYMBOLS:
        observed = rows.loc[rows.symbol == symbol, "window_start"]
        if observed.duplicated().any() or set(observed) != set(expected):
            raise ValueError(f"Incomplete session coverage for {symbol}: expected {len(expected)} distinct bars")
        counts[symbol] = len(observed)
    return {"policy": "nyse-full-session-v1", "status": "passed",
            "session_date": str(day), "market_open": opening.isoformat(),
            "market_close": close.isoformat(), "bars_per_symbol": counts,
            "scope": "bar-coverage; not lossless-feed or model-readiness certification"}
