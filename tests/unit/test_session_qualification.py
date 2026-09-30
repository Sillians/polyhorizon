import pandas as pd
import pytest

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.core.session_qualification import qualify_session
from polyhorizon.core.session_qualification import qualify_trades


def bars(opening="2026-09-29T13:30Z", close="2026-09-29T20:00Z"):
    return pd.DataFrame([dict(symbol=s, window_start=t, window_end=t + pd.Timedelta(minutes=30),
                             open=10., high=11., low=9., close=10., total_volume=100)
                         for s in PRODUCT_SYMBOLS
                         for t in pd.date_range(opening, close, freq="30min", inclusive="left")])


def test_full_session_and_early_close():
    assert qualify_session(bars(), pd.Timestamp("2026-09-29T20:00Z"))["bars_per_symbol"]["NVDA"] == 13
    frame = bars("2026-11-27T14:30Z", "2026-11-27T18:00Z")
    assert qualify_session(frame, pd.Timestamp("2026-11-27T18:00Z"))["bars_per_symbol"]["NVDA"] == 7


@pytest.mark.parametrize("index", [0, 5, 12])
def test_missing_open_interior_or_close_fails(index):
    with pytest.raises(ValueError, match="Incomplete"):
        qualify_session(bars().drop(index), pd.Timestamp("2026-09-29T20:00Z"))


@pytest.mark.parametrize("kind", ["duplicate", "nan", "zero", "duration", "symbol"])
def test_bad_data_fails(kind):
    frame = bars()
    if kind == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]])
    elif kind == "nan":
        frame.loc[0, "close"] = float("nan")
    elif kind == "zero":
        frame.loc[0, "total_volume"] = 0
    elif kind == "duration":
        frame.loc[0, "window_end"] += pd.Timedelta(minutes=1)
    else:
        frame.loc[0, "symbol"] = "INVALID"
    with pytest.raises(ValueError):
        qualify_session(frame, pd.Timestamp("2026-09-29T20:00Z"))


def test_weekend_rejected():
    with pytest.raises(ValueError, match="NYSE"):
        qualify_session(bars(), pd.Timestamp("2026-09-26T20:00Z"))


@pytest.mark.parametrize("gap", ["none", "opening", "interior", "closing"])
def test_trade_boundaries_and_gaps(gap):
    times = pd.date_range("2026-09-29T13:30Z", "2026-09-29T20:00Z", freq="1min", inclusive="left")
    if gap == "opening":
        times = times[3:]
    elif gap == "closing":
        times = times[:-3]
    elif gap == "interior":
        times = times.delete([100, 101, 102])
    frame = pd.DataFrame([dict(symbol=s, price=10., volume=1., event_time=t)
                          for s in PRODUCT_SYMBOLS for t in times])
    if gap == "none":
        assert qualify_trades(frame, pd.Timestamp("2026-09-29T20:00Z"))["NVDA"]["trade_count"] == 390
    else:
        with pytest.raises(ValueError, match="gap"):
            qualify_trades(frame, pd.Timestamp("2026-09-29T20:00Z"))
