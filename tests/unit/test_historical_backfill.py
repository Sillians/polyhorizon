from datetime import date, datetime, timezone

import pandas as pd
import pandas_market_calendars as mcal
import pytest

from polyhorizon.core.historical_qualification import HistoricalSource, bars_digest, qualify_historical_bars
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.ingestion.historical_backfill import build_release, save_json_exclusive


def history(start="2026-09-28", end="2026-09-29"):
    schedule = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    return pd.DataFrame([dict(symbol=s, window_start=t, window_end=t + pd.Timedelta(minutes=30),
                             open=100. + i, high=102. + i, low=99. + i, close=101. + i, total_volume=100)
                         for s in PRODUCT_SYMBOLS for _, day in schedule.iterrows()
                         for i, t in enumerate(pd.date_range(day.market_open, day.market_close, freq="30min", inclusive="left"))])


def source(frame, start="2026-09-28", end="2026-09-29"):
    return HistoricalSource(provider="csv", adjustment="unadjusted", start_date=date.fromisoformat(start),
        end_date=date.fromisoformat(end), retrieved_at=datetime(2026, 10, 3, tzinfo=timezone.utc),
        content_sha256=bars_digest(frame), request_sha256="a" * 64)


def test_historical_session_calendar_and_early_close():
    frame = history("2026-11-25", "2026-11-27")
    recorded = source(frame, "2026-11-25", "2026-11-27").model_copy(update={"retrieved_at": datetime(2026, 12, 1, tzinfo=timezone.utc)})
    evidence = qualify_historical_bars(frame, recorded, now=pd.Timestamp("2026-12-02T00:00Z"))
    assert evidence["session_count"] == 2
    assert evidence["sessions"][-1]["bars_per_symbol"]["NVDA"] == 7
    assert "trade_coverage" not in evidence


@pytest.mark.parametrize("kind", ["missing", "duplicate", "fractional", "off_session", "checksum"])
def test_historical_fail_closed(kind):
    frame = history()
    original = source(frame)
    if kind == "missing":
        frame = frame.drop(0)
    elif kind == "duplicate":
        frame = pd.concat([frame, frame.iloc[:1]])
    elif kind == "fractional":
        frame["total_volume"] = frame.total_volume.astype(float)
        frame.loc[0, "total_volume"] = 1.5
    elif kind == "off_session":
        frame.loc[0, "window_start"] -= pd.Timedelta(minutes=30)
    else:
        frame.loc[0, "close"] += 0.1
    with pytest.raises(ValueError):
        qualify_historical_bars(frame, original if kind == "checksum" else source(frame))


def test_immutable_delta_release_retry(tmp_path):
    frame = history()
    kwargs = dict(feature_root=str(tmp_path / "features"), raw_root=str(tmp_path / "raw"), lookback_bars=2)
    release = build_release(frame, source(frame), **kwargs)
    assert release.row_count == len(frame) - 3
    assert release == build_release(frame.sample(frac=1, random_state=2), source(frame), **kwargs)
    from deltalake import DeltaTable
    features = DeltaTable(release.feature_path).to_pandas()
    assert features.loc[features.symbol == "NVDA", "rolling_avg_close"].iloc[0] == 101.5


def test_atomic_evidence_never_overwritten(tmp_path):
    target = tmp_path / "nested/manifest.json"
    save_json_exclusive(target, {"id": 1})
    with pytest.raises(FileExistsError):
        save_json_exclusive(target, {"id": 2})
    assert '"id": 1' in target.read_text()


def test_checksum_preserves_bigint_precision():
    frame = history()
    frame.loc[0, "total_volume"] = 2**53
    before = bars_digest(frame)
    frame.loc[0, "total_volume"] = 2**53 + 1
    assert before != bars_digest(frame)


def test_future_retrieval_and_incomplete_session_are_rejected():
    frame = history()
    with pytest.raises(ValueError, match="retrieval time"):
        qualify_historical_bars(frame, source(frame), now=pd.Timestamp("2026-09-29T20:00Z"))
    recorded = source(frame).model_copy(update={"retrieved_at": datetime(2026, 9, 29, 19, tzinfo=timezone.utc)})
    with pytest.raises(ValueError, match="unfinished session"):
        qualify_historical_bars(frame, recorded, now=pd.Timestamp("2026-09-29T19:00Z"))


def test_provider_retries_and_resumes_without_refetch(tmp_path, monkeypatch):
    from unittest.mock import Mock
    from polyhorizon.ingestion.historical_backfill import fetch_finnhub
    frame = history("2026-09-29", "2026-09-29").loc[lambda f: f.symbol == "NVDA"]
    payload = {"s": "ok", "t": [int(t.timestamp()) for t in frame.window_start],
               "o": frame.open.tolist(), "h": frame.high.tolist(), "l": frame.low.tolist(),
               "c": frame.close.tolist(), "v": frame.total_volume.tolist()}
    session = Mock()
    session.get.side_effect = [Mock(status_code=429)] + [Mock(status_code=200, json=lambda: payload)] * 3
    monkeypatch.setattr("polyhorizon.ingestion.historical_backfill.time.sleep", lambda _: None)
    start = end = date(2026, 9, 29)
    first, retrieved = fetch_finnhub(start, end, "not-a-real-secret", tmp_path, session)
    assert len(first) == 39 and session.get.call_count == 4
    assert session.get.call_args.kwargs["headers"] == {"X-Finnhub-Token": "not-a-real-secret"}
    assert "token" not in session.get.call_args.kwargs["params"]
    session.get.reset_mock()
    second, retried = fetch_finnhub(start, end, "not-a-real-secret", tmp_path, session)
    pd.testing.assert_frame_equal(first, second)
    assert retrieved == retried
    session.get.assert_not_called()


def test_provider_access_denied_does_not_leak_credentials(tmp_path):
    from unittest.mock import Mock
    from polyhorizon.ingestion.historical_backfill import fetch_finnhub
    session = Mock(get=Mock(return_value=Mock(status_code=403)))
    with pytest.raises(RuntimeError, match="HTTP 403") as error:
        fetch_finnhub(date(2026, 9, 29), date(2026, 9, 29), "secret-credential", tmp_path, session)
    assert "secret-credential" not in str(error.value)
    assert not list(tmp_path.glob("*.json"))
