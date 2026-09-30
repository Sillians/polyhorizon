from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.ops import session_report as mod
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def test_gap_report_includes_missing_symbols():
    frame = pd.DataFrame({"symbol": ["NVDA"], "event_time": [pd.Timestamp("2026-09-29T14:00Z")]})
    result = mod.gap_details(frame, pd.Timestamp("2026-09-29T13:30Z"), pd.Timestamp("2026-09-29T20:00Z"))
    assert result["AAPL"]["rows"] == 0
    assert len(result["AAPL"]["missing_bars"]) == 13
    assert result["NVDA"]["gaps_over_120s"][0]["seconds"] == 1800


@pytest.fixture
def data(monkeypatch):
    opening, close = pd.Timestamp("2026-09-29T13:30Z"), pd.Timestamp("2026-09-29T20:00Z")
    trades = pd.DataFrame([dict(symbol=s, price=10., volume=1., event_time=t)
        for s in PRODUCT_SYMBOLS for t in pd.date_range(opening, close, freq="1min", inclusive="left")])
    gold = pd.DataFrame([dict(symbol=s, open=10., close=10., high=11., low=9., total_volume=1.,
        window_start=t, window_end=t + pd.Timedelta(minutes=30))
        for s in PRODUCT_SYMBOLS for t in pd.date_range(opening, close, freq="30min", inclusive="left")])
    monkeypatch.setenv("BRONZE_PATH", "s3://test/bronze")
    monkeypatch.setattr(mod, "table", lambda *a: SimpleNamespace(version=lambda: 1))
    monkeypatch.setattr(mod, "bounded_frame", lambda *a: gold if a[3] == "window_start" else trades)
    evidence = mod.qualify_session(gold, close)
    evidence["trade_coverage"] = mod.qualify_trades(trades, close)
    release = SimpleNamespace(bronze_path="bronze", bronze_version=1, gold_path="gold", gold_version=0,
                               session_qualification=evidence, model_dump=lambda **kw: {})
    monkeypatch.setattr(mod, "manifests", lambda day: [release])
    return release


def test_saved_evidence_is_revalidated(data):
    result = mod.report("2026-09-29")
    assert result["status"] == "qualified"
    assert result["published"] is False
    assert result["trained"] is False
    assert result["promoted"] is False


def test_missing_manifest_never_passes(data, monkeypatch):
    monkeypatch.setattr(mod, "manifests", lambda day: [])
    assert mod.report("2026-09-29")["status"] == "incomplete"


def test_evidence_mismatch_fails_closed(data):
    data.session_qualification = {"status": "passed"}
    assert mod.report("2026-09-29")["status"] == "incomplete"
