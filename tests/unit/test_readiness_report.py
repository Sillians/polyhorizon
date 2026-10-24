from unittest.mock import Mock

import pytest

from scripts.ops.readiness_report import inspect_replica


def session_for(ready, eligible=True):
    responses = [Mock(status_code=200, json=lambda: {"max_prediction_length": 3}),
        Mock(status_code=200, json=lambda: ready),
        Mock(status_code=200, json=lambda: {"forecast_gate": {"status": "eligible" if eligible else "blocked"}}),
        Mock(status_code=200, json=lambda: ready)]
    session = Mock()
    session.get.side_effect = responses

    def post(*_, **kwargs):
        symbol = kwargs["json"]["symbol"]
        return Mock(status_code=200, json=lambda: {"symbol": symbol, "model_version": "2",
            "features_timestamp": "2026-09-29T20:00:00Z", "predictions": [{"p10": 1, "p50": 2, "p90": 3}] * 3})
    session.post.side_effect = post
    return session


def test_readiness_requires_stable_champion_and_all_uncached_forecasts():
    ready = {"status": "ready", "model_version": "2", "champion_version": "2"}
    session = session_for(ready)
    result = inspect_replica("http://127.0.0.1:8000", "client", "operator", session)
    assert result["passed"] and len(result["checks"]["forecasts"]) == 3
    assert all(call.kwargs["json"]["use_cache"] is False for call in session.post.call_args_list)
    assert "operator" not in str(result) and "client" not in str(result)


@pytest.mark.parametrize("kind", ["transition", "mismatch", "ops_blocked", "no_operator"])
def test_readiness_incomplete_evidence_is_not_success(kind):
    ready = {"status": "ready", "model_version": "2", "champion_version": "2"}
    if kind == "transition":
        ready["transitioning"] = True
    if kind == "mismatch":
        ready["champion_version"] = "3"
    session = session_for(ready, eligible=kind != "ops_blocked")
    result = inspect_replica("http://127.0.0.1:8000", "", "" if kind == "no_operator" else "operator", session)
    assert not result["passed"]
    session.post.assert_not_called()


def test_no_remote_plaintext_or_embedded_secrets():
    with pytest.raises(ValueError):
        inspect_replica("http://example.com", "", "")
    with pytest.raises(ValueError):
        inspect_replica("https://user:secret@example.com", "", "")
