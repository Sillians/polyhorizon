import pytest

from polyhorizon.serving.services.freshness import StaleFeaturesError, require_post_close_features


@pytest.mark.parametrize("now,cutoff", [
    ("2026-07-27T20:59Z", "2026-07-24T20:00Z"),  # grace period
    ("2026-07-26T22:00Z", "2026-07-24T20:00Z"),  # weekend
    ("2026-07-03T22:00Z", "2026-07-02T20:00Z"),  # holiday
    ("2026-11-27T19:00Z", "2026-11-27T18:00Z"),  # early close, EST
    ("2026-07-27T20:01Z", "2026-07-27T20:00Z"),  # early publication
])
def test_accepts_completed_session(now, cutoff):
    require_post_close_features(cutoff, now=now)


@pytest.mark.parametrize("now,cutoff", [
    ("2026-07-27T21:00Z", "2026-07-24T20:00Z"),
    ("2026-07-27T18:00Z", "2026-07-27T17:30Z"),
    ("2026-07-27T18:00Z", "2026-07-27T20:00Z"),
    ("2026-11-27T19:00Z", "2026-11-25T21:00Z"),
    ("2026-07-27T18:00Z", None),
    ("2026-07-27T18:00Z", "2026-07-24T20:00"),
])
def test_rejects_stale_partial_future_or_missing(now, cutoff):
    with pytest.raises(StaleFeaturesError):
        require_post_close_features(cutoff, now=now)
