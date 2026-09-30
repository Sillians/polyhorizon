from datetime import datetime, timezone
from types import SimpleNamespace

from polyhorizon.features.utils.metrics import FeatureMetricsPublisher


def test_publication_metrics_preserve_all_symbol_gaps(monkeypatch):
    calls = []

    def capture(_url, job, registry):
        calls.append((job, [sample for family in registry.collect() for sample in family.samples]))

    monkeypatch.setattr("polyhorizon.features.utils.metrics.pushadd_to_gateway", capture)
    config = SimpleNamespace(enable_metrics=True, pushgateway_url="http://pushgateway:9091",
                             metrics_prefix="features")
    FeatureMetricsPublisher(config).push_publication_metrics(
        success=True, event_timestamp=datetime(2026, 9, 23, 20, tzinfo=timezone.utc),
        interior_session_gaps={"NVDA": 0, "AAPL": 1, "MSFT": 2},
    )
    names = {sample.name for _, samples in calls for sample in samples}
    assert "features_publication_success" in names
    assert "features_last_published_event_timestamp_seconds" in names
    gaps = [sample for _, samples in calls for sample in samples
            if sample.name == "features_interior_session_gaps"]
    assert {sample.labels["symbol"]: sample.value for sample in gaps} == {
        "NVDA": 0, "AAPL": 1, "MSFT": 2,
    }
