from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd

from polyhorizon.serving.services.feature_store import FeatureStoreClient


def test_offline_history_reads_only_the_latest_published_dataset():
    client = FeatureStoreClient.__new__(FeatureStoreClient)
    client.config = SimpleNamespace(
        connection_parameters=SimpleNamespace(model_dump=lambda exclude_none=True: {}),
        offline_store=SimpleNamespace(
            db_schema="feast_schema",
            offline_table_name="stock_ohlcv",
        ),
        inference=SimpleNamespace(time_field="event_timestamp"),
    )
    connection = Mock()
    frame = pd.DataFrame(
        {"symbol": ["NVDA"], "event_timestamp": [pd.Timestamp("2026-09-22T20:00:00Z")]}
    )

    with patch("polyhorizon.serving.services.feature_store.psycopg2.connect", return_value=connection), patch(
        "polyhorizon.serving.services.feature_store.pd.read_sql_query", return_value=frame
    ) as read_sql:
        result = client.get_offline_history("NVDA", 64)

    query = read_sql.call_args.args[0]
    assert "dataset_publications" in query
    assert "ORDER BY max_event_time DESC, updated_at DESC LIMIT 1" in query
    assert read_sql.call_args.kwargs["params"] == ("NVDA", 64)
    assert result.event_timestamp.iloc[0] == pd.Timestamp("2026-09-22T20:00:00Z")
    connection.close.assert_called_once()
