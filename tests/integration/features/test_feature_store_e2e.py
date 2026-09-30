from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.feast_ops.feature_manager import FeatureManager


@pytest.mark.integration
def test_feature_store_e2e() -> None:
    if os.getenv("FEATURES_E2E") != "1":
        pytest.skip("Set FEATURES_E2E=1 to run integration test")

    psycopg2 = pytest.importorskip("psycopg2")

    config_path = os.getenv("FEATURES_CONFIG_PATH")
    config = load_config(config_path)

    # Insert a minimal offline row
    conn = psycopg2.connect(**config.connection_parameters.model_dump(exclude_none=True))
    cur = conn.cursor()

    schema = config.data.db_schema
    table = config.data.offline_table_name
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            symbol TEXT NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ,
            open DOUBLE PRECISION,
            high DOUBLE PRECISION,
            low DOUBLE PRECISION,
            close DOUBLE PRECISION,
            total_volume BIGINT,
            created_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, window_start)
        );
        """
    )

    now = datetime.now(timezone.utc)
    row = (
        "E2E",
        now,
        now,
        now + timedelta(minutes=1),
        100.0,
        110.0,
        90.0,
        105.0,
        1000,
    )
    cur.execute(
        f"""
        INSERT INTO {schema}.{table}
        (symbol, event_timestamp, window_start, window_end, open, high, low, close, total_volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """,
        row,
    )
    conn.commit()
    cur.close()
    conn.close()

    fm = FeatureManager(config)
    try:
        fm.store.get_feature_view(config.project.feature_view)
    except Exception:
        pytest.skip("Feature view not found; run `feast apply` before E2E test")

    start_date = now - timedelta(days=1)
    end_date = now + timedelta(minutes=1)
    fm.materialize_features(
        start_date=start_date,
        end_date=end_date,
        incremental=False,
        auto_detect=False,
        allow_backfill=True,
    )

    online = fm.get_latest_stock_features(["E2E"])
    assert not online.empty
    assert "close" in online.columns
