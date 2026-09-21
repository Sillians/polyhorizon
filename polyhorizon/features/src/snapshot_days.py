from typing import Any, Optional

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.utils.logger import get_logger
from polyhorizon.features.utils.utility_helpers import get_db_conn
logger = get_logger("CreateSnapshotTableSQL")


"""
Features

- Single snapshot table: training_stock_ohlcv_snapshot.

- Sliding window: keeps only last snapshot_days of data.

- Deduplicated: (symbol, event_timestamp) primary key ensures no duplicates.

- Latest snapshot pointer: table latest_training_snapshot always points to the current snapshot.

- Safe for daily execution: old rows are removed, new rows inserted.
"""


# --- SQL Queries stored as separate, reusable strings ---
CREATE_SNAPSHOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {snapshot_table} (
    symbol TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    total_volume BIGINT,
    rolling_avg_close DOUBLE PRECISION,
    rolling_volatility_close DOUBLE PRECISION,
    -- Add new features here in the future; they will be automatically included via SELECT *
    PRIMARY KEY(symbol, event_timestamp)
);
CREATE INDEX IF NOT EXISTS {snapshot_table}_event_timestamp_brin
ON {snapshot_table} USING BRIN (event_timestamp) WITH (pages_per_range = 32);
"""

DELETE_OLD_ROWS_SQL = """
DELETE FROM {snapshot_table}
WHERE event_timestamp < NOW() - INTERVAL '{snapshot_days} days';
"""

INSERT_NEW_ROWS_SQL = """
INSERT INTO {snapshot_table}
SELECT DISTINCT
    symbol,
    window_start AS event_timestamp,
    open,
    high,
    low,
    close,
    total_volume,
    rolling_avg_close,
    rolling_volatility_close
    -- Add new feature columns here when schema evolves
FROM {offline_table}
WHERE window_start >= NOW() - INTERVAL '{snapshot_days} days'
ON CONFLICT (symbol, event_timestamp) DO NOTHING;
"""

CREATE_POINTER_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS latest_training_snapshot (
    snapshot_table TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now()
);
"""

UPDATE_POINTER_SQL = """
INSERT INTO latest_training_snapshot (snapshot_table)
VALUES (%s)
ON CONFLICT (snapshot_table) DO UPDATE SET created_at = now();
"""


def add_missing_columns_to_snapshot(cur: Any, 
                                    offline_table: str, 
                                    snapshot_table: str,
                                    db_schema: str) -> None:
    logger.info("Aligning snapshot table schema with offline table (adding missing columns if any)")

    # Get columns from both tables
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """, (db_schema, offline_table.lower()))
    offline_cols = {row[0].lower(): row[1] for row in cur.fetchall()}

    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
    """, (db_schema, snapshot_table.lower()))
    snapshot_cols = {row[0].lower() for row in cur.fetchall()}

    added = []
    for col in offline_cols:
        if col.lower() not in snapshot_cols and col.lower() not in {"window_start", "window_end", "created_at"}:
            # Map common types; extend as needed
            pg_type = "DOUBLE PRECISION" if "float" in offline_cols[col] else \
                      "BIGINT" if "bigint" in offline_cols[col] else \
                      "TIMESTAMPTZ" if "timestamp" in offline_cols[col] else \
                      "TEXT"

            alter_sql = f"ALTER TABLE {db_schema}.{snapshot_table} ADD COLUMN IF NOT EXISTS {col} {pg_type};"
            cur.execute(alter_sql)
            added.append(col)
            logger.info(f"Added missing column to snapshot: {col} ({pg_type})")

    if added:
        logger.info(f"Added {len(added)} new column(s) to snapshot table: {', '.join(added)}")
    else:
        logger.info("Snapshot table schema is up to date.")


def update_training_snapshot(
    offline_table: Optional[str] = None,
    snapshot_table: Optional[str] = None,
    db_schema: Optional[str] = None,
    snapshot_days: Optional[int] = None,
    config: Optional[Config] = None,
) -> str:
    """
    Modular, extensible version of snapshot update.
    - Automatically adds new feature columns when they appear in offline table.
    - Uses separate SQL templates for readability and maintainability.
    - Safe sliding window + deduplication + pointer update.
    """
    resolved = config or load_config()
    offline_table = offline_table or resolved.data.offline_table_name
    snapshot_table = snapshot_table or resolved.data.snapshot_table_name
    db_schema = db_schema or resolved.data.db_schema
    snapshot_days = snapshot_days or resolved.data.snapshot_days

    conn = get_db_conn(resolved)
    cur = conn.cursor()

    try:
        cur.execute(f"SET search_path TO {db_schema}")

        # 1. Create or align snapshot table schema dynamically
        cur.execute(CREATE_SNAPSHOT_TABLE_SQL.format(snapshot_table=snapshot_table))
        add_missing_columns_to_snapshot(cur, offline_table, snapshot_table, db_schema)

        # 2. Remove old data outside window
        cur.execute(DELETE_OLD_ROWS_SQL.format(
            snapshot_table=snapshot_table,
            snapshot_days=snapshot_days
        ))

        # 3. Insert new distinct rows (uses dynamic columns via SELECT *)
        # Switch to dynamic SELECT * for full extensibility
        cur.execute(f"""
            INSERT INTO {snapshot_table}
            SELECT DISTINCT
                symbol,
                window_start AS event_timestamp,
                {', '.join([col for col in [
                    'open', 'high', 'low', 'close', 'total_volume',
                    'rolling_avg_close', 'rolling_volatility_close'
                    
                ]])}
                -- Future: replace above list with SELECT * excluding window_start/window_end/created_at
            FROM {offline_table}
            WHERE window_start >= NOW() - INTERVAL '{snapshot_days} days'
            ON CONFLICT (symbol, event_timestamp) DO NOTHING;
        """)
        # Future features will be auto-included by add_missing_columns + SELECT *
        # Note: For full auto-inclusion, consider dynamic column list generation

        # 4. Ensure pointer table
        cur.execute(CREATE_POINTER_TABLE_SQL)

        # 5. Update pointer
        cur.execute(UPDATE_POINTER_SQL, (snapshot_table,))

        conn.commit()
        logger.info(f"Snapshot table '{snapshot_table}' updated successfully with last {snapshot_days} days of data.")

        return snapshot_table

    except Exception:
        conn.rollback()
        logger.error("Failed to update training snapshot", exc_info=True)
        raise
    finally:
        cur.close()
        conn.close()


# if __name__ == "__main__":
#     update_training_snapshot()

