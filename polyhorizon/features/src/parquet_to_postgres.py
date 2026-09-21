
import io
import json
import hashlib
import re
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2
from deltalake import DeltaTable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.utils.logger import get_logger
from polyhorizon.features.utils.utility_helpers import get_db_conn

logger = get_logger("ParquetToPostgres")
_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(value: str, kind: str) -> str:
    if not _SQL_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe PostgreSQL {kind}: {value!r}")
    return value


def _resolve_config(config: Optional[Config]) -> Config:
    return config or load_config()


def make_utc_aware(dt_value) -> pd.Timestamp:
    if dt_value is None:
        return pd.Timestamp.min.tz_localize("UTC")
    ts = pd.to_datetime(dt_value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    else:
        return ts.tz_convert("UTC")


def impute_missing_values(df: pd.DataFrame, 
                          group_col: str = "symbol") -> pd.DataFrame:
    if df.empty:
        return df

    # 1. Sort chronologically
    df = df.sort_values([group_col, "window_start", "window_end"])

    # 2. Identify numeric columns
    fill_cols = df.select_dtypes(include=['number']).columns
    
    # Calculate null counts before
    nulls_before = df[fill_cols].isnull().sum().sum()
    total_cells = df[fill_cols].size

    # 3. Apply imputation
    # ffill carries the last known state forward; bfill handles the very first rows
    df[fill_cols] = (
        df.groupby(group_col, sort=False)[fill_cols]
        .ffill()
        .bfill()
    )

    # 4. Calculate null counts after and log stats
    nulls_after = df[fill_cols].isnull().sum().sum()
    fixed_count = nulls_before - nulls_after
    
    success_rate = (fixed_count / nulls_before * 100) if nulls_before > 0 else 100
    remaining_pct = (nulls_after / total_cells * 100)

    logger.info(f"Imputation Stats: Fixed {fixed_count} nulls ({success_rate:.2f}% success).")
    
    if nulls_after > 0:
        logger.warning(f"Remaining Nulls: {nulls_after} cells ({remaining_pct:.2f}% of numeric data) are still NULL.")
    
    return df


def add_missing_columns(
    cur,
    table_name: str,
    df: pd.DataFrame,
    db_schema: str,
) -> None:
    logger.info("Checking and adding missing columns if any...")

    # Get existing columns from information_schema
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s;
    """, (db_schema, table_name.lower()))
    
    existing_cols = {row[0].lower() for row in cur.fetchall()}

    # Define type mapping (extend as needed)
    type_map = {
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'bool': 'BOOLEAN',
        'object': 'TEXT',
        'string': 'TEXT',
        'datetime64[ns, UTC]': 'TIMESTAMPTZ',
        'datetime64[ns]': 'TIMESTAMP',
    }

    added = []
    for col in df.columns:
        col_lower = col.lower()
        if col_lower not in existing_cols:
            pd_type = str(df[col].dtype)
            pg_type = type_map.get(pd_type, 'TEXT')  # fallback to TEXT
            
            # Special handling for timestamp columns
            if pd_type.startswith('datetime'):
                pg_type = 'TIMESTAMPTZ'
            
            alter_sql = f"ALTER TABLE {db_schema}.{table_name} ADD COLUMN IF NOT EXISTS {col} {pg_type};"
            cur.execute(alter_sql)
            added.append(col)
            logger.info(f"Added new column: {col} ({pg_type})")

    if added:
        logger.info(f"Added {len(added)} new column(s): {', '.join(added)}")
    else:
        logger.info("No new columns to add.")


def validate_data_contract(df: pd.DataFrame, config: Config) -> None:
    for column in df.columns:
        _validate_identifier(str(column), "column name")
    contract = config.data_contract
    missing = [c for c in contract.required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in offline data: {missing}")

    def _normalize(dtype: str) -> str:
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "datetime"
        if pd.api.types.is_float_dtype(dtype):
            return "float"
        if pd.api.types.is_integer_dtype(dtype):
            return "int"
        if pd.api.types.is_bool_dtype(dtype):
            return "bool"
        return "string"

    for col, expected in contract.column_types.items():
        if col not in df.columns:
            raise ValueError(f"Contract expects column '{col}' but it is missing")
        actual = _normalize(df[col].dtype)
        if actual != expected:
            raise ValueError(
                f"Column '{col}' expected type '{expected}' but found '{actual}'"
            )

    if not contract.allow_extra_columns:
        extra = [c for c in df.columns if c not in contract.required_columns]
        if extra:
            raise ValueError(f"Extra columns found but not allowed: {extra}")


def _ensure_schema_version_table(cur, db_schema: str) -> None:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.feature_schema_versions (
            id BIGSERIAL PRIMARY KEY,
            table_name TEXT NOT NULL,
            schema_hash TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            columns_json JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS feature_schema_versions_table_hash_uq
        ON {db_schema}.feature_schema_versions (table_name, schema_hash);
    """)


def _ensure_feature_indexes(cur, db_schema: str, table_name: str) -> None:
    """Create indexes for the two dominant ML access patterns.

    The primary key serves per-symbol history reads. A BRIN index keeps broad
    training/drift time scans cheap without the write amplification of a second
    large B-tree on an append-oriented table.
    """
    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS {table_name}_window_start_brin
        ON {db_schema}.{table_name} USING BRIN (window_start)
        WITH (pages_per_range = 32);
    """)


def _record_schema_version(cur, db_schema: str, table_name: str, df: pd.DataFrame) -> int:
    columns = [
        {"name": col, "dtype": str(df[col].dtype)}
        for col in sorted(df.columns)
    ]
    payload = json.dumps(columns, sort_keys=True)
    schema_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    cur.execute(
        f"""
        SELECT schema_hash, schema_version
        FROM {db_schema}.feature_schema_versions
        WHERE table_name = %s
        ORDER BY created_at DESC
        LIMIT 1;
        """,
        (table_name,),
    )
    row = cur.fetchone()
    if row is None:
        schema_version = 1
        cur.execute(
            f"""
            INSERT INTO {db_schema}.feature_schema_versions
            (table_name, schema_hash, schema_version, columns_json)
            VALUES (%s, %s, %s, %s);
            """,
            (table_name, schema_hash, schema_version, payload),
        )
        return schema_version

    last_hash, last_version = row
    if last_hash == schema_hash:
        return int(last_version)

    schema_version = int(last_version) + 1
    cur.execute(
        f"""
        INSERT INTO {db_schema}.feature_schema_versions
        (table_name, schema_hash, schema_version, columns_json)
        VALUES (%s, %s, %s, %s);
        """,
        (table_name, schema_hash, schema_version, payload),
    )
    return schema_version


def _table_exists(cur, db_schema: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s;
        """,
        (db_schema, table_name.lower()),
    )
    return cur.fetchone() is not None


def _is_partitioned(cur, db_schema: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s;
        """,
        (db_schema, table_name.lower()),
    )
    row = cur.fetchone()
    return row is not None and row[0] == "p"


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _add_month(dt: datetime) -> datetime:
    year = dt.year + (1 if dt.month == 12 else 0)
    month = 1 if dt.month == 12 else dt.month + 1
    return dt.replace(year=year, month=month, day=1)


def _ensure_partitions_for_range(
    cur,
    db_schema: str,
    table_name: str,
    start_ts: datetime,
    end_ts: datetime,
    granularity: str,
) -> None:
    if granularity == "day":
        current = start_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        end = end_ts.replace(hour=0, minute=0, second=0, microsecond=0)
        while current <= end:
            next_day = current + pd.Timedelta(days=1)
            partition = f"{table_name}_{current:%Y%m%d}"
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{partition}
                PARTITION OF {db_schema}.{table_name}
                FOR VALUES FROM (%s) TO (%s);
                """,
                (current, next_day),
            )
            current = next_day
        return

    current = _month_start(start_ts)
    end = _month_start(end_ts)
    while current <= end:
        next_month = _add_month(current)
        partition = f"{table_name}_{current:%Y%m}"
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {db_schema}.{partition}
            PARTITION OF {db_schema}.{table_name}
            FOR VALUES FROM (%s) TO (%s);
            """,
            (current, next_month),
        )
        current = next_month


def _ensure_partitioned_table(cur, db_schema: str, table_name: str) -> None:
    if _table_exists(cur, db_schema, table_name):
        if not _is_partitioned(cur, db_schema, table_name):
            logger.warning(
                "Table %s.%s exists but is not partitioned; skipping partition setup",
                db_schema,
                table_name,
            )
        return

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
            symbol TEXT NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (symbol, window_start)
        ) PARTITION BY RANGE (window_start);
    """)


@retry(
    stop=stop_after_attempt(3), 
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.OperationalError),
    before_sleep=lambda retry_state: print(f"Retrying DB connection... Attempt {retry_state.attempt_number}")
)
def delta_seaweedfs_to_postgres_copy(
    table_name: Optional[str] = None,
    config: Optional[Config] = None,
) -> Dict[str, Any]:
    logger.info("Configuration for SeaweedFS S3 Gateway")
    resolved = _resolve_config(config)
    db_schema = resolved.data.db_schema
    table_name = table_name or resolved.data.offline_table_name
    _validate_identifier(db_schema, "schema name")
    _validate_identifier(table_name, "table name")
    seaweed_path = resolved.bucket_details.feature_parquet_path  # e.g. s3://bucket/path/to/delta

    storage_options = {
        "AWS_ENDPOINT_URL": resolved.bucket_details.seaweedfs_s3_endpoint,
        "AWS_ACCESS_KEY_ID": resolved.bucket_details.seaweedfs_access_key,
        "AWS_SECRET_ACCESS_KEY": resolved.bucket_details.seaweedfs_secret_key,
        "AWS_REGION": "us-east-1",  # required but not used by SeaweedFS
        "AWS_ALLOW_HTTP": "true",   # Seaweed usually runs without TLS locally
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # needed for Delta log ops
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",  # forces path-style URLs (important)
    }

    logger.info(f"Reading from SeaweedFS Delta table: {seaweed_path}")

    conn = None
    cur = None
    lock_key = None
    try:
        # Load latest Delta version from SeaweedFS
        dt = DeltaTable(seaweed_path, storage_options=storage_options)
        df = dt.to_pandas()

        if df.empty:
            logger.info("Delta table empty — nothing to ingest")
            return {"inserted": 0}

        conn = get_db_conn(resolved)
        cur = conn.cursor()

        # hashtext is stable across Python processes; Python's hash() is not.
        lock_key = f"{db_schema}.{table_name}"
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s));", (lock_key,))
        if not cur.fetchone()[0]:
            raise RuntimeError("Could not acquire advisory lock")

        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
        conn.commit()

        if resolved.data.enable_partitioning:
            _ensure_partitioned_table(cur, db_schema, table_name)
            conn.commit()
        else:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
                    symbol TEXT NOT NULL,
                    window_start TIMESTAMPTZ NOT NULL,
                    window_end TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (symbol, window_start)
                );
            """)
            conn.commit()

        _ensure_feature_indexes(cur, db_schema, table_name)
        conn.commit()

        if "event_timestamp" not in df.columns and "window_start" in df.columns:
            df["event_timestamp"] = df["window_start"]

        df["window_start"] = df["window_start"].apply(make_utc_aware)
        validate_data_contract(df, resolved)
        df = impute_missing_values(df)

        add_missing_columns(cur, table_name, df, db_schema)
        _ensure_schema_version_table(cur, db_schema)
        _record_schema_version(cur, db_schema, table_name, df)
        conn.commit()

        if resolved.data.enable_partitioning and _is_partitioned(cur, db_schema, table_name):
            start_ts = pd.to_datetime(df["window_start"].min()).to_pydatetime()
            end_ts = pd.to_datetime(df["window_start"].max()).to_pydatetime()
            _ensure_partitions_for_range(
                cur,
                db_schema,
                table_name,
                start_ts,
                end_ts,
                resolved.data.partition_granularity,
            )
            conn.commit()

        cur.execute(f"""
            SELECT symbol, MAX(window_start) AS max_ws
            FROM {db_schema}.{table_name}
            GROUP BY symbol;
        """)
        rows = cur.fetchall()

        max_window_dict = {
            symbol: make_utc_aware(max_ts)
            for symbol, max_ts in rows
        }

        df["max_ws"] = df["symbol"].map(max_window_dict).fillna(pd.Timestamp.min.tz_localize("UTC"))
        df_new = df[df["window_start"] > df["max_ws"]].copy()
        df_new.drop(columns=["max_ws"], inplace=True)

        if df_new.empty:
            logger.info("No new rows to insert")
            return {"inserted": 0}

        csv_buffer = io.StringIO()
        df_new.to_csv(
            csv_buffer,
            sep="\t",
            header=False,
            index=False,
            na_rep="",
            date_format="%Y-%m-%d %H:%M:%S%z",
        )
        csv_buffer.seek(0)

        columns_str = ", ".join(df_new.columns)
        copy_sql = f"""
            COPY {db_schema}.{table_name} ({columns_str}) 
            FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')
        """
        cur.copy_expert(copy_sql, csv_buffer)
        conn.commit()

        inserted = len(df_new)
        logger.info(f"Successfully inserted {inserted} new rows")
        return {"inserted": inserted}

    except Exception:
        if conn:
            conn.rollback()
        logger.error("Ingestion failed", exc_info=True)
        raise
    finally:
        if cur and lock_key is not None:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s));", (lock_key,))
        if cur:
            cur.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    delta_seaweedfs_to_postgres_copy()
    
