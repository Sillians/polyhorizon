from __future__ import annotations

from contextlib import contextmanager
from typing import Generator, Optional

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as Psycopg2Connection
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.utils.logger import get_logger

logger = get_logger("FeatureStoreDB")


def _resolve_config(config: Optional[Config]) -> Config:
    return config or load_config()


@contextmanager
def get_db_conn_generator(config: Optional[Config] = None) -> Generator[Psycopg2Connection, None, None]:
    resolved = _resolve_config(config)
    params = resolved.connection_parameters.model_dump(exclude_none=True)
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()


def get_db_conn(config: Optional[Config] = None) -> Psycopg2Connection:
    resolved = _resolve_config(config)
    params = resolved.connection_parameters.model_dump(exclude_none=True)
    return psycopg2.connect(**params)


def ensure_utc(ts) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_convert("UTC") if ts.tzinfo else ts.tz_localize("UTC")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(psycopg2.OperationalError),
    before_sleep=lambda retry_state: logger.warning(
        "Retrying DB connection... Attempt %s", retry_state.attempt_number
    ),
)
def fetch_stock_data(ticker: str, config: Optional[Config] = None) -> pd.DataFrame:
    resolved = _resolve_config(config)
    table_path = f"{resolved.data.db_schema}.{resolved.data.offline_table_name}"
    query = f"SELECT * FROM {table_path} WHERE symbol = %s"

    conn = get_db_conn(resolved)
    try:
        df = pd.read_sql_query(query, conn, params=(ticker,))
        if not df.empty and "event_timestamp" in df.columns:
            df["event_timestamp"] = df["event_timestamp"].apply(ensure_utc)
        return df
    except Exception:
        logger.error("Error fetching data for %s", ticker, exc_info=True)
        raise
    finally:
        conn.close()
