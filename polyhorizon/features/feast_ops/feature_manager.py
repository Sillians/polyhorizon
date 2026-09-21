from __future__ import annotations

from datetime import datetime, timedelta, UTC
from uuid import uuid4
from typing import List, Optional

import pandas as pd
from scipy import stats
from scipy.stats import ks_2samp

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.feast_ops.store import init_feature_store
from polyhorizon.features.feast_ops.entity_df import (
    build_entity_df,
    build_entity_training_data,
    load_sp500_symbols,
)
from polyhorizon.features.utils.logger import get_logger
from polyhorizon.features.utils.utility_helpers import get_db_conn


class FeatureManager:
    """High-level manager for Feast operations."""

    def __init__(self, config: Optional[Config] = None, *, load_symbols: bool = True):
        self.config = config or load_config()
        self.store = init_feature_store(config=self.config)
        self.logger = get_logger("FeatureManager")

        if not load_symbols:
            from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
            self.symbols = list(PRODUCT_SYMBOLS)
            return
        symbols = load_sp500_symbols(
            config=self.config,
            bucket=self.config.bucket_details.symbols_bucket,
            key=self.config.bucket_details.symbols_key,
        )
        self.symbols = symbols or self.config.features.symbols

    def list_symbols(self) -> List[str]:
        return self.symbols

    def validate_features(
        self,
        df: pd.DataFrame,
        check_nulls: bool = True,
        check_duplicates: bool = True,
        check_outliers: bool = False,
    ) -> bool:
        missing_cols = [c for c in self.config.features.required_columns if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required feature columns: {missing_cols}")

        if "rolling_volatility_close" in df.columns and df["rolling_volatility_close"].isnull().any():
            self.logger.info(
                "Detected missing values in 'rolling_volatility_close' — filling with 0.0"
            )
            df["rolling_volatility_close"] = df["rolling_volatility_close"].fillna(0.0)

        if check_nulls and df.isnull().any().any():
            raise ValueError("Null values detected in feature set.")

        if check_duplicates and df.duplicated().any():
            raise ValueError("Duplicate records detected in feature set.")

        if check_outliers:
            numeric_df = df.select_dtypes(include=["float64", "int64"])
            z_scores = abs(stats.zscore(numeric_df, nan_policy="omit"))
            if (z_scores > self.config.feast_parameters.z_threshold).any():
                self.logger.warning("Potential outliers detected beyond Z-score threshold.")

        self.logger.info("Feature validation passed.")
        return True

    def fetch_example_features(self) -> pd.DataFrame:
        entity_df = build_entity_df(symbols=self.symbols)
        features = self.config.feast_features.stock_ohlcv_features

        feature_data = self.store.get_historical_features(
            entity_df=entity_df,
            features=features,
        ).to_df()

        feature_data.sort_values(["symbol", "event_timestamp"], inplace=True)
        feature_data.reset_index(drop=True, inplace=True)
        return feature_data

    def generate_training_data(
        self,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        features = self.config.features.stock_ohlcv_features
        if not features:
            raise ValueError("Feature list cannot be empty.")

        entity_df = build_entity_training_data(
            self.symbols,
            start_date,
            end_date,
            self.config.feast_parameters.freq,
        )

        training_data = self.store.get_historical_features(
            entity_df=entity_df,
            features=features,
        ).to_df()

        training_data.sort_values(["symbol", "event_timestamp"], inplace=True)
        training_data.reset_index(drop=True, inplace=True)
        return training_data

    def fetch_online_features(self) -> dict:
        feature_view = self.config.project.feature_view
        features = self.config.feast_features.stock_ohlcv_features
        if not features:
            features = [
                f"{feature_view}:{f.name}" for f in self.store.get_feature_view(feature_view).features
            ]

        entity_rows = [{"symbol": sym} for sym in self.symbols]
        return self.store.get_online_features(
            features=features,
            entity_rows=entity_rows,
            include_timestamp=True,
        ).to_dict()

    def get_latest_stock_features(self, tickers: list[str]) -> pd.DataFrame:
        feature_refs = self.config.feast_features.stock_ohlcv_features

        entity_rows = [{"symbol": t} for t in tickers]
        online_response = self.store.get_online_features(
            features=feature_refs,
            entity_rows=entity_rows,
            include_timestamp=True,
        )

        df = online_response.to_df()
        if df.isnull().values.any():
            self.logger.warning(
                "Some features returned NULL for tickers %s. Check materialization status.",
                tickers,
            )

        return df

    def detect_feature_drift(
        self,
        reference_df: pd.DataFrame,
        production_df: pd.DataFrame,
    ) -> pd.DataFrame:
        drift_report = []
        common_cols = set(reference_df.columns) & set(production_df.columns)
        threshold = self.config.feast_parameters.drift_threshold

        for col in common_cols:
            if pd.api.types.is_numeric_dtype(reference_df[col]):
                _, p_val = ks_2samp(reference_df[col].dropna(), production_df[col].dropna())
                drift_report.append(
                    {
                        "feature": col,
                        "p_value": round(p_val, 4),
                        "drift_detected": p_val < threshold,
                    }
                )

        return pd.DataFrame(drift_report)

    def get_snapshot_freshness(self) -> dict:
        """Return latest snapshot timestamp and freshness in seconds."""
        table = f"{self.config.data.db_schema}.{self.config.data.snapshot_table_name}"
        query = f"SELECT MAX(event_timestamp) AS latest_ts FROM {table}"

        conn = get_db_conn(self.config)
        try:
            df = pd.read_sql_query(query, conn)
        finally:
            conn.close()

        latest_ts = df["latest_ts"].iloc[0]
        if pd.isna(latest_ts):
            return {"latest_timestamp": None, "staleness_seconds": None}

        latest_ts = pd.Timestamp(latest_ts)
        latest_ts = latest_ts.tz_convert("UTC") if latest_ts.tzinfo else latest_ts.tz_localize("UTC")
        staleness = (datetime.now(UTC) - latest_ts).total_seconds()
        return {
            "latest_timestamp": latest_ts.isoformat(),
            "staleness_seconds": round(staleness, 2),
        }

    def compute_snapshot_drift(self, lookback_days: int | None = None) -> pd.DataFrame:
        """Compare recent vs prior window snapshot distributions."""
        lookback_days = lookback_days or self.config.feast_parameters.lookback_days
        now = datetime.now(UTC)
        recent_start = now - timedelta(days=lookback_days)
        prior_start = now - timedelta(days=2 * lookback_days)
        prior_end = recent_start

        table = f"{self.config.data.db_schema}.{self.config.data.snapshot_table_name}"
        feature_cols = [
            col for col in self.config.features.stock_ohlcv_features
            if col not in {"symbol", "event_timestamp"}
        ]
        columns = ", ".join(["symbol", "event_timestamp"] + feature_cols)

        query = f"""
            SELECT {columns}
            FROM {table}
            WHERE event_timestamp >= %s AND event_timestamp < %s
        """
        conn = get_db_conn(self.config)
        try:
            recent_df = pd.read_sql_query(query, conn, params=(recent_start, now))
            prior_df = pd.read_sql_query(query, conn, params=(prior_start, prior_end))
        finally:
            conn.close()

        if recent_df.empty or prior_df.empty:
            self.logger.warning("Drift check skipped: insufficient data for comparison")
            return pd.DataFrame()

        drift_report = self.detect_feature_drift(prior_df, recent_df)
        return drift_report

    def materialize_features(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        auto_detect: bool = False,
        incremental: bool = False,
        allow_backfill: bool = False,
        feature_views: list[str] | None = None,
    ) -> str:
        lookback_days = self.config.feast_parameters.lookback_days
        max_backfill_days = self.config.feast_parameters.max_backfill_days

        if auto_detect:
            project_name = self.store.project
            feature_views = self.store.registry.list_feature_views(project_name)
            self.logger.info("Detected Feature Views: %s", [fv.name for fv in feature_views])

        if incremental:
            end_date = end_date or datetime.now(UTC)
            self.logger.info("Running incremental materialization up to %s", end_date)
            self.store.materialize_incremental(end_date, feature_views=feature_views)
        else:
            if not allow_backfill:
                raise ValueError("Full materialization requires allow_backfill=True")
            if not (start_date and end_date):
                raise ValueError("start_date and end_date are required for full materialization.")
            backfill_days = (end_date - start_date).days
            if backfill_days > max_backfill_days:
                raise ValueError(
                    f"Backfill window {backfill_days} days exceeds max_backfill_days={max_backfill_days}"
                )
            start_date = start_date or datetime.now(UTC) - timedelta(days=lookback_days)
            self.logger.info("Materializing features from %s → %s", start_date, end_date)
            self.store.materialize(start_date, end_date, feature_views=feature_views)

        self.logger.info("Feature materialization complete.")
        return "success"

    def run_production_materialization(self, incremental: bool = True) -> str:
        now = datetime.now(UTC)
        days_offset = self.config.feast_parameters.lookback_days

        if incremental:
            self.logger.info("Triggering incremental materialization up to %s", now)
            self.store.materialize_incremental(end_date=now)
        else:
            start_date = now - timedelta(days=days_offset)
            self.logger.info("Triggering full materialization: %s to %s", start_date, now)
            self.store.materialize(start_date, now)

        return "success"

    def get_current_schema_version(self) -> int | None:
        table = f"{self.config.data.db_schema}.feature_schema_versions"
        conn = get_db_conn(self.config)
        try:
            df = pd.read_sql_query(
                f"""
                SELECT schema_version
                FROM {table}
                WHERE table_name = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                conn,
                params=(self.config.data.offline_table_name,),
            )
        except Exception:
            self.logger.warning("Schema version table not found yet")
            return None
        finally:
            conn.close()

        if df.empty:
            return None
        return int(df["schema_version"].iloc[0])

    def record_materialization_run(
        self,
        started_at: datetime,
        ended_at: datetime,
        incremental: bool,
        status: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> str:
        run_id = str(uuid4())
        table = f"{self.config.data.db_schema}.feature_materialization_runs"
        schema_version = self.get_current_schema_version()

        conn = get_db_conn(self.config)
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    run_id UUID PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL,
                    ended_at TIMESTAMPTZ NOT NULL,
                    duration_seconds DOUBLE PRECISION NOT NULL,
                    incremental BOOLEAN NOT NULL,
                    status TEXT NOT NULL,
                    lookback_days INTEGER,
                    schema_version INTEGER,
                    window_start TIMESTAMPTZ,
                    window_end TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                """
            )

            duration = (ended_at - started_at).total_seconds()
            cur.execute(
                f"""
                INSERT INTO {table}
                (run_id, started_at, ended_at, duration_seconds, incremental, status,
                 lookback_days, schema_version, window_start, window_end)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    run_id,
                    started_at,
                    ended_at,
                    duration,
                    incremental,
                    status,
                    self.config.feast_parameters.lookback_days,
                    schema_version,
                    start_date,
                    end_date,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            self.logger.warning("Failed to record materialization run", exc_info=True)
        finally:
            conn.close()

        return run_id

    def list_feature_views(self) -> List[str]:
        return [fv.name for fv in self.store.list_feature_views()]

    def get_features_from_view(self, view_name: str) -> List[str]:
        fv = self.store.get_feature_view(view_name)
        return [f"{view_name}:{f.name}" for f in fv.features]
