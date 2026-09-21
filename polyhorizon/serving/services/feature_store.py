from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd
import psycopg2
from feast import FeatureStore

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.utils.logger import get_logger


class FeatureStoreClient:
    def __init__(self, config: Config):
        self.config = config
        self.logger = get_logger("FeatureStore")
        repo_path = Path(self.config.feast.repo_path)
        if not repo_path.exists():
            raise FileNotFoundError(f"Feast repo not found: {repo_path}")
        store_yaml = Path(self.config.feast.feature_store_yaml)
        if not store_yaml.exists():
            self.logger.warning("Feast store config not found at %s", store_yaml)
        self.store = FeatureStore(repo_path=str(repo_path))

    def get_online_features(self, symbols: List[str]) -> pd.DataFrame:
        feature_refs = self.config.feast.feature_refs
        if not feature_refs:
            view = self.store.get_feature_view(self.config.feast.feature_view)
            feature_refs = [f"{view.name}:{feat.name}" for feat in view.features]

        entity_rows = [{"symbol": sym} for sym in symbols]
        response = self.store.get_online_features(
            features=feature_refs,
            entity_rows=entity_rows,
            include_timestamp=True,
        )
        return response.to_df()

    def get_offline_history(self, symbol: str, limit: int) -> pd.DataFrame:
        cfg = self.config
        params = cfg.connection_parameters.model_dump(exclude_none=True)
        table = f"{cfg.offline_store.db_schema}.{cfg.offline_store.offline_table_name}"
        time_field = cfg.inference.time_field

        query = f"""
            SELECT *
            FROM {table}
            WHERE symbol = %s
            ORDER BY {time_field} DESC
            LIMIT %s;
        """

        conn = psycopg2.connect(**params)
        try:
            df = pd.read_sql_query(query, conn, params=(symbol, limit))
        finally:
            conn.close()

        if df.empty:
            return df

        df = df.sort_values(time_field).reset_index(drop=True)
        return df

    def get_feature_window(self, symbol: str, limit: int) -> pd.DataFrame:
        history = self.get_offline_history(symbol, limit)
        if history.empty:
            return history

        try:
            online = self.get_online_features([symbol])
        except Exception:
            self.logger.warning("Online features unavailable; using offline history", exc_info=True)
            return history

        time_field = self.config.inference.time_field
        if online.empty:
            return history

        combined = pd.concat([history, online], ignore_index=True)
        if time_field in combined.columns:
            combined = combined.sort_values(time_field).drop_duplicates(
                subset=[time_field, "symbol"], keep="last"
            )
        return combined.reset_index(drop=True)
