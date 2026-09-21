import tempfile
import pandas as pd
from typing import Tuple
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import mlflow
import mlflow.data
from polyhorizon.core.product_symbols import filter_product_frame
from mlflow.data.pandas_dataset import PandasDataset
from mlflow.data.dataset_source import DatasetSource

from polyhorizon.training.configs.settings import Config # check for errors
from polyhorizon.training.utils.logger import get_logger
logger = get_logger(name="PostgresDataLoader")


class PostgresDataLoader:
    """
    Clean, reusable, scalable reader for Postgres tables.
    Includes MLflow dataset artifact logging.
    """
    def __init__(self, config: Config):
        # Everything comes from the centralized config
        self.config = config
        self.conn_params = self.config.connection_parameters
        self.schema = self.config.data.db_schema
        self.snapshot_table_name = self.config.data.snapshot_table_name
        self.fully_qualified_table = f"{self.schema}.{self.snapshot_table_name}"

    @lru_cache(maxsize=1)
    def get_engine(self):
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=self.conn_params.user,
            password=self.conn_params.password,
            host=self.conn_params.host,
            port=self.conn_params.port,
            database=self.conn_params.database,
        )
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=5,
            pool_recycle=1800,
            connect_args={
                "connect_timeout": self.conn_params.connect_timeout,
                "application_name": "polyhorizon-training",
                "options": "-c statement_timeout=900000 -c idle_in_transaction_session_timeout=60000",
            },
        )
    
    
    # -----------------------
    # SQL Builder
    # -----------------------
    def build_query(self) -> str:
        return f"""
        SELECT *
        FROM {self.fully_qualified_table}
        WHERE event_timestamp >= NOW() - INTERVAL '{self.config.data.min_days_required} days';
        """
    
    # -----------------------
    # Fetch Data
    # -----------------------
    def fetch(self) -> pd.DataFrame:
        query = self.build_query()
        return filter_product_frame(pd.read_sql(query, self.get_engine()))
    
    
    # Fetch dataset and log to mlflow
    def fetch_data(self, track_mlflow: bool = True) -> pd.DataFrame:
        query = self.build_query()
        df = filter_product_frame(pd.read_sql(query, self.get_engine()))

        if track_mlflow and not df.empty:
            self._log_to_mlflow(df, query)
        
        return df

    def _log_to_mlflow(self, df, query):
        dataset = mlflow.data.from_pandas(df, source=query, name=f"{self.snapshot_table_name}_snapshot")
        mlflow.log_input(dataset, context="training_source")


    def fetch_and_track(self) -> pd.DataFrame:
        query = self.build_query()
        
        # Fetch data
        df = filter_product_frame(pd.read_sql(query, self.get_engine()))
        
        if df.empty:
            logger.warning("Fetched DataFrame is empty — skipping dataset logging")
            return df

        # Construct MLflow Dataset (The "Source" Tracking)
        # This records WHERE the data came from automatically
        dataset_source = DatasetSource.load(self.get_engine().url)
        dataset: PandasDataset = mlflow.data.from_pandas(
            df, 
            source=query, # Tracks the SQL query
            name=f"{self.snapshot_table_name}_snapshot",
            digest=None,
        )
        
        # Add custom tags for query and time window
        dataset = dataset._with_tags({
            "table": self.fully_qualified_table,
            "days_back": str(self.config.data.min_days_required),
            "sql_query": query.strip(),
            "row_count": str(len(df)),
        })

        # Log to MLflow
        # This shows up in a dedicated "Datasets" tab in the UI
        mlflow.log_input(dataset, context="training_dataset_source")
        logger.info(
                f"Logged dataset  from '{dataset_source}' to MLflow (rows={len(df)})"
            )
        
        return df

    # -----------------------
    # Log Dataset to MLflow
    # -----------------------
    def log_dataset(self, df: pd.DataFrame) -> str:
        """
        Saves df as parquet to MLflow artifact store + temp local parquet.
        Returns path of artifact inside MLflow.
        """

        # Local temp parquet (useful for debugging)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            temp_path = tmp.name
            df.to_parquet(temp_path, index=False)

        # Log to MLflow
        artifact_path = f"datasets/{self.snapshot_table_name}_snapshot"
        mlflow.log_artifact(temp_path, artifact_path=artifact_path)

        return artifact_path

    # ----------------------
    # DRIFT DETECTION
    # ----------------------
    def fetch_drift_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fetches Reference (6m - 1m) and Current (last 30d) datasets."""
        ref_days = self.config.drift.reference_days
        cur_days = self.config.drift.current_days
        
        query_ref = f"""
            SELECT * FROM {self.fully_qualified_table}
            WHERE event_timestamp >= NOW() - INTERVAL '{ref_days} days'
                AND event_timestamp < NOW() - INTERVAL '{cur_days} days';
        """
        query_cur = f"""
            SELECT * FROM {self.fully_qualified_table}
            WHERE event_timestamp >= NOW() - INTERVAL '{cur_days} days';
        """
        
        engine = self.get_engine()
        df_ref = filter_product_frame(pd.read_sql(query_ref, engine))
        df_cur = filter_product_frame(pd.read_sql(query_cur, engine))
        
        return df_ref, df_cur



