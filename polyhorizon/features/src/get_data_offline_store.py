from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from polyhorizon.features.configs.settings import Config, load_config


def load_training_snapshot(
    config: Optional[Config] = None,
    schema: Optional[str] = None,
    snapshot_table: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load the latest training snapshot (rolling table).
    """

    resolved = config or load_config()
    schema = schema or resolved.data.db_schema
    snapshot_table = snapshot_table or resolved.data.snapshot_table_name

    params = resolved.connection_parameters
    engine = create_engine(
        f"postgresql+psycopg2://{params.user}:{params.password}@{params.host}:{params.port}/{params.database}",
        pool_pre_ping=True,
        future=True,
    )

    sql = f"""
        SELECT
            symbol,
            event_timestamp,
            open,
            high,
            low,
            close,
            total_volume,
            rolling_avg_close,
            rolling_volatility_close
        FROM {schema}.{snapshot_table}
        ORDER BY symbol, event_timestamp
    """

    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)

    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
    return df



if __name__ == "__main__":
    df_train = load_training_snapshot()
    print(df_train.shape)
    print(df_train.head(20))
