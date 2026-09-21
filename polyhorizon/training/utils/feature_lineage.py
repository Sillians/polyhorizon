from __future__ import annotations

from typing import Dict, Any

import mlflow
from sqlalchemy import create_engine, text

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.utils.logger import get_logger

logger = get_logger("feature_lineage")


def _get_engine(config: Config):
    params = config.connection_parameters
    url = (
        f"postgresql+psycopg2://{params.user}:{params.password}"
        f"@{params.host}:{params.port}/{params.database}"
    )
    return create_engine(url, pool_pre_ping=True)


def fetch_feature_lineage(config: Config) -> Dict[str, Any]:
    """Fetch latest schema and materialization metadata from feature store tables."""
    schema = config.data.db_schema
    offline_table = config.data.offline_table_name

    lineage: Dict[str, Any] = {}
    engine = _get_engine(config)

    with engine.connect() as conn:
        try:
            schema_row = conn.execute(
                text(
                    f"""
                    SELECT schema_version, schema_hash, created_at
                    FROM {schema}.feature_schema_versions
                    WHERE table_name = :table
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"table": offline_table},
            ).fetchone()

            if schema_row:
                lineage.update(
                    {
                        "feature_schema_version": schema_row[0],
                        "feature_schema_hash": schema_row[1],
                        "feature_schema_recorded_at": schema_row[2],
                    }
                )
        except Exception:
            logger.warning("Feature schema version table not found or unreadable", exc_info=True)

        try:
            run_row = conn.execute(
                text(
                    f"""
                    SELECT run_id, started_at, ended_at, duration_seconds, incremental,
                           status, lookback_days, schema_version, window_start, window_end
                    FROM {schema}.feature_materialization_runs
                    ORDER BY ended_at DESC
                    LIMIT 1
                    """
                )
            ).fetchone()

            if run_row:
                lineage.update(
                    {
                        "feature_materialization_run_id": str(run_row[0]),
                        "feature_materialization_started_at": run_row[1],
                        "feature_materialization_ended_at": run_row[2],
                        "feature_materialization_duration_seconds": run_row[3],
                        "feature_materialization_incremental": run_row[4],
                        "feature_materialization_status": run_row[5],
                        "feature_materialization_lookback_days": run_row[6],
                        "feature_materialization_schema_version": run_row[7],
                        "feature_materialization_window_start": run_row[8],
                        "feature_materialization_window_end": run_row[9],
                    }
                )
        except Exception:
            logger.warning("Feature materialization run table not found or unreadable", exc_info=True)

    return lineage


def log_feature_lineage_mlflow(lineage: Dict[str, Any]) -> None:
    if not lineage:
        return

    for key in [
        "feature_schema_version",
        "feature_schema_hash",
        "feature_schema_recorded_at",
        "feature_materialization_run_id",
        "feature_materialization_status",
        "feature_materialization_incremental",
        "feature_materialization_window_start",
        "feature_materialization_window_end",
    ]:
        value = lineage.get(key)
        if value is not None:
            mlflow.set_tag(key, str(value))

    duration = lineage.get("feature_materialization_duration_seconds")
    if duration is not None:
        mlflow.log_metric("feature_materialization_duration_seconds", float(duration))
