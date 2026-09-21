import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from prefect import task, get_run_logger

from polyhorizon.features.src.parquet_to_postgres import (
    delta_seaweedfs_to_postgres_copy,
)
from polyhorizon.features.src.snapshot_days import update_training_snapshot
from polyhorizon.features.src.validate_snapshot_data import validate_snapshot_table
from polyhorizon.features.feast_ops.feature_manager import FeatureManager
from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.utils.metrics import FeatureMetricsPublisher

# Ingest Parquet → Postgres → Feast apply → snapshot → validate → materialize → retrain → deploy.

@task(name="publish-feature-release", retries=2, retry_delay_seconds=30)
def publish_feature_release(manifest: dict, apply_changes: bool = False) -> dict:
    from polyhorizon.features.src.publish_dataset import publish_dataset
    return publish_dataset(manifest, apply_changes)

@task(name="ingest_parquet_to_postgres", 
      retries=2, 
      retry_delay_seconds=[10, 30, 60])
def ingest_parquet_to_postgres() -> None:
    logger = get_run_logger()
    logger.info("Ingesting Parquet → Postgres (bronze → silver)")
    delta_seaweedfs_to_postgres_copy()
    logger.info("Ingestion complete")


# Prefer running in CI/CD when feature definitions change; can be toggled in Prefect flow.
@task(name="feast_apply", 
      retries=2, 
      retry_delay_seconds=[10, 30])
def run_feast_apply(repo_path: str | None = None) -> None:
    logger = get_run_logger()
    if repo_path is None:
        config = load_config()
        repo_path = config.paths.feast_repo_path
    logger.info(f"Running 'feast apply' CLI on repo: {repo_path}")

    # Validate repo path exists
    repo_path_obj = Path(repo_path)
    if not repo_path_obj.exists():
        raise ValueError(f"Feast repo path does not exist: {repo_path}")

    # Run feast apply CLI (idempotent and equivalent to programmatic in older versions)
    result = subprocess.run(
        ["feast", "apply"],
        cwd=str(repo_path_obj),
        capture_output=True,
        text=True,
    )
    logger.info(result)

    if result.returncode != 0:
        logger.error(f"feast apply failed:\n{result.stderr}")
        raise RuntimeError(f"feast apply failed with return code {result.returncode}")

    logger.info("feast apply succeeded")
    logger.debug(f"feast apply output:\n{result.stdout}")



@task(name="update_training_snapshot", 
      retries=2, 
      retry_delay_seconds=[10, 20, 40])
def create_training_snapshot() -> None:
    logger = get_run_logger()
    logger.info("Creating/updating training snapshot table")
    update_training_snapshot()
    logger.info("Snapshot table ready")
    


@task(name="validate_snapshot_data_quality", 
      retries=1, 
      retry_delay_seconds=[10, 20, 30])
def validate_snapshot() -> bool:
    logger = get_run_logger()
    logger.info("Running Great Expectations validation on snapshot table")
    result = validate_snapshot_table()
    if not result["success"]:
        raise ValueError("Snapshot table Data quality check failed")
    logger.info("Snapshot table Data quality Validation PASSED")
    return True



# This materialize command allows to backfill data into the online store for a given date range.
# It will typically be used in orchestrated jobs to backfill data for recent days.
# It will query the offline store for feature data in the given date range and push to online store.
# materialize-incremental can be set to True for subsequent runs to only process new data.
@task(name="materialize_to_redis", 
      retries=2, 
      retry_delay_seconds=[10, 30, 60])
def materialize_to_redis(days: int) -> str:
    logger = get_run_logger()
    config = load_config()
    metrics = FeatureMetricsPublisher(config.monitoring)
    
    # Initialize Feast Store
    fm = FeatureManager(config)

    if days > config.feast_parameters.max_backfill_days:
        raise ValueError(
            f"Requested backfill window {days} exceeds max_backfill_days={config.feast_parameters.max_backfill_days}"
        )
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)

    start_time = datetime.now(timezone.utc)
    logger.info(f"Materializing last {days} day(s) to Redis (online store)")
    success = True
    try:
        fm.materialize_features(
            start_date=start_date,
            end_date=end_date,
            incremental=False,
            auto_detect=False,
            allow_backfill=True,
        )
    except Exception:
        success = False
        raise
    finally:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        metrics.push_materialization_metrics(duration, incremental=False, success=success)
        fm.record_materialization_run(
            started_at=start_time,
            ended_at=datetime.now(timezone.utc),
            incremental=False,
            status="success" if success else "failed",
            start_date=start_date,
            end_date=end_date,
        )
        logger.info("Materialization to Redis completed in %.2fs", duration)


# Use this task in production orchestrations
@task(name="materialize_to_redis_prod", 
      retries=2, 
      retry_delay_seconds=[10, 30, 60])
def prod_materialize_to_redis() -> str:
    logger = get_run_logger()
    config = load_config()
    metrics = FeatureMetricsPublisher(config.monitoring)
    
    logger.info("Initializing Feast Store")
    fm = FeatureManager(config)
    
    start_time = datetime.now(timezone.utc)
    logger.info(
        "Incremental materialization up to now (lookback=%s days)",
        config.feast_parameters.lookback_days,
    )
    success = True
    try:
        fm.run_production_materialization(incremental=True)
    except Exception:
        success = False
        raise
    finally:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        metrics.push_materialization_metrics(duration, incremental=True, success=success)
        fm.record_materialization_run(
            started_at=start_time,
            ended_at=datetime.now(timezone.utc),
            incremental=True,
            status="success" if success else "failed",
        )
        logger.info("Materialization to Redis completed in %.2fs", duration)



# if __name__ == "__main__":
#     # For local testing only
#     ingest_parquet_to_postgres()
#     create_training_snapshot()
#     validate_snapshot()
#     materialize_to_redis(days=config.feast_parameters.lookback_days)
#     prod_materialize_to_redis()
