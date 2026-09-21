"""Validated, serialized dataset publication to PostgreSQL and Feast.

Postgres commits precede Feast; a durable pending record allows safe retry of
that same dataset and prevents another release from overtaking partial publication.
"""

import io
from datetime import datetime, timedelta, timezone

from deltalake import DeltaTable

from polyhorizon.core.dataset_release import DatasetRelease, canonical_path, validate_feature_frame
from polyhorizon.features.configs.settings import load_config
from polyhorizon.features.src.parquet_to_postgres import (
    _validate_identifier, validate_data_contract, add_missing_columns,
    _ensure_schema_version_table, _record_schema_version,
    _ensure_partitioned_table, _is_partitioned, _ensure_partitions_for_range,
)
from polyhorizon.features.utils.utility_helpers import get_db_conn


COLUMNS = ["symbol", "window_start", "window_end", "event_timestamp", "open", "high", "low", "close",
           "total_volume", "rolling_avg_close", "rolling_volatility_close", "dataset_version"]
DEFINITION = """symbol TEXT NOT NULL, window_start TIMESTAMPTZ NOT NULL,
window_end TIMESTAMPTZ NOT NULL, event_timestamp TIMESTAMPTZ NOT NULL,
open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION,
total_volume BIGINT, rolling_avg_close DOUBLE PRECISION,
rolling_volatility_close DOUBLE PRECISION, dataset_version TEXT,
created_at TIMESTAMPTZ DEFAULT now()
"""


def read_release(release, config):
    release.validate_root(config.bucket_details.feature_output_path)
    bucket = config.bucket_details
    options = {
        "AWS_ENDPOINT_URL": str(bucket.seaweedfs_s3_endpoint),
        "AWS_ACCESS_KEY_ID": bucket.seaweedfs_access_key,
        "AWS_SECRET_ACCESS_KEY": bucket.seaweedfs_secret_key,
        "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
        "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
    }
    frame = DeltaTable(canonical_path(release.feature_path), version=release.feature_version,
                       storage_options=options).to_pandas()
    validate_feature_frame(frame, release)
    validate_data_contract(frame, config)
    return frame


def publish_dataset(manifest: dict, apply_changes: bool = False, config=None) -> dict:
    release = DatasetRelease.model_validate(manifest)
    config = config or load_config()
    # Validation must finish before acquiring a connection or mutating any table.
    frame = read_release(release, config)
    schema = _validate_identifier(config.data.db_schema, "schema")
    offline = _validate_identifier(config.data.offline_table_name, "offline table")
    snapshot = _validate_identifier(config.data.snapshot_table_name, "snapshot table")
    ledger = f"{schema}.dataset_publications"
    dataset_id = str(release.dataset_id)
    conn = get_db_conn(config)
    try:
        with conn.cursor() as cur:
            # Match the legacy loader's lock so it cannot write during publication.
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"{schema}.{offline}",))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another dataset publication is active")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {ledger} (
                dataset_id TEXT PRIMARY KEY, manifest JSONB NOT NULL,
                status TEXT NOT NULL, max_event_time TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
            cur.execute(f"SELECT manifest, status FROM {ledger} WHERE dataset_id=%s", (dataset_id,))
            previous = cur.fetchone()
            if previous and previous[0] != release.model_dump(mode="json"):
                raise ValueError("Dataset ID was reused with a different manifest")
            if previous and previous[1] == "complete":
                return {"status": "success", "dataset_id": dataset_id, "already_published": True}
            cur.execute(f"SELECT dataset_id FROM {ledger} WHERE status != 'complete' AND dataset_id != %s", (dataset_id,))
            if cur.fetchone():
                raise RuntimeError("Retry the pending dataset before publishing another release")
            cur.execute(f"SELECT MAX(max_event_time) FROM {ledger}")
            latest = cur.fetchone()[0]
            if latest and latest > release.max_event_time:
                raise ValueError("Cannot publish an older dataset over a newer release")
            if not previous:
                if getattr(config.data, "enable_partitioning", False):
                    _ensure_partitioned_table(cur, schema, offline)
                for table in (offline, snapshot):
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({DEFINITION}, PRIMARY KEY(symbol, window_start))")
                    # Existing tables may predate versioned publication.
                    cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS dataset_version TEXT")
                    cur.execute(f"ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT now()")
                    add_missing_columns(cur, table, frame, schema)
                cur.execute(f"ALTER TABLE {schema}.{snapshot} ADD COLUMN IF NOT EXISTS window_start TIMESTAMPTZ")
                cur.execute(f"ALTER TABLE {schema}.{snapshot} ADD COLUMN IF NOT EXISTS window_end TIMESTAMPTZ")
                cur.execute(f"SELECT MAX(event_timestamp) FROM {schema}.{offline}")
                existing_end = cur.fetchone()[0]
                if existing_end and existing_end > release.max_event_time:
                    raise ValueError("Offline store contains newer data than this release")
                cur.execute(f"CREATE TEMP TABLE dataset_stage ({DEFINITION}) ON COMMIT DROP")
                frame = frame.copy()
                frame["dataset_version"] = dataset_id
                _ensure_schema_version_table(cur, schema)
                _record_schema_version(cur, schema, offline, frame)
                if getattr(config.data, "enable_partitioning", False) and _is_partitioned(cur, schema, offline):
                    _ensure_partitions_for_range(cur, schema, offline,
                        frame.window_start.min().to_pydatetime(), frame.window_start.max().to_pydatetime(),
                        config.data.partition_granularity)
                buffer = io.StringIO()
                frame[COLUMNS].to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S%z")
                buffer.seek(0)
                cur.copy_expert(f"COPY dataset_stage ({','.join(COLUMNS)}) FROM STDIN WITH (FORMAT csv)", buffer)
                updates = ','.join(f"{column}=EXCLUDED.{column}" for column in COLUMNS if column not in {"symbol", "window_start"}) + ',created_at=now()'
                cur.execute(f"""INSERT INTO {schema}.{offline} ({','.join(COLUMNS)})
                    SELECT {','.join(COLUMNS)} FROM dataset_stage
                    ON CONFLICT (symbol,window_start) DO UPDATE SET {updates}""")
                # Rebuild the training snapshot atomically using explicit columns
                # and event_timestamp (bar end), retaining lineage on every row.
                cur.execute(f"DELETE FROM {schema}.{snapshot}")
                cur.execute(f"""INSERT INTO {schema}.{snapshot} ({','.join(COLUMNS)})
                    SELECT {','.join(COLUMNS)} FROM dataset_stage WHERE event_timestamp >= %s""",
                    (release.max_event_time - timedelta(days=config.data.snapshot_days),))
                cur.execute(f"""CREATE TABLE IF NOT EXISTS {schema}.latest_training_snapshot (
                    snapshot_table TEXT PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT now(),
                    dataset_version TEXT)""")
                cur.execute(f"ALTER TABLE {schema}.latest_training_snapshot ADD COLUMN IF NOT EXISTS dataset_version TEXT")
                cur.execute(f"""INSERT INTO {schema}.latest_training_snapshot (snapshot_table,dataset_version)
                    VALUES (%s,%s) ON CONFLICT (snapshot_table) DO UPDATE
                    SET dataset_version=EXCLUDED.dataset_version, created_at=now()""", (snapshot, dataset_id))
                cur.execute(f"""INSERT INTO {ledger} (dataset_id,manifest,status,max_event_time)
                    VALUES (%s,%s::jsonb,'postgres_complete',%s)""",
                    (dataset_id, release.model_dump_json(), release.max_event_time))
                conn.commit()
            # Keep the session advisory lock across the network side effect.
            # A crash leaves postgres_complete; retries materialize the same bounds.
            if apply_changes:
                from polyhorizon.features.tasks.feast_tasks import run_feast_apply
                run_feast_apply.fn(config.paths.feast_repo_path)
            from polyhorizon.features.feast_ops.feature_manager import FeatureManager
            # Publication already validates product membership. It must not
            # depend on an unrelated universe refresh being available.
            manager = FeatureManager(config, load_symbols=False)
            end = release.max_event_time + timedelta(microseconds=1)
            start = max(release.min_event_time, end - timedelta(days=config.feast_parameters.lookback_days))
            started = datetime.now(timezone.utc)
            success = False
            try:
                manager.materialize_features(start_date=start, end_date=end, incremental=False,
                                             allow_backfill=True, feature_views=[config.project.feature_view])
                success = True
            finally:
                manager.record_materialization_run(
                    started_at=started, ended_at=datetime.now(timezone.utc), incremental=False,
                    status="success" if success else "failed", start_date=start, end_date=end,
                )
            cur.execute(f"UPDATE {ledger} SET status='complete', updated_at=now() WHERE dataset_id=%s", (dataset_id,))
            conn.commit()
        return {"status": "success", "dataset_id": dataset_id,
                "feature_version": release.feature_version, "rows": release.row_count}
    except Exception:
        conn.rollback()
        raise
    finally:
        # Closing the session releases the advisory lock even on failure.
        conn.close()
