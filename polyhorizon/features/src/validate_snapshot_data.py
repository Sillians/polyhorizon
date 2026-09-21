from polyhorizon.features.data_quality.context import get_ge_context
from polyhorizon.features.data_quality.datasource import get_or_create_postgres_datasource
from polyhorizon.features.data_quality.assets import get_table_asset,get_full_table_batch
from polyhorizon.features.data_quality.expectations import (
    snapshot_table_expectations,
    SNAPSHOT_EXPECTATION_SUITE_VERSION,
)
from polyhorizon.features.data_quality.validators import validate_batch

from polyhorizon.features.configs.settings import Config, load_config
from polyhorizon.features.utils.logger import get_logger
logger = get_logger("ValidateSnapshotTable")


def validate_snapshot_table(config: Config | None = None):
    resolved = config or load_config()
    context = get_ge_context()
    
    # Access the pre-validated string from your single source of truth
    connection_string = resolved.connection_parameters.connection_string

    datasource = get_or_create_postgres_datasource(
        context,
        name="my_snapshot_datasource",
        connection_string=connection_string,
    )

    asset = get_table_asset(
        datasource,
        asset_name="SNAPSHOT_TABLE_ASSET",
        schema_name=resolved.data.db_schema,
        table_name=resolved.data.snapshot_table_name,
    )

    batch = get_full_table_batch(asset)

    success, results = validate_batch(
        batch,
        snapshot_table_expectations(),
    )

    if success:
        logger.info(
            "Snapshot table validation PASSED (suite version: %s)",
            SNAPSHOT_EXPECTATION_SUITE_VERSION,
        )
    else:
        logger.error(
            "Snapshot table validation FAILED (suite version: %s)",
            SNAPSHOT_EXPECTATION_SUITE_VERSION,
        )

    return {"success": success}


if __name__ == "__main__":
    validate_snapshot_table()
