def get_table_asset(
    datasource: str,
    *,
    asset_name: str,
    schema_name: str,
    table_name: str,
):
    """
    Idempotently create or fetch a table asset.
    """
    existing = [a.name for a in datasource.assets]
    if asset_name in existing:
        return datasource.get_asset(asset_name)

    return datasource.add_table_asset(
        name=asset_name,
        schema_name=schema_name,
        table_name=table_name,
    )


def get_full_table_batch(asset):
    """
    Returns a full-table batch.
    """
    batch_def = asset.add_batch_definition_whole_table(
        name="full_table"
    )
    return batch_def.get_batch()
