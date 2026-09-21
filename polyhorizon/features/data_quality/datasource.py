from typing import Any

def get_or_create_postgres_datasource(
    context: str | Any,
    name: str,
    connection_string: str,
):
    """
    Idempotently create or fetch a Postgres datasource.
    """
    existing = [ds.name for ds in context.list_datasources()]
    if name in existing:
        return context.data_sources.get(name)

    return context.data_sources.add_postgres(
        name=name,
        connection_string=connection_string,
    )