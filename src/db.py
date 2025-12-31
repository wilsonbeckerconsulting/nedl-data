"""
Database Client
===============
Supabase client helpers for raw, app, and analytics schemas.

Environment-aware routing:
    ENVIRONMENT=dev  → all tables go to 'dev' schema
    ENVIRONMENT=prod → tables go to their defined schema (raw, analytics, app)
"""

from functools import lru_cache

from src.config import get_settings


@lru_cache
def get_supabase_client():
    """
    Get Supabase client.

    Returns:
        Supabase client instance
    """
    # Import here to avoid requiring supabase for all operations
    from supabase import create_client

    settings = get_settings()
    return create_client(settings.supabase_url, settings.supabase_service_key)


def _resolve_table(table_name: str) -> tuple[str, str]:
    """
    Resolve table name to (schema, table) based on environment.

    In dev: all tables route to 'dev' schema with original table name
    In prod: tables use their defined schema (raw, analytics, etc.)

    Args:
        table_name: Full table name (e.g., 'raw.cherre_transactions')

    Returns:
        Tuple of (schema, table)

    Examples:
        ENVIRONMENT=prod: 'raw.cherre_transactions' → ('raw', 'cherre_transactions')
        ENVIRONMENT=dev:  'raw.cherre_transactions' → ('dev', 'raw_cherre_transactions')
    """
    settings = get_settings()

    # Parse schema.table
    if "." in table_name:
        schema, table = table_name.split(".", 1)
    else:
        schema, table = "public", table_name

    # In dev, route everything to 'dev' schema with schema prefix on table name
    if settings.environment == "dev":
        return "dev", f"{schema}_{table}"

    return schema, table


def insert_batch(
    table_name: str,
    records: list[dict],
    batch_size: int = 1000,
) -> int:
    """
    Insert records in batches (append-only for raw tables).

    Args:
        table_name: Full table name (e.g., 'raw.cherre_transactions')
        records: List of records to insert
        batch_size: Records per batch

    Returns:
        Number of records inserted
    """
    if not records:
        return 0

    client = get_supabase_client()
    schema, table = _resolve_table(table_name)
    total = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        client.schema(schema).table(table).insert(batch).execute()
        total += len(batch)

    return total


def upsert_batch(
    table_name: str,
    records: list[dict],
    on_conflict: str = "id",
    batch_size: int = 1000,
) -> int:
    """
    Upsert records in batches (for app/analytics tables).

    Args:
        table_name: Full table name (e.g., 'analytics.dim_property')
        records: List of records to upsert
        on_conflict: Column(s) to use for conflict resolution
        batch_size: Records per batch

    Returns:
        Number of records upserted
    """
    if not records:
        return 0

    client = get_supabase_client()
    schema, table = _resolve_table(table_name)
    total = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        client.schema(schema).table(table).upsert(batch, on_conflict=on_conflict).execute()
        total += len(batch)

    return total


def read_table(
    table_name: str,
    columns: str = "*",
    filters: dict | None = None,
    limit: int | None = None,
) -> list[dict]:
    """
    Read records from a table.

    Args:
        table_name: Full table name (e.g., 'raw.cherre_transactions')
        columns: Columns to select (default: all)
        filters: Optional filters as {column: value}
        limit: Optional row limit

    Returns:
        List of records
    """
    client = get_supabase_client()
    schema, table = _resolve_table(table_name)
    query = client.schema(schema).table(table).select(columns)

    if filters:
        for col, val in filters.items():
            query = query.eq(col, val)

    if limit:
        query = query.limit(limit)

    result = query.execute()
    return list(result.data)  # type: ignore[arg-type]


def wrap_for_raw(
    records: list[dict],
    id_field: str,
) -> list[dict]:
    """
    Wrap records for raw table insert (JSONB data column pattern).

    Raw tables have: id, <id_field>, data JSONB, extracted_at

    Args:
        records: Raw API records
        id_field: Field to extract as indexed column (e.g., 'recorder_id')

    Returns:
        List of {<id_field>: ..., data: {...}} ready for insert
    """
    wrapped = []
    for record in records:
        wrapped.append(
            {
                id_field: record.get(id_field),
                "data": record,  # Full record as JSONB
            }
        )
    return wrapped
