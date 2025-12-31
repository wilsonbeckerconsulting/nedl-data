"""
Database Client
===============
Supabase client helpers for raw, app, and analytics schemas.
"""

from datetime import datetime
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
    total = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        client.table(table_name).insert(batch).execute()
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
    total = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
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
        table_name: Full table name
        columns: Columns to select (default: all)
        filters: Optional filters as {column: value}
        limit: Optional row limit

    Returns:
        List of records
    """
    client = get_supabase_client()
    query = client.table(table_name).select(columns)

    if filters:
        for col, val in filters.items():
            query = query.eq(col, val)

    if limit:
        query = query.limit(limit)

    result = query.execute()
    return result.data


def add_metadata(
    records: list[dict],
    batch_id: str,
    extracted_at: datetime | None = None,
    source_start: str | None = None,
    source_end: str | None = None,
) -> list[dict]:
    """
    Add metadata columns to records for raw tables.

    Args:
        records: Records to augment
        batch_id: Unique batch/run ID
        extracted_at: Extraction timestamp (defaults to now)
        source_start: Source query start date
        source_end: Source query end date

    Returns:
        Records with metadata columns added
    """
    if extracted_at is None:
        extracted_at = datetime.utcnow()

    for record in records:
        record["_extracted_at"] = extracted_at.isoformat()
        record["_batch_id"] = batch_id
        if source_start:
            record["_source_query_start"] = source_start
        if source_end:
            record["_source_query_end"] = source_end

    return records
