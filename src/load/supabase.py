"""
Supabase Loader
===============
Loads dimensional model data to Supabase.
"""

from prefect import get_run_logger, task

from src.config import get_settings


@task(name="load-to-supabase")
def load_to_supabase(
    dim_property: list[dict],
    dim_entity: list[dict],
    dim_entity_identifier: list[dict],
    fact_transaction: list[dict],
    bridge_transaction_party: list[dict],
    bridge_property_owner: list[dict],
) -> dict:
    """
    Load all tables to Supabase.

    Args:
        All dimension and fact tables

    Returns:
        Dict with row counts loaded per table
    """
    logger = get_run_logger()
    settings = get_settings()

    logger.info(f"📤 Loading to Supabase: {settings.supabase_url}")

    # TODO: Implement actual Supabase loading
    # For now, just log what would be loaded

    tables = {
        "dim_property": dim_property,
        "dim_entity": dim_entity,
        "dim_entity_identifier": dim_entity_identifier,
        "fact_transaction": fact_transaction,
        "bridge_transaction_party": bridge_transaction_party,
        "bridge_property_owner": bridge_property_owner,
    }

    result = {}
    for table_name, data in tables.items():
        result[table_name] = len(data)
        logger.info(f"   {table_name}: {len(data):,} rows")

    # Placeholder for actual implementation:
    #
    # from supabase import create_client
    #
    # supabase = create_client(settings.supabase_url, settings.supabase_service_key)
    #
    # for table_name, data in tables.items():
    #     if data:
    #         # Upsert in batches
    #         batch_size = 1000
    #         for i in range(0, len(data), batch_size):
    #             batch = data[i:i+batch_size]
    #             supabase.table(f"analytics.{table_name}").upsert(batch).execute()
    #         logger.info(f"   ✅ {table_name}: {len(data):,} rows upserted")

    total = sum(result.values())
    logger.info(f"✅ Total: {total:,} rows")

    return result
