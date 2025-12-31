"""
Raw: Cherre Transactions
========================
Extracts deed/transaction records from Cherre recorder_v2 table.
"""

from prefect import get_run_logger, task

from src.db import insert_batch, wrap_for_raw
from src.raw.cherre_client import paginated_query

TABLE_NAME = "raw.cherre_transactions"

# GraphQL fields to extract (excluding nested parties - those go to separate tables)
FIELDS = """
    recorder_id
    tax_assessor_id
    document_recorded_date
    document_instrument_date
    document_number_formatted
    document_type_code
    document_amount
    transfer_tax_amount
    arms_length_code
    inter_family_flag
    is_foreclosure_auction_sale
    is_quit_claim
    new_construction_flag
    resale_flag
    property_address
    property_city
    property_state
    property_zip
    cherre_ingest_datetime
"""

# Full fields including nested parties (for extracting grantors/grantees)
FIELDS_WITH_PARTIES = """
    recorder_id
    tax_assessor_id
    document_recorded_date
    document_instrument_date
    document_number_formatted
    document_type_code
    document_amount
    transfer_tax_amount
    arms_length_code
    inter_family_flag
    is_foreclosure_auction_sale
    is_quit_claim
    new_construction_flag
    resale_flag
    property_address
    property_city
    property_state
    property_zip
    cherre_ingest_datetime
    recorder_grantor_v2__recorder_id {
        cherre_recorder_grantor_pk
        grantor_name
        grantor_address
        grantor_entity_code
        grantor_first_name
        grantor_last_name
    }
    recorder_grantee_v2__recorder_id {
        cherre_recorder_grantee_pk
        grantee_name
        grantee_address
        grantee_entity_code
        grantee_first_name
        grantee_last_name
    }
"""


def extract(start_date: str, end_date: str, include_parties: bool = True) -> list[dict]:
    """
    Extract transactions from Cherre for a date range.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        include_parties: Include nested grantor/grantee data

    Returns:
        List of transaction records
    """
    fields = FIELDS_WITH_PARTIES if include_parties else FIELDS

    transactions = paginated_query(
        table_name="recorder_v2",
        fields=fields,
        where_clause=f'where: {{document_recorded_date: {{_gte: "{start_date}", _lte: "{end_date}"}}}}',
        order_by="order_by: {document_recorded_date: asc}",
    )

    return transactions


def flatten_for_load(transactions: list[dict]) -> list[dict]:
    """
    Flatten transaction records for raw table (remove nested parties).

    Args:
        transactions: Raw transactions with nested parties

    Returns:
        Flattened transaction records
    """
    flattened = []
    for txn in transactions:
        record = {k: v for k, v in txn.items() if not k.startswith("recorder_g")}
        # Add party counts
        record["grantor_count"] = len(txn.get("recorder_grantor_v2__recorder_id", []))
        record["grantee_count"] = len(txn.get("recorder_grantee_v2__recorder_id", []))
        flattened.append(record)
    return flattened


@task(name="sync-cherre-transactions", retries=2, retry_delay_seconds=30)
def sync(start_date: str, end_date: str) -> dict:
    """
    Extract and load transactions.

    Returns dict with transaction records (for downstream use) and count.
    """
    logger = get_run_logger()
    logger.info(f"📊 Syncing {TABLE_NAME} for {start_date} to {end_date}")

    # Extract with parties (we need them for grantor/grantee tables)
    raw_transactions = extract(start_date, end_date, include_parties=True)

    # Flatten for storage (strip nested parties, add counts)
    transactions = flatten_for_load(raw_transactions)

    # Wrap for raw table (JSONB pattern)
    wrapped = wrap_for_raw(transactions, id_field="recorder_id")
    count = insert_batch(TABLE_NAME, wrapped)

    logger.info(f"✅ Synced {count:,} transactions")

    # Return raw transactions (with parties) for downstream modules
    return {"raw_transactions": raw_transactions, "count": count}
