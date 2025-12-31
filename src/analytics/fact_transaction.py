"""
Analytics: fact_transaction
===========================
Transaction fact table from raw.cherre_transactions.
"""

from prefect import get_run_logger, task

from src.db import read_table, upsert_batch

TABLE_NAME = "analytics.fact_transaction"
SOURCE_TABLES = ["raw.cherre_transactions", "analytics.dim_property"]


def read_source() -> tuple[list[dict], dict]:
    """
    Read transactions from raw and property lookup from analytics.

    Returns:
        Tuple of (transactions, property_key_lookup)
    """
    transactions = read_table("raw.cherre_transactions")

    # Build property lookup from dim_property
    dim_property = read_table("analytics.dim_property", columns="tax_assessor_id, property_key")
    property_key_lookup = {
        p["tax_assessor_id"]: p["property_key"] for p in dim_property if p.get("tax_assessor_id")
    }

    return transactions, property_key_lookup


def transform(transactions: list[dict], property_key_lookup: dict) -> list[dict]:
    """
    Transform transactions into fact_transaction.

    Args:
        transactions: Raw transaction records
        property_key_lookup: tax_assessor_id → property_key mapping

    Returns:
        fact_transaction records
    """
    fact_transaction = []
    transaction_key_counter = 1

    for txn in transactions:
        # Classify transaction
        is_sale = False
        transaction_category = "OTHER"

        if (
            txn.get("arms_length_code")
            and txn.get("document_amount")
            and txn["document_amount"] > 0
        ):
            is_sale = True
            transaction_category = "SALE"
        elif txn.get("document_amount") == 0:
            transaction_category = "MORTGAGE"

        # Get property_key
        property_key = None
        if txn.get("tax_assessor_id"):
            property_key = property_key_lookup.get(txn["tax_assessor_id"])

        fact_transaction.append(
            {
                "transaction_key": transaction_key_counter,
                "recorder_id": txn.get("recorder_id"),
                "property_key": property_key,
                "transaction_date": txn.get("document_recorded_date"),
                "instrument_date": txn.get("document_instrument_date"),
                "document_number": txn.get("document_number_formatted"),
                "document_type_code": txn.get("document_type_code"),
                "document_amount": txn.get("document_amount"),
                "transfer_tax_amount": txn.get("transfer_tax_amount"),
                "transaction_category": transaction_category,
                "is_sale": is_sale,
                "is_arms_length": bool(txn.get("arms_length_code")),
                "is_inter_family": bool(txn.get("inter_family_flag")),
                "is_foreclosure": bool(txn.get("is_foreclosure_auction_sale")),
                "is_quit_claim": bool(txn.get("is_quit_claim")),
                "is_new_construction": bool(txn.get("new_construction_flag")),
                "is_resale": bool(txn.get("resale_flag")),
                "grantor_count": txn.get("grantor_count", 0),
                "grantee_count": txn.get("grantee_count", 0),
                "property_address": txn.get("property_address"),
                "property_city": txn.get("property_city"),
                "property_state": txn.get("property_state"),
                "property_zip": txn.get("property_zip"),
                "source_system": "cherre",
                "cherre_ingest_datetime": txn.get("cherre_ingest_datetime"),
            }
        )
        transaction_key_counter += 1

    return fact_transaction


def load(records: list[dict]) -> int:
    """
    Upsert records to analytics.fact_transaction.

    Args:
        records: fact_transaction records

    Returns:
        Number of records upserted
    """
    logger = get_run_logger()

    count = upsert_batch(TABLE_NAME, records, on_conflict="recorder_id")
    logger.info(f"✅ Loaded {count:,} records to {TABLE_NAME}")

    return count


@task(name="build-fact-transaction")
def build() -> int:
    """
    Full pipeline: read → transform → load.

    Returns:
        Number of records loaded
    """
    logger = get_run_logger()
    logger.info(f"🔨 Building {TABLE_NAME}")

    transactions, property_lookup = read_source()
    logger.info(f"   Read {len(transactions):,} transactions from raw")
    logger.info(f"   Property lookup has {len(property_lookup):,} entries")

    records = transform(transactions, property_lookup)
    logger.info(f"   Transformed to {len(records):,} fact_transaction records")

    count = load(records)

    logger.info(f"✅ Built {TABLE_NAME}: {count:,} records")
    return count
