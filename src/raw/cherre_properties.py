"""
Raw: Cherre Properties
======================
Extracts property records from Cherre tax_assessor_v2 table.
"""

import json

from prefect import get_run_logger, task

from src.config import get_settings
from src.db import add_metadata, insert_batch
from src.raw.cherre_client import query_cherre

TABLE_NAME = "raw.cherre_properties"

FIELDS = """
    tax_assessor_id
    assessor_parcel_number_raw
    address
    city
    state
    zip
    situs_county
    property_use_standardized_code
    year_built
    building_sq_ft
    lot_size_sq_ft
    units_count
    assessed_value_total
    market_value_total
    latitude
    longitude
"""


def extract(tax_assessor_ids: list[str], mf_only: bool = True) -> list[dict]:
    """
    Extract property records from Cherre.

    Args:
        tax_assessor_ids: List of tax assessor IDs to fetch
        mf_only: Filter to multifamily properties only

    Returns:
        List of property records
    """
    settings = get_settings()
    batch_size = settings.batch_size
    mf_codes = settings.mf_codes

    properties = []

    for i in range(0, len(tax_assessor_ids), batch_size):
        batch = tax_assessor_ids[i : i + batch_size]

        where_clause = f"tax_assessor_id: {{_in: {json.dumps(batch)}}}"
        if mf_only:
            where_clause += f", property_use_standardized_code: {{_in: {json.dumps(mf_codes)}}}"

        query = f"""
        query {{
            tax_assessor_v2(
                where: {{{where_clause}}}
            ) {{
                {FIELDS}
            }}
        }}
        """

        result = query_cherre(query)
        if result and "data" in result:
            properties.extend(result["data"]["tax_assessor_v2"])

    return properties


def load(records: list[dict], batch_id: str) -> int:
    """
    Load property records to raw.cherre_properties.

    Args:
        records: Property records
        batch_id: Unique batch ID

    Returns:
        Number of records inserted
    """
    logger = get_run_logger()

    records = add_metadata(records, batch_id)
    count = insert_batch(TABLE_NAME, records)
    logger.info(f"✅ Loaded {count:,} records to {TABLE_NAME}")

    return count


@task(name="sync-cherre-properties", retries=2, retry_delay_seconds=30)
def sync(transactions: list[dict], batch_id: str) -> dict:
    """
    Extract properties for transactions and load.

    Args:
        transactions: Raw transactions (to get tax_assessor_ids)
        batch_id: Unique batch ID

    Returns:
        Dict with properties and count
    """
    logger = get_run_logger()
    logger.info(f"📊 Syncing {TABLE_NAME}")

    # Get unique tax_assessor_ids from transactions
    tax_ids = list(set(t["tax_assessor_id"] for t in transactions if t.get("tax_assessor_id")))
    logger.info(f"   Found {len(tax_ids):,} unique property IDs")

    # Extract properties
    properties = extract(tax_ids, mf_only=True)
    logger.info(f"   Extracted {len(properties):,} MULTIFAMILY properties")

    # Add metadata and load
    properties = add_metadata(properties, batch_id)
    count = insert_batch(TABLE_NAME, properties)

    logger.info(f"✅ Synced {count:,} properties")
    return {"properties": properties, "count": count}
