"""
Raw: Cherre Grantees
====================
Flattens grantee (buyer) records from transactions.
"""

from prefect import get_run_logger, task

from src.db import add_metadata, insert_batch

TABLE_NAME = "raw.cherre_grantees"


def extract_from_transactions(transactions: list[dict]) -> list[dict]:
    """
    Extract and flatten grantee records from transactions.

    Args:
        transactions: Raw transactions with nested recorder_grantee_v2__recorder_id

    Returns:
        Flattened grantee records with transaction FK
    """
    grantees = []

    for txn in transactions:
        recorder_id = txn.get("recorder_id")
        nested_grantees = txn.get("recorder_grantee_v2__recorder_id", [])

        for grantee in nested_grantees:
            grantees.append(
                {
                    "recorder_id": recorder_id,
                    "cherre_recorder_grantee_pk": grantee.get("cherre_recorder_grantee_pk"),
                    "grantee_name": grantee.get("grantee_name"),
                    "grantee_address": grantee.get("grantee_address"),
                    "grantee_entity_code": grantee.get("grantee_entity_code"),
                    "grantee_first_name": grantee.get("grantee_first_name"),
                    "grantee_last_name": grantee.get("grantee_last_name"),
                }
            )

    return grantees


def load(records: list[dict], batch_id: str) -> int:
    """
    Load grantee records to raw.cherre_grantees.

    Args:
        records: Grantee records
        batch_id: Unique batch ID

    Returns:
        Number of records inserted
    """
    logger = get_run_logger()

    records = add_metadata(records, batch_id)
    count = insert_batch(TABLE_NAME, records)
    logger.info(f"✅ Loaded {count:,} records to {TABLE_NAME}")

    return count


@task(name="sync-cherre-grantees")
def sync(transactions: list[dict], batch_id: str) -> int:
    """
    Extract grantees from transactions and load.

    Args:
        transactions: Raw transactions with nested parties
        batch_id: Unique batch ID

    Returns:
        Number of records loaded
    """
    logger = get_run_logger()
    logger.info(f"📊 Syncing {TABLE_NAME}")

    grantees = extract_from_transactions(transactions)
    grantees = add_metadata(grantees, batch_id)
    count = insert_batch(TABLE_NAME, grantees)

    logger.info(f"✅ Synced {count:,} grantees")
    return count
