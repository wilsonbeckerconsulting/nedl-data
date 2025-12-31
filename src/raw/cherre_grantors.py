"""
Raw: Cherre Grantors
====================
Flattens grantor (seller) records from transactions.
"""

from prefect import get_run_logger, task

from src.db import insert_batch, wrap_for_raw

TABLE_NAME = "raw.cherre_grantors"


def extract_from_transactions(transactions: list[dict]) -> list[dict]:
    """
    Extract and flatten grantor records from transactions.

    Args:
        transactions: Raw transactions with nested recorder_grantor_v2__recorder_id

    Returns:
        Flattened grantor records with transaction FK
    """
    grantors = []

    for txn in transactions:
        recorder_id = txn.get("recorder_id")
        nested_grantors = txn.get("recorder_grantor_v2__recorder_id", [])

        for grantor in nested_grantors:
            grantors.append(
                {
                    "recorder_id": recorder_id,
                    "cherre_recorder_grantor_pk": grantor.get("cherre_recorder_grantor_pk"),
                    "grantor_name": grantor.get("grantor_name"),
                    "grantor_address": grantor.get("grantor_address"),
                    "grantor_entity_code": grantor.get("grantor_entity_code"),
                    "grantor_first_name": grantor.get("grantor_first_name"),
                    "grantor_last_name": grantor.get("grantor_last_name"),
                }
            )

    return grantors


@task(name="sync-cherre-grantors")
def sync(transactions: list[dict]) -> int:
    """
    Extract grantors from transactions and load.

    Args:
        transactions: Raw transactions with nested parties

    Returns:
        Number of records loaded
    """
    logger = get_run_logger()
    logger.info(f"📊 Syncing {TABLE_NAME}")

    grantors = extract_from_transactions(transactions)
    wrapped = wrap_for_raw(grantors, id_field="recorder_id")
    count = insert_batch(TABLE_NAME, wrapped)

    logger.info(f"✅ Synced {count:,} grantors")
    return count
