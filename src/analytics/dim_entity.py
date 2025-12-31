"""
Analytics: dim_entity
=====================
Entity dimension from raw.cherre_grantors and raw.cherre_grantees.
"""

from prefect import get_run_logger, task

from src.db import read_table, upsert_batch

TABLE_NAME = "analytics.dim_entity"
SOURCE_TABLES = ["raw.cherre_grantors", "raw.cherre_grantees"]


def read_source() -> tuple[list[dict], list[dict]]:
    """
    Read grantors and grantees from raw tables.

    Returns:
        Tuple of (grantors, grantees)
    """
    grantors = read_table("raw.cherre_grantors")
    grantees = read_table("raw.cherre_grantees")

    return grantors, grantees


def transform(grantors: list[dict], grantees: list[dict]) -> list[dict]:
    """
    Transform grantors/grantees into dim_entity.

    Creates canonical entities by deduplicating on normalized name.

    Args:
        grantors: Raw grantor records
        grantees: Raw grantee records

    Returns:
        dim_entity records
    """
    dim_entity = []
    entity_key_counter = 1

    # Combine all parties
    all_parties = []

    for g in grantors:
        name = g.get("grantor_name")
        if name:
            all_parties.append(
                {
                    "name": name,
                    "entity_code": g.get("grantor_entity_code"),
                    "first_name": g.get("grantor_first_name"),
                    "last_name": g.get("grantor_last_name"),
                    "role": "GRANTOR",
                    "source_pk": g.get("cherre_recorder_grantor_pk"),
                }
            )

    for g in grantees:
        name = g.get("grantee_name")
        if name:
            all_parties.append(
                {
                    "name": name,
                    "entity_code": g.get("grantee_entity_code"),
                    "first_name": g.get("grantee_first_name"),
                    "last_name": g.get("grantee_last_name"),
                    "role": "GRANTEE",
                    "source_pk": g.get("cherre_recorder_grantee_pk"),
                }
            )

    # Deduplicate by normalized name
    seen_names: dict[str, dict] = {}
    for party in all_parties:
        name = party["name"]
        normalized = name.upper().strip()

        if normalized not in seen_names:
            seen_names[normalized] = party

    # Create dim_entity records
    for normalized_name, party in seen_names.items():
        # Determine entity type from entity_code
        entity_code = party.get("entity_code")
        if entity_code in ("C", "CORP", "CORPORATION"):
            entity_type = "CORPORATION"
        elif entity_code in ("T", "TRUST"):
            entity_type = "TRUST"
        elif entity_code in ("L", "LLC"):
            entity_type = "LLC"
        elif entity_code in ("P", "PARTNERSHIP"):
            entity_type = "PARTNERSHIP"
        elif party.get("first_name") and party.get("last_name"):
            entity_type = "INDIVIDUAL"
        else:
            entity_type = "UNKNOWN"

        dim_entity.append(
            {
                "entity_key": entity_key_counter,
                "canonical_entity_id": f"cherre::{normalized_name}",
                "canonical_entity_name": party["name"],
                "entity_type": entity_type,
                "state": None,
                "confidence_score": 50,  # Default confidence
                "occurrences_count": 1,
                "is_resolved": True,
                "resolution_method": "name_normalization",
                "valid_from": "2025-01-01",
                "valid_to": None,
                "is_current": True,
                "source_system": "cherre",
            }
        )
        entity_key_counter += 1

    return dim_entity


def load(records: list[dict]) -> int:
    """
    Upsert records to analytics.dim_entity.

    Args:
        records: dim_entity records

    Returns:
        Number of records upserted
    """
    logger = get_run_logger()

    count = upsert_batch(TABLE_NAME, records, on_conflict="entity_key")
    logger.info(f"✅ Loaded {count:,} records to {TABLE_NAME}")

    return count


@task(name="build-dim-entity")
def build() -> int:
    """
    Full pipeline: read → transform → load.

    Returns:
        Number of records loaded
    """
    logger = get_run_logger()
    logger.info(f"🔨 Building {TABLE_NAME}")

    grantors, grantees = read_source()
    logger.info(f"   Read {len(grantors):,} grantors, {len(grantees):,} grantees from raw")

    records = transform(grantors, grantees)
    logger.info(f"   Transformed to {len(records):,} dim_entity records")

    count = load(records)

    logger.info(f"✅ Built {TABLE_NAME}: {count:,} records")
    return count
