"""
Entity Dimension Builder
========================
Builds dim_entity and dim_entity_identifier from Cherre owner data.
"""

from collections import defaultdict

from prefect import get_run_logger, task


@task(name="build-dim-entity")
def build_dim_entity(entities_raw: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """
    Build dim_entity and dim_entity_identifier dimensions.

    Args:
        entities_raw: List of entity records from usa_owner_unmask_v2

    Returns:
        Tuple of (dim_entity, dim_entity_identifier, entity_key_lookup)
    """
    logger = get_run_logger()
    logger.info("🔨 Building dim_entity (SCD Type 2)...")

    dim_entity = []
    dim_entity_identifier = []
    entity_key_counter = 1
    identifier_key_counter = 1

    # Group entities by owner_id (canonical ID)
    entities_by_owner_id = defaultdict(list)
    for ent in entities_raw:
        if ent.get("owner_id"):
            entities_by_owner_id[ent["owner_id"]].append(ent)

    for owner_id, ent_records in entities_by_owner_id.items():
        # Take first record as base (they should be duplicates)
        base = ent_records[0]

        # Calculate confidence score
        confidence = 0
        if base.get("has_confidence"):
            confidence += 50

        occ = base.get("occurrences_count") or 0
        if occ >= 10:
            confidence += 50
        elif occ >= 5:
            confidence += 30
        elif occ >= 2:
            confidence += 20
        else:
            confidence += 10

        entity_record = {
            "entity_key": entity_key_counter,
            "canonical_entity_id": owner_id,
            "cherre_owner_pk": base.get("cherre_usa_owner_unmask_pk"),
            "canonical_entity_name": base.get("owner_name"),
            "entity_type": base.get("owner_type"),
            "state": base.get("owner_state"),
            "confidence_score": confidence,
            "occurrences_count": base.get("occurrences_count"),
            "is_resolved": True,
            "resolution_method": "cherre",
            "valid_from": "2025-01-01",
            "valid_to": base.get("last_seen_date"),
            "is_current": True,
            "source_system": "cherre",
        }
        dim_entity.append(entity_record)

        # Build identifiers
        # Full owner_id as primary identifier
        dim_entity_identifier.append(
            {
                "identifier_key": identifier_key_counter,
                "entity_key": entity_key_counter,
                "identifier_type": "cherre_owner_id",
                "identifier_value": owner_id,
                "source_system": "cherre",
                "source_table": "usa_owner_unmask_v2",
                "is_primary": True,
                "valid_from": entity_record["valid_from"],
                "valid_to": entity_record["valid_to"],
                "is_current": True,
            }
        )
        identifier_key_counter += 1

        # Parse composite owner_id: "NAME::TYPE::STATE::ADDRESS"
        parts = owner_id.split("::")
        if len(parts) > 0 and parts[0]:
            dim_entity_identifier.append(
                {
                    "identifier_key": identifier_key_counter,
                    "entity_key": entity_key_counter,
                    "identifier_type": "owner_name",
                    "identifier_value": parts[0],
                    "source_system": "cherre",
                    "source_table": "usa_owner_unmask_v2",
                    "is_primary": False,
                    "valid_from": entity_record["valid_from"],
                    "valid_to": entity_record["valid_to"],
                    "is_current": True,
                }
            )
            identifier_key_counter += 1

        entity_key_counter += 1

    # Create lookup: owner_id → entity_key
    entity_key_lookup = {e["canonical_entity_id"]: e["entity_key"] for e in dim_entity}

    logger.info(f"✅ Created {len(dim_entity):,} dim_entity records")
    logger.info(f"✅ Created {len(dim_entity_identifier):,} dim_entity_identifier records")

    return dim_entity, dim_entity_identifier, entity_key_lookup
