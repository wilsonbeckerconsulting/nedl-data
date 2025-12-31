"""
Bridge Table Builders
=====================
Builds bridge_transaction_party and bridge_property_owner tables.
"""

from collections import defaultdict

from prefect import get_run_logger, task


@task(name="build-bridge-transaction-party")
def build_bridge_transaction_party(
    transactions_raw: list[dict], transaction_key_lookup: dict
) -> list[dict]:
    """
    Build bridge_transaction_party (many-to-many transaction ↔ party).

    Args:
        transactions_raw: List of raw transaction records
        transaction_key_lookup: Dict mapping recorder_id → transaction_key

    Returns:
        List of bridge_transaction_party records
    """
    logger = get_run_logger()
    logger.info("🔨 Building bridge_transaction_party...")

    bridge_transaction_party = []
    bridge_key_counter = 1

    for txn in transactions_raw:
        transaction_key = transaction_key_lookup.get(txn["recorder_id"])
        if not transaction_key:
            continue

        # Grantors (sellers)
        for seq, grantor in enumerate(txn.get("recorder_grantor_v2__recorder_id", []), 1):
            bridge_transaction_party.append(
                {
                    "bridge_key": bridge_key_counter,
                    "transaction_key": transaction_key,
                    "entity_key": None,  # TODO: entity resolution
                    "party_role": "grantor",
                    "party_sequence": seq,
                    "party_name_raw": grantor.get("grantor_name"),
                    "party_address_raw": grantor.get("grantor_address"),
                    "party_entity_code": grantor.get("grantor_entity_code"),
                    "is_resolved": False,
                    "resolution_method": None,
                    "source_table": "recorder_grantor_v2",
                    "source_record_pk": grantor.get("cherre_recorder_grantor_pk"),
                }
            )
            bridge_key_counter += 1

        # Grantees (buyers)
        for seq, grantee in enumerate(txn.get("recorder_grantee_v2__recorder_id", []), 1):
            bridge_transaction_party.append(
                {
                    "bridge_key": bridge_key_counter,
                    "transaction_key": transaction_key,
                    "entity_key": None,  # TODO: entity resolution
                    "party_role": "grantee",
                    "party_sequence": seq,
                    "party_name_raw": grantee.get("grantee_name"),
                    "party_address_raw": grantee.get("grantee_address"),
                    "party_entity_code": grantee.get("grantee_entity_code"),
                    "is_resolved": False,
                    "resolution_method": None,
                    "source_table": "recorder_grantee_v2",
                    "source_record_pk": grantee.get("cherre_recorder_grantee_pk"),
                }
            )
            bridge_key_counter += 1

    grantor_count = sum(1 for b in bridge_transaction_party if b["party_role"] == "grantor")
    grantee_count = sum(1 for b in bridge_transaction_party if b["party_role"] == "grantee")

    logger.info(f"✅ Created {len(bridge_transaction_party):,} bridge_transaction_party records")
    logger.info(f"   Grantors: {grantor_count:,} | Grantees: {grantee_count:,}")

    return bridge_transaction_party


@task(name="build-bridge-property-owner")
def build_bridge_property_owner(
    entities_raw: list[dict], property_key_lookup: dict, entity_key_lookup: dict
) -> list[dict]:
    """
    Build bridge_property_owner (many-to-many property ↔ owner with SCD Type 2).

    Args:
        entities_raw: List of raw entity records
        property_key_lookup: Dict mapping tax_assessor_id → property_key
        entity_key_lookup: Dict mapping owner_id → entity_key

    Returns:
        List of bridge_property_owner records
    """
    logger = get_run_logger()
    logger.info("🔨 Building bridge_property_owner...")

    bridge_property_owner = []
    bridge_key_counter = 1

    # Group entities by tax_assessor_id
    owners_by_property = defaultdict(list)
    for ent in entities_raw:
        if ent.get("tax_assessor_id") and ent.get("owner_id"):
            owners_by_property[ent["tax_assessor_id"]].append(ent)

    for tax_id, owners in owners_by_property.items():
        property_key = property_key_lookup.get(tax_id)
        if not property_key:
            continue

        for seq, owner in enumerate(owners, 1):
            entity_key = entity_key_lookup.get(owner["owner_id"])
            if not entity_key:
                continue

            bridge_property_owner.append(
                {
                    "bridge_key": bridge_key_counter,
                    "property_key": property_key,
                    "entity_key": entity_key,
                    "ownership_sequence": seq,
                    "ownership_percentage": None,
                    "ownership_type": None,
                    "valid_from": "2025-01-01",
                    "valid_to": owner.get("last_seen_date"),
                    "is_current": True,
                    "is_derived": False,
                }
            )
            bridge_key_counter += 1

    unique_properties = len(set(b["property_key"] for b in bridge_property_owner))
    unique_entities = len(set(b["entity_key"] for b in bridge_property_owner))

    logger.info(f"✅ Created {len(bridge_property_owner):,} bridge_property_owner records")
    logger.info(f"   Properties: {unique_properties:,} | Entities: {unique_entities:,}")

    return bridge_property_owner
