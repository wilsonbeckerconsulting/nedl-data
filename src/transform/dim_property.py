"""
Property Dimension Builder
==========================
Builds dim_property with SCD Type 2 from property and history data.
"""

from collections import defaultdict

from prefect import get_run_logger, task


@task(name="build-dim-property")
def build_dim_property(
    properties: list[dict], property_history: list[dict]
) -> tuple[list[dict], dict]:
    """
    Build dim_property dimension with SCD Type 2.

    Args:
        properties: List of property records from tax_assessor_v2
        property_history: List of history records from tax_assessor_history_v2

    Returns:
        Tuple of (dim_property records, property_key_lookup dict)
    """
    logger = get_run_logger()
    logger.info("🔨 Building dim_property (SCD Type 2)...")

    dim_property = []
    property_key_counter = 1

    # Create property lookup by tax_assessor_id
    property_lookup = {p["tax_assessor_id"]: p for p in properties}

    # Group history by tax_assessor_id
    history_by_property = defaultdict(list)
    for hist in property_history:
        history_by_property[hist["tax_assessor_id"]].append(hist)

    # Deduplicate: Keep only the most recent record per (tax_assessor_id, year)
    for tax_id in history_by_property.keys():
        records = history_by_property[tax_id]

        # Group by year, keep highest PK (most recent) per year
        year_groups = defaultdict(list)
        for rec in records:
            year_groups[rec["assessor_snap_shot_year"]].append(rec)

        # Keep only the record with highest PK per year
        deduped = []
        for _year, recs in year_groups.items():
            latest = max(recs, key=lambda x: x.get("cherre_tax_assessor_history_v2_pk", 0))
            deduped.append(latest)

        history_by_property[tax_id] = deduped

    # Build dimension records
    for tax_id, prop in property_lookup.items():
        histories = sorted(
            history_by_property.get(tax_id, []), key=lambda x: x["assessor_snap_shot_year"]
        )

        if histories:
            # Create SCD Type 2 rows from history
            for i, hist in enumerate(histories):
                is_current = i == len(histories) - 1
                valid_from = f"{hist['assessor_snap_shot_year']}-01-01"
                valid_to = (
                    None if is_current else f"{histories[i + 1]['assessor_snap_shot_year']}-01-01"
                )

                dim_property.append(
                    {
                        "property_key": property_key_counter,
                        "tax_assessor_id": tax_id,
                        "assessor_parcel_number": prop.get("assessor_parcel_number_raw"),
                        "property_address": prop.get("address"),
                        "property_city": prop.get("city"),
                        "property_state": prop.get("state"),
                        "property_zip": prop.get("zip"),
                        "property_county": prop.get("situs_county"),
                        "property_use_code": prop.get("property_use_standardized_code"),
                        "land_use_code": None,
                        "year_built": prop.get("year_built"),
                        "building_sqft": hist.get("building_sq_ft") or prop.get("building_sq_ft"),
                        "land_sqft": hist.get("lot_size_sq_ft") or prop.get("lot_size_sq_ft"),
                        "units_count": prop.get("units_count"),
                        "assessed_value": hist.get("assessed_value_total"),
                        "market_value": hist.get("market_value_total"),
                        "latitude": prop.get("latitude"),
                        "longitude": prop.get("longitude"),
                        "valid_from": valid_from,
                        "valid_to": valid_to,
                        "is_current": is_current,
                        "source_system": "cherre",
                    }
                )
                property_key_counter += 1
        else:
            # No history - create single current row
            dim_property.append(
                {
                    "property_key": property_key_counter,
                    "tax_assessor_id": tax_id,
                    "assessor_parcel_number": prop.get("assessor_parcel_number_raw"),
                    "property_address": prop.get("address"),
                    "property_city": prop.get("city"),
                    "property_state": prop.get("state"),
                    "property_zip": prop.get("zip"),
                    "property_county": prop.get("situs_county"),
                    "property_use_code": prop.get("property_use_standardized_code"),
                    "land_use_code": None,
                    "year_built": prop.get("year_built"),
                    "building_sqft": prop.get("building_sq_ft"),
                    "land_sqft": prop.get("lot_size_sq_ft"),
                    "units_count": prop.get("units_count"),
                    "assessed_value": prop.get("assessed_value_total"),
                    "market_value": prop.get("market_value_total"),
                    "latitude": prop.get("latitude"),
                    "longitude": prop.get("longitude"),
                    "valid_from": "2025-01-01",
                    "valid_to": None,
                    "is_current": True,
                    "source_system": "cherre",
                }
            )
            property_key_counter += 1

    # Create lookup: tax_assessor_id → current property_key
    property_key_lookup = {}
    for prop in dim_property:
        if prop["is_current"]:
            property_key_lookup[prop["tax_assessor_id"]] = prop["property_key"]

    logger.info(f"✅ Created {len(dim_property):,} dim_property records (SCD Type 2)")
    logger.info(f"   Unique properties: {len(property_key_lookup):,}")

    return dim_property, property_key_lookup
