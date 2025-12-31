"""
Analytics: dim_property
=======================
Property dimension with SCD Type 2 from raw.cherre_properties.
"""

from prefect import get_run_logger, task

from src.db import read_table, upsert_batch

TABLE_NAME = "analytics.dim_property"
SOURCE_TABLES = ["raw.cherre_properties"]


def read_source() -> list[dict]:
    """
    Read latest properties from raw table.

    Returns:
        List of property records (deduplicated to latest per tax_assessor_id)
    """
    # Read all properties
    properties = read_table("raw.cherre_properties")

    # Deduplicate: keep latest _extracted_at per tax_assessor_id
    by_tax_id: dict[str, dict] = {}
    for prop in properties:
        tax_id = prop.get("tax_assessor_id")
        if not tax_id:
            continue

        existing = by_tax_id.get(tax_id)
        if not existing or prop.get("_extracted_at", "") > existing.get("_extracted_at", ""):
            by_tax_id[tax_id] = prop

    return list(by_tax_id.values())


def transform(properties: list[dict]) -> list[dict]:
    """
    Transform properties into dim_property with SCD Type 2.

    Note: For initial implementation, creates single current row per property.
    Full SCD Type 2 with history requires property_history table.

    Args:
        properties: Raw property records

    Returns:
        dim_property records
    """
    dim_property = []
    property_key_counter = 1

    for prop in properties:
        dim_property.append(
            {
                "property_key": property_key_counter,
                "tax_assessor_id": prop.get("tax_assessor_id"),
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
                "valid_from": "2025-01-01",  # TODO: derive from history
                "valid_to": None,
                "is_current": True,
                "source_system": "cherre",
            }
        )
        property_key_counter += 1

    return dim_property


def load(records: list[dict]) -> int:
    """
    Upsert records to analytics.dim_property.

    Args:
        records: dim_property records

    Returns:
        Number of records upserted
    """
    logger = get_run_logger()

    count = upsert_batch(TABLE_NAME, records, on_conflict="property_key")
    logger.info(f"✅ Loaded {count:,} records to {TABLE_NAME}")

    return count


@task(name="build-dim-property")
def build() -> int:
    """
    Full pipeline: read → transform → load.

    Returns:
        Number of records loaded
    """
    logger = get_run_logger()
    logger.info(f"🔨 Building {TABLE_NAME}")

    properties = read_source()
    logger.info(f"   Read {len(properties):,} properties from raw")

    records = transform(properties)
    logger.info(f"   Transformed to {len(records):,} dim_property records")

    count = load(records)

    logger.info(f"✅ Built {TABLE_NAME}: {count:,} records")
    return count
