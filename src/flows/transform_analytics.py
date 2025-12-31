#!/usr/bin/env python3
"""
Transform Analytics Flow
========================
Transforms raw data to analytics dimensional model.

Tables populated:
- analytics.dim_property
- analytics.dim_entity
- analytics.fact_transaction

Usage:
    python src/flows/transform_analytics.py
"""

# Ensure src is importable when running as script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prefect import flow, get_run_logger
from prefect.events import emit_event

from src.analytics import dim_entity, dim_property, fact_transaction


@flow(name="transform-analytics", log_prints=True)
def transform_analytics_flow() -> dict:
    """
    Transform raw data to analytics dimensional model.

    Order matters:
    1. dim_property (no dependencies)
    2. dim_entity (no dependencies)
    3. fact_transaction (depends on dim_property)

    Returns:
        Summary of records transformed
    """
    logger = get_run_logger()
    logger.info("🚀 Starting transform-analytics flow")

    # ==================== DIM_PROPERTY ====================
    logger.info("\n🏢 Building dim_property")
    logger.info("-" * 40)

    dim_property_count = dim_property.build()

    # ==================== DIM_ENTITY ====================
    logger.info("\n👤 Building dim_entity")
    logger.info("-" * 40)

    dim_entity_count = dim_entity.build()

    # ==================== FACT_TRANSACTION ====================
    logger.info("\n📊 Building fact_transaction")
    logger.info("-" * 40)

    fact_transaction_count = fact_transaction.build()

    # ==================== SUMMARY ====================
    logger.info("\n" + "=" * 60)
    logger.info("TRANSFORM COMPLETE")
    logger.info("=" * 60)

    summary = {
        "counts": {
            "dim_property": dim_property_count,
            "dim_entity": dim_entity_count,
            "fact_transaction": fact_transaction_count,
        },
    }

    total = sum(summary["counts"].values())
    logger.info(f"📊 Total records transformed: {total:,}")

    for table, count in summary["counts"].items():
        logger.info(f"   {table}: {count:,}")

    # Emit success event
    emit_event(
        event="nedl.transform.complete",
        resource={"prefect.resource.id": "nedl-data.transform-analytics"},
        payload=summary,
    )

    return summary


def main():
    """CLI entry point."""
    result = transform_analytics_flow()

    print("\nTransform complete!")
    print(f"Total records: {sum(result['counts'].values()):,}")


if __name__ == "__main__":
    main()
