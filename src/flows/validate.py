#!/usr/bin/env python3
"""
Validate Flow
=============
Runs data quality checks on analytics tables.

Emits Prefect events on DQ failures for alerting.

Usage:
    python src/flows/validate.py
"""

# Ensure src is importable when running as script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prefect import flow, get_run_logger
from prefect.events import emit_event

from src.db import read_table
from src.validation.data_quality import validate_data_quality


@flow(name="validate-analytics", log_prints=True)
def validate_flow() -> dict:
    """
    Run data quality checks on analytics tables.

    Reads current state from analytics schema and validates:
    - Required fields
    - Primary key uniqueness
    - Referential integrity
    - Business logic rules

    Returns:
        DQ report summary
    """
    logger = get_run_logger()
    logger.info("🔍 Starting validate-analytics flow")

    # ==================== READ ANALYTICS TABLES ====================
    logger.info("\n📊 Reading analytics tables")
    logger.info("-" * 40)

    dim_property = read_table("analytics.dim_property")
    logger.info(f"   dim_property: {len(dim_property):,} rows")

    dim_entity = read_table("analytics.dim_entity")
    logger.info(f"   dim_entity: {len(dim_entity):,} rows")

    # dim_entity_identifier may not exist yet
    try:
        dim_entity_identifier = read_table("analytics.dim_entity_identifier")
    except Exception:
        dim_entity_identifier = []
    logger.info(f"   dim_entity_identifier: {len(dim_entity_identifier):,} rows")

    fact_transaction = read_table("analytics.fact_transaction")
    logger.info(f"   fact_transaction: {len(fact_transaction):,} rows")

    # Bridge tables may not exist yet
    try:
        bridge_transaction_party = read_table("analytics.bridge_transaction_party")
    except Exception:
        bridge_transaction_party = []
    logger.info(f"   bridge_transaction_party: {len(bridge_transaction_party):,} rows")

    try:
        bridge_property_owner = read_table("analytics.bridge_property_owner")
    except Exception:
        bridge_property_owner = []
    logger.info(f"   bridge_property_owner: {len(bridge_property_owner):,} rows")

    # ==================== RUN DQ CHECKS ====================
    logger.info("\n✅ Running data quality checks")
    logger.info("-" * 40)

    dq_report = validate_data_quality(
        dim_property=dim_property,
        dim_entity=dim_entity,
        dim_entity_identifier=dim_entity_identifier,
        fact_transaction=fact_transaction,
        bridge_transaction_party=bridge_transaction_party,
        bridge_property_owner=bridge_property_owner,
    )

    # ==================== HANDLE RESULTS ====================
    summary = {
        "total_checks": dq_report.total,
        "passed": dq_report.passed,
        "warnings": dq_report.warnings,
        "failed": dq_report.failed,
        "checks": dq_report.checks,
        "statistics": dq_report.statistics,
    }

    if dq_report.failed > 0:
        logger.error(f"❌ DQ FAILED: {dq_report.failed} checks failed")

        # Emit failure event for Prefect Automations
        failed_checks = [c for c in dq_report.checks if c["status"] == "FAIL"]
        emit_event(
            event="nedl.dq.failure",
            resource={"prefect.resource.id": "nedl-etl.validate-analytics"},
            payload={
                "failed_count": dq_report.failed,
                "total_checks": dq_report.total,
                "failed_checks": [
                    {"check": c["check"], "percentage": c["percentage"]} for c in failed_checks
                ],
            },
        )
    else:
        logger.info(f"✅ DQ PASSED: {dq_report.passed}/{dq_report.total} checks passed")

        # Emit success event
        emit_event(
            event="nedl.dq.success",
            resource={"prefect.resource.id": "nedl-etl.validate-analytics"},
            payload=summary,
        )

    return summary


def main():
    """CLI entry point."""
    result = validate_flow()

    print("\nValidation complete!")
    print(f"Passed: {result['passed']}/{result['total_checks']}")
    print(f"Failed: {result['failed']}")


if __name__ == "__main__":
    main()
