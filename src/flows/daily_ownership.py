#!/usr/bin/env python3
"""
Daily Ownership ETL Flow
========================
Builds the NEDL dimensional model from Cherre GraphQL API.

Tables produced:
- dim_property (SCD Type 2)
- dim_entity (SCD Type 2)
- dim_entity_identifier
- fact_transaction
- bridge_transaction_party
- bridge_property_owner

Usage:
    python src/flows/daily_ownership.py
    python src/flows/daily_ownership.py --start-date 2025-01-01 --end-date 2025-01-31
"""

# Ensure src is importable when running as script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import argparse
from datetime import datetime, timedelta
from typing import Optional

from prefect import flow, get_run_logger
from prefect.events import emit_event

from src.config import get_settings

# Extract
from src.extract.cherre import (
    extract_entities,
    extract_properties,
    extract_property_history,
    extract_transactions,
)

# Load
from src.load.supabase import load_to_supabase
from src.transform.bridges import (
    build_bridge_property_owner,
    build_bridge_transaction_party,
)
from src.transform.dim_entity import build_dim_entity

# Transform
from src.transform.dim_property import build_dim_property
from src.transform.fact_transaction import build_fact_transaction

# Validate
from src.validation.data_quality import validate_data_quality


def send_dq_alert(dq_report, start_date: str, end_date: str):
    """
    Emit a Prefect event for DQ failures.
    This can trigger Prefect Automations for alerting.
    """
    failed_checks = [c for c in dq_report.checks if c["status"] == "FAIL"]

    # Emit a custom event that Prefect Automations can trigger on
    emit_event(
        event="nedl.dq.failure",
        resource={"prefect.resource.id": "nedl-etl.dq-check"},
        payload={
            "start_date": start_date,
            "end_date": end_date,
            "failed_count": dq_report.failed,
            "total_checks": dq_report.total,
            "failed_checks": [
                {"check": c["check"], "percentage": c["percentage"]} for c in failed_checks
            ],
        },
    )


@flow(name="daily-ownership-etl", log_prints=True)
def daily_ownership_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Daily ETL flow for ownership dimensional model.

    This flow:
    1. Extracts data from Cherre GraphQL API
    2. Transforms into dimensional model (dim_property, dim_entity, fact_transaction, bridges)
    3. Validates data quality
    4. Loads to Supabase (if DQ passes)

    Args:
        start_date: Start date (YYYY-MM-DD), defaults to yesterday
        end_date: End date (YYYY-MM-DD), defaults to today

    Returns:
        Summary of records processed and DQ results
    """
    logger = get_run_logger()
    settings = get_settings()

    # Default dates
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info("NEDL DAILY OWNERSHIP ETL")
    logger.info("=" * 60)
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info("=" * 60)

    # ==================== PHASE 1: EXTRACT ====================
    logger.info("\n📥 PHASE 1: EXTRACT")
    logger.info("-" * 40)

    # Extract transactions with nested grantors/grantees
    transactions_raw = extract_transactions(start_date, end_date)

    # Extract properties (multifamily only)
    properties_raw = extract_properties(transactions_raw)

    # Extract property history for SCD Type 2
    property_history_raw = extract_property_history(properties_raw)

    # Extract entities/owners
    entities_raw = extract_entities(properties_raw)

    logger.info("\n📊 Extraction Summary:")
    logger.info(f"   Transactions: {len(transactions_raw):,}")
    logger.info(f"   Properties: {len(properties_raw):,}")
    logger.info(f"   Property History: {len(property_history_raw):,}")
    logger.info(f"   Entities: {len(entities_raw):,}")

    # ==================== PHASE 2: TRANSFORM ====================
    logger.info("\n🔄 PHASE 2: TRANSFORM")
    logger.info("-" * 40)

    # Build dim_property with SCD Type 2
    dim_property, property_key_lookup = build_dim_property(properties_raw, property_history_raw)

    # Build dim_entity and dim_entity_identifier
    dim_entity, dim_entity_identifier, entity_key_lookup = build_dim_entity(entities_raw)

    # Build fact_transaction
    fact_transaction, transaction_key_lookup = build_fact_transaction(
        transactions_raw, property_key_lookup
    )

    # Build bridge tables
    bridge_transaction_party = build_bridge_transaction_party(
        transactions_raw, transaction_key_lookup
    )
    bridge_property_owner = build_bridge_property_owner(
        entities_raw, property_key_lookup, entity_key_lookup
    )

    logger.info("\n📊 Transform Summary:")
    logger.info(f"   dim_property: {len(dim_property):,}")
    logger.info(f"   dim_entity: {len(dim_entity):,}")
    logger.info(f"   dim_entity_identifier: {len(dim_entity_identifier):,}")
    logger.info(f"   fact_transaction: {len(fact_transaction):,}")
    logger.info(f"   bridge_transaction_party: {len(bridge_transaction_party):,}")
    logger.info(f"   bridge_property_owner: {len(bridge_property_owner):,}")

    # ==================== PHASE 3: VALIDATE ====================
    logger.info("\n✅ PHASE 3: VALIDATE")
    logger.info("-" * 40)

    dq_report = validate_data_quality(
        dim_property=dim_property,
        dim_entity=dim_entity,
        dim_entity_identifier=dim_entity_identifier,
        fact_transaction=fact_transaction,
        bridge_transaction_party=bridge_transaction_party,
        bridge_property_owner=bridge_property_owner,
        start_date=start_date,
        end_date=end_date,
    )

    # ==================== PHASE 4: LOAD ====================
    logger.info("\n📤 PHASE 4: LOAD")
    logger.info("-" * 40)

    if dq_report.failed > 0:
        # Log the failures
        logger.error(f"❌ DATA QUALITY FAILED: {dq_report.failed} checks failed")
        for check in dq_report.checks:
            if check["status"] == "FAIL":
                logger.error(
                    f"   ❌ {check['check']}: {check['percentage']} - {check.get('message', '')}"
                )

        # Send notification (flow continues)
        send_dq_alert(dq_report, start_date, end_date)

        # Skip load but don't fail
        load_result = {"skipped": True, "reason": f"{dq_report.failed} DQ failures"}
    else:
        load_result = load_to_supabase(
            dim_property=dim_property,
            dim_entity=dim_entity,
            dim_entity_identifier=dim_entity_identifier,
            fact_transaction=fact_transaction,
            bridge_transaction_party=bridge_transaction_party,
            bridge_property_owner=bridge_property_owner,
        )

    # ==================== SUMMARY ====================
    logger.info("\n" + "=" * 60)
    logger.info("ETL COMPLETE")
    logger.info("=" * 60)

    total_records = (
        len(dim_property)
        + len(dim_entity)
        + len(dim_entity_identifier)
        + len(fact_transaction)
        + len(bridge_transaction_party)
        + len(bridge_property_owner)
    )

    logger.info(f"📊 Total records: {total_records:,}")
    logger.info(f"✅ DQ Passed: {dq_report.passed}/{dq_report.total}")
    logger.info(f"⚠️  DQ Warnings: {dq_report.warnings}/{dq_report.total}")
    logger.info(f"❌ DQ Failed: {dq_report.failed}/{dq_report.total}")

    return {
        "date_range": {"start": start_date, "end": end_date},
        "extraction": {
            "transactions": len(transactions_raw),
            "properties": len(properties_raw),
            "property_history": len(property_history_raw),
            "entities": len(entities_raw),
        },
        "transformation": {
            "dim_property": len(dim_property),
            "dim_entity": len(dim_entity),
            "dim_entity_identifier": len(dim_entity_identifier),
            "fact_transaction": len(fact_transaction),
            "bridge_transaction_party": len(bridge_transaction_party),
            "bridge_property_owner": len(bridge_property_owner),
        },
        "data_quality": {
            "total": dq_report.total,
            "passed": dq_report.passed,
            "warnings": dq_report.warnings,
            "failed": dq_report.failed,
        },
        "load": load_result,
    }


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="NEDL Daily Ownership ETL")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    result = daily_ownership_flow(start_date=args.start_date, end_date=args.end_date)

    print(f"\n{'=' * 60}")
    print("RESULT SUMMARY")
    print(f"{'=' * 60}")
    print(f"Date range: {result['date_range']['start']} to {result['date_range']['end']}")
    print(f"Transactions extracted: {result['extraction']['transactions']:,}")
    print(f"Properties extracted: {result['extraction']['properties']:,}")
    print(f"Total transformed: {sum(result['transformation'].values()):,}")
    print(f"DQ: {result['data_quality']['passed']}/{result['data_quality']['total']} passed")
    print()


if __name__ == "__main__":
    main()
