#!/usr/bin/env python3
"""
Extract Flow
============
Extracts data from Cherre API to raw schema.

Tables populated:
- raw.cherre_transactions
- raw.cherre_grantors
- raw.cherre_grantees
- raw.cherre_properties

Usage:
    python src/flows/extract.py
    python src/flows/extract.py --start-date 2025-01-01 --end-date 2025-01-31
"""

# Ensure src is importable when running as script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import argparse
from datetime import datetime, timedelta

from prefect import flow, get_run_logger

from src.raw import cherre_grantees, cherre_grantors, cherre_properties, cherre_transactions


@flow(name="extract-cherre", log_prints=True)
def extract_flow(
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict:
    """
    Extract data from Cherre API to raw schema.

    Args:
        start_date: Start date (YYYY-MM-DD), defaults to yesterday
        end_date: End date (YYYY-MM-DD), defaults to today

    Returns:
        Summary of records extracted
    """
    logger = get_run_logger()
    logger.info("🚀 Starting extract flow")

    # Default dates
    if not start_date:
        start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"📅 Date range: {start_date} to {end_date}")

    # ==================== EXTRACT TRANSACTIONS ====================
    logger.info("\n📊 PHASE 1: Transactions")
    logger.info("-" * 40)

    txn_result = cherre_transactions.sync(start_date, end_date)
    raw_transactions = txn_result["raw_transactions"]

    # ==================== EXTRACT PARTIES ====================
    logger.info("\n👥 PHASE 2: Parties")
    logger.info("-" * 40)

    grantor_count = cherre_grantors.sync(raw_transactions)
    grantee_count = cherre_grantees.sync(raw_transactions)

    # ==================== EXTRACT PROPERTIES ====================
    logger.info("\n🏢 PHASE 3: Properties")
    logger.info("-" * 40)

    prop_result = cherre_properties.sync(raw_transactions)

    # ==================== SUMMARY ====================
    logger.info("\n" + "=" * 60)
    logger.info("EXTRACT COMPLETE")
    logger.info("=" * 60)

    summary = {
        "date_range": {"start": start_date, "end": end_date},
        "counts": {
            "transactions": txn_result["count"],
            "grantors": grantor_count,
            "grantees": grantee_count,
            "properties": prop_result["count"],
        },
    }

    total = sum(summary["counts"].values())
    logger.info(f"📊 Total records extracted: {total:,}")

    for table, count in summary["counts"].items():
        logger.info(f"   {table}: {count:,}")

    return summary


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Extract Cherre data to raw schema")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    result = extract_flow(
        start_date=args.start_date,
        end_date=args.end_date,
    )

    print("\nExtract complete!")
    print(f"Date range: {result['date_range']['start']} to {result['date_range']['end']}")
    print(f"Total records: {sum(result['counts'].values()):,}")


if __name__ == "__main__":
    main()
