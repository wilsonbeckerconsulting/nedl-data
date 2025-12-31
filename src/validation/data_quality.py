"""
Data Quality Validation
=======================
Main validation task that orchestrates all DQ checks.
"""

from prefect import get_run_logger, task

from src.validation.checks import (
    check_business_logic,
    check_consistency,
    check_referential_integrity,
    check_required_fields,
    check_uniqueness,
    collect_statistics,
)
from src.validation.core import DQReport


@task(name="validate-data-quality")
def validate_data_quality(
    dim_property: list[dict],
    dim_entity: list[dict],
    dim_entity_identifier: list[dict],
    fact_transaction: list[dict],
    bridge_transaction_party: list[dict],
    bridge_property_owner: list[dict],
    start_date: str | None = None,
    end_date: str | None = None,
) -> DQReport:
    """
    Run comprehensive data quality validation.

    Args:
        dim_property: Property dimension records
        dim_entity: Entity dimension records
        dim_entity_identifier: Entity identifier records (optional)
        fact_transaction: Transaction fact records
        bridge_transaction_party: Transaction-party bridge records (optional)
        bridge_property_owner: Property-owner bridge records (optional)
        start_date: Expected start date for date range validation
        end_date: Expected end date for date range validation

    Returns:
        DQReport with all checks and statistics
    """
    logger = get_run_logger()
    logger.info("🔍 Running data quality validation...")

    report = DQReport()

    # Run all check categories
    logger.info("   Checking required fields...")
    check_required_fields(
        report, dim_property, dim_entity, fact_transaction, bridge_transaction_party
    )

    logger.info("   Checking primary key uniqueness...")
    check_uniqueness(report, dim_property, dim_entity, fact_transaction)

    logger.info("   Checking referential integrity...")
    check_referential_integrity(
        report,
        dim_property,
        dim_entity,
        fact_transaction,
        bridge_transaction_party,
        bridge_property_owner,
    )

    logger.info("   Checking data consistency...")
    check_consistency(
        report, dim_property, fact_transaction, bridge_transaction_party, start_date, end_date
    )

    logger.info("   Checking business logic...")
    check_business_logic(report, dim_property, fact_transaction)

    logger.info("   Recording statistics...")
    collect_statistics(report, dim_property, fact_transaction)

    # Summary
    logger.info("✅ DQ validation complete:")
    logger.info(f"   Total checks: {report.total}")
    logger.info(f"   ✅ Passed: {report.passed}")
    logger.info(f"   ⚠️  Warnings: {report.warnings}")
    logger.info(f"   ❌ Failed: {report.failed}")

    if report.failed > 0:
        logger.warning("❌ Some DQ checks failed:")
        for check in report.checks:
            if check["status"] == "FAIL":
                msg = f": {check['message']}" if check.get("message") else ""
                logger.warning(f"   - {check['check']}: {check['percentage']}{msg}")

    return report
