"""
Data Quality Validation
=======================
Comprehensive DQ checks for the dimensional model.
"""

from collections import Counter
from dataclasses import dataclass, field

from prefect import get_run_logger, task

from src.config import get_settings


@dataclass
class DQReport:
    """Data quality report."""

    checks: list = field(default_factory=list)
    statistics: list = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "PASS")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "FAIL")

    @property
    def warnings(self) -> int:
        return sum(1 for c in self.checks if c["status"] == "WARN")

    @property
    def total(self) -> int:
        return len(self.checks)


def dq_check(
    report: DQReport,
    category: str,
    check_name: str,
    passed: int,
    total: int,
    message: str = "",
    threshold: int = 100,
) -> None:
    """
    Record a DQ check result.

    Args:
        report: DQReport to add check to
        category: Check category (e.g., 'REQUIRED_FIELD')
        check_name: Name of the check
        passed: Number of records that passed
        total: Total number of records checked
        message: Optional message
        threshold: Pass threshold percentage (default 100)
    """
    pct = (passed / total * 100) if total > 0 else 0

    if pct >= threshold:
        status = "PASS"
    elif pct >= (threshold - 15):
        status = "WARN"
    else:
        status = "FAIL"

    report.checks.append(
        {
            "category": category,
            "check": check_name,
            "status": status,
            "passed": passed,
            "total": total,
            "percentage": f"{pct:.1f}%",
            "message": message,
        }
    )


def record_stat(
    report: DQReport, category: str, metric: str, value: str, description: str = ""
) -> None:
    """Record a statistic (informational, no pass/fail)."""
    report.statistics.append(
        {"category": category, "metric": metric, "value": value, "description": description}
    )


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
        All dimension and fact tables
        start_date: Expected start date for validation
        end_date: Expected end date for validation

    Returns:
        DQReport with all checks and statistics
    """
    logger = get_run_logger()
    settings = get_settings()

    logger.info("🔍 Running data quality validation...")
    report = DQReport()

    # ==================== REQUIRED FIELDS ====================
    logger.info("   Checking required fields...")

    # dim_property required fields
    if len(dim_property) > 0:
        dq_check(
            report,
            "REQUIRED_FIELD",
            "dim_property.tax_assessor_id NOT NULL",
            sum(1 for p in dim_property if p.get("tax_assessor_id")),
            len(dim_property),
        )
        dq_check(
            report,
            "REQUIRED_FIELD",
            "dim_property.valid_from NOT NULL",
            sum(1 for p in dim_property if p.get("valid_from")),
            len(dim_property),
        )

    # dim_entity required fields
    if len(dim_entity) > 0:
        dq_check(
            report,
            "REQUIRED_FIELD",
            "dim_entity.canonical_entity_id NOT NULL",
            sum(1 for e in dim_entity if e.get("canonical_entity_id")),
            len(dim_entity),
        )
        dq_check(
            report,
            "REQUIRED_FIELD",
            "dim_entity.canonical_entity_name NOT NULL",
            sum(1 for e in dim_entity if e.get("canonical_entity_name")),
            len(dim_entity),
        )

    # fact_transaction required fields
    if len(fact_transaction) > 0:
        dq_check(
            report,
            "REQUIRED_FIELD",
            "fact_transaction.recorder_id NOT NULL",
            sum(1 for t in fact_transaction if t.get("recorder_id")),
            len(fact_transaction),
        )
        dq_check(
            report,
            "REQUIRED_FIELD",
            "fact_transaction.transaction_date NOT NULL",
            sum(1 for t in fact_transaction if t.get("transaction_date")),
            len(fact_transaction),
        )

    # bridge required fields
    if len(bridge_transaction_party) > 0:
        dq_check(
            report,
            "REQUIRED_FIELD",
            "bridge_transaction_party.party_name_raw NOT NULL",
            sum(1 for b in bridge_transaction_party if b.get("party_name_raw")),
            len(bridge_transaction_party),
        )

    # ==================== PRIMARY KEY UNIQUENESS ====================
    logger.info("   Checking primary key uniqueness...")

    if len(dim_property) > 0:
        dq_check(
            report,
            "UNIQUENESS",
            "dim_property.property_key is unique",
            len(set(p["property_key"] for p in dim_property)),
            len(dim_property),
        )

        # SCD Type 2 natural key
        unique_nk = set((p["tax_assessor_id"], p["valid_from"]) for p in dim_property)
        dq_check(
            report,
            "UNIQUENESS",
            "dim_property (tax_assessor_id, valid_from) is unique",
            len(unique_nk),
            len(dim_property),
            message="SCD Type 2 natural key must be unique",
        )

    if len(dim_entity) > 0:
        dq_check(
            report,
            "UNIQUENESS",
            "dim_entity.entity_key is unique",
            len(set(e["entity_key"] for e in dim_entity)),
            len(dim_entity),
        )

    if len(fact_transaction) > 0:
        dq_check(
            report,
            "UNIQUENESS",
            "fact_transaction.transaction_key is unique",
            len(set(t["transaction_key"] for t in fact_transaction)),
            len(fact_transaction),
        )
        dq_check(
            report,
            "UNIQUENESS",
            "fact_transaction.recorder_id is unique",
            len(set(t["recorder_id"] for t in fact_transaction)),
            len(fact_transaction),
        )

    # ==================== REFERENTIAL INTEGRITY ====================
    logger.info("   Checking referential integrity...")

    valid_property_keys = set(p["property_key"] for p in dim_property)
    valid_entity_keys = set(e["entity_key"] for e in dim_entity)
    valid_txn_keys = set(t["transaction_key"] for t in fact_transaction)

    # fact_transaction.property_key → dim_property
    if len(fact_transaction) > 0:
        txns_with_prop_key = [t for t in fact_transaction if t.get("property_key")]
        valid_fk = sum(1 for t in txns_with_prop_key if t["property_key"] in valid_property_keys)
        dq_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "fact_transaction.property_key → dim_property",
            valid_fk,
            len(txns_with_prop_key),
            message="All non-null FKs must exist in parent table",
        )

    # bridge_transaction_party → fact_transaction
    if len(bridge_transaction_party) > 0:
        valid_bridge = sum(
            1 for b in bridge_transaction_party if b["transaction_key"] in valid_txn_keys
        )
        dq_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_transaction_party.transaction_key → fact_transaction",
            valid_bridge,
            len(bridge_transaction_party),
        )

    # bridge_property_owner → dim_property
    if len(bridge_property_owner) > 0:
        valid_po_prop = sum(
            1 for b in bridge_property_owner if b["property_key"] in valid_property_keys
        )
        dq_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_property_owner.property_key → dim_property",
            valid_po_prop,
            len(bridge_property_owner),
        )

        valid_po_ent = sum(1 for b in bridge_property_owner if b["entity_key"] in valid_entity_keys)
        dq_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_property_owner.entity_key → dim_entity",
            valid_po_ent,
            len(bridge_property_owner),
        )

    # ==================== DATA CONSISTENCY ====================
    logger.info("   Checking data consistency...")

    # SCD Type 2: is_current flag consistency
    if len(dim_property) > 0:
        current_props = sum(1 for p in dim_property if p["is_current"])
        unique_current = len(set(p["tax_assessor_id"] for p in dim_property if p["is_current"]))
        dq_check(
            report,
            "CONSISTENCY",
            "dim_property: 1 current row per tax_assessor_id",
            unique_current,
            current_props if current_props > 0 else 1,
            message=f"{current_props - unique_current} duplicate current rows"
            if current_props != unique_current
            else "No duplicates",
        )

    # Bridge counts match fact aggregates
    if len(fact_transaction) > 0 and len(bridge_transaction_party) > 0:
        bridge_grantor = sum(1 for b in bridge_transaction_party if b["party_role"] == "grantor")
        fact_grantor = sum(t["grantor_count"] for t in fact_transaction)
        matches = bridge_grantor == fact_grantor
        dq_check(
            report,
            "CONSISTENCY",
            "bridge grantor count = fact.grantor_count sum",
            fact_grantor if matches else 0,
            fact_grantor if fact_grantor > 0 else 1,
            message=f"Bridge: {bridge_grantor:,}, Fact: {fact_grantor:,}",
        )

    # Date range validation
    if len(fact_transaction) > 0 and start_date and end_date:
        txn_dates = [t["transaction_date"] for t in fact_transaction if t.get("transaction_date")]
        if txn_dates:
            min_date = min(txn_dates)
            max_date = max(txn_dates)
            in_range = sum(1 for d in txn_dates if start_date <= d <= end_date)
            dq_check(
                report,
                "CONSISTENCY",
                "Transactions within date range",
                in_range,
                len(txn_dates),
                message=f"Expected: {start_date} to {end_date}, Actual: {min_date} to {max_date}",
            )

    # ==================== BUSINESS LOGIC ====================
    logger.info("   Checking business logic...")

    if len(fact_transaction) > 0:
        # Sales must have amount > 0
        sales = [t for t in fact_transaction if t["is_sale"]]
        if len(sales) > 0:
            sales_with_amount = sum(
                1 for t in sales if t.get("document_amount") and t["document_amount"] > 0
            )
            dq_check(
                report,
                "BUSINESS_LOGIC",
                "Sales have document_amount > 0",
                sales_with_amount,
                len(sales),
            )

        # Transactions should have parties
        with_parties = sum(
            1 for t in fact_transaction if t["grantor_count"] > 0 or t["grantee_count"] > 0
        )
        dq_check(
            report,
            "BUSINESS_LOGIC",
            "Transactions have at least one party",
            with_parties,
            len(fact_transaction),
            threshold=95,
        )

    # Multifamily validation
    if len(dim_property) > 0:
        mf_codes = set(settings.mf_codes)
        with_valid_code = sum(1 for p in dim_property if p.get("property_use_code") in mf_codes)
        dq_check(
            report,
            "BUSINESS_LOGIC",
            "Properties have valid MF use codes",
            with_valid_code,
            len(dim_property),
            threshold=95,
        )

    # ==================== STATISTICS ====================
    logger.info("   Recording statistics...")

    # Completeness stats
    if len(dim_property) > 0:
        addr_complete = sum(1 for p in dim_property if p.get("property_address"))
        record_stat(
            report,
            "COMPLETENESS",
            "dim_property.property_address",
            f"{addr_complete:,}/{len(dim_property):,} ({addr_complete / len(dim_property) * 100:.1f}%)",
        )

    # Cardinality stats
    if len(fact_transaction) > 0:
        grantor_dist = Counter(t["grantor_count"] for t in fact_transaction)
        multi_grantor = sum(c for g, c in grantor_dist.items() if g >= 2)
        record_stat(
            report,
            "CARDINALITY",
            "Transactions with 2+ grantors",
            f"{multi_grantor:,}/{len(fact_transaction):,} ({multi_grantor / len(fact_transaction) * 100:.1f}%)",
        )

        grantee_dist = Counter(t["grantee_count"] for t in fact_transaction)
        multi_grantee = sum(c for g, c in grantee_dist.items() if g >= 2)
        record_stat(
            report,
            "CARDINALITY",
            "Transactions with 2+ grantees",
            f"{multi_grantee:,}/{len(fact_transaction):,} ({multi_grantee / len(fact_transaction) * 100:.1f}%)",
        )

    # Transaction type stats
    if len(fact_transaction) > 0:
        category_dist = Counter(t["transaction_category"] for t in fact_transaction)
        for cat, count in category_dist.items():
            record_stat(
                report,
                "TRANSACTION_TYPE",
                f"{cat} transactions",
                f"{count:,} ({count / len(fact_transaction) * 100:.1f}%)",
            )

    # ==================== SUMMARY ====================
    logger.info("✅ DQ validation complete:")
    logger.info(f"   Total checks: {report.total}")
    logger.info(f"   ✅ Passed: {report.passed}")
    logger.info(f"   ⚠️  Warnings: {report.warnings}")
    logger.info(f"   ❌ Failed: {report.failed}")

    if report.failed > 0:
        logger.warning("❌ Some DQ checks failed:")
        for check in report.checks:
            if check["status"] == "FAIL":
                logger.warning(f"   - {check['check']}: {check['percentage']}")

    return report
