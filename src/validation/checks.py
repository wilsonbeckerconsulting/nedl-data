"""
Data Quality Checks
===================
Individual check functions organized by category.
"""

from collections import Counter

from src.config import get_settings
from src.validation.core import DQReport, add_check, add_stat


def check_required_fields(
    report: DQReport,
    dim_property: list[dict],
    dim_entity: list[dict],
    fact_transaction: list[dict],
    bridge_transaction_party: list[dict],
) -> None:
    """Check that required fields are populated."""
    # dim_property
    if dim_property:
        add_check(
            report,
            "REQUIRED_FIELD",
            "dim_property.tax_assessor_id NOT NULL",
            sum(1 for p in dim_property if p.get("tax_assessor_id")),
            len(dim_property),
        )
        add_check(
            report,
            "REQUIRED_FIELD",
            "dim_property.valid_from NOT NULL",
            sum(1 for p in dim_property if p.get("valid_from")),
            len(dim_property),
        )

    # dim_entity
    if dim_entity:
        add_check(
            report,
            "REQUIRED_FIELD",
            "dim_entity.canonical_entity_id NOT NULL",
            sum(1 for e in dim_entity if e.get("canonical_entity_id")),
            len(dim_entity),
        )
        add_check(
            report,
            "REQUIRED_FIELD",
            "dim_entity.canonical_entity_name NOT NULL",
            sum(1 for e in dim_entity if e.get("canonical_entity_name")),
            len(dim_entity),
        )

    # fact_transaction
    if fact_transaction:
        add_check(
            report,
            "REQUIRED_FIELD",
            "fact_transaction.recorder_id NOT NULL",
            sum(1 for t in fact_transaction if t.get("recorder_id")),
            len(fact_transaction),
        )
        add_check(
            report,
            "REQUIRED_FIELD",
            "fact_transaction.transaction_date NOT NULL",
            sum(1 for t in fact_transaction if t.get("transaction_date")),
            len(fact_transaction),
        )

    # bridge
    if bridge_transaction_party:
        add_check(
            report,
            "REQUIRED_FIELD",
            "bridge_transaction_party.party_name_raw NOT NULL",
            sum(1 for b in bridge_transaction_party if b.get("party_name_raw")),
            len(bridge_transaction_party),
        )


def check_uniqueness(
    report: DQReport,
    dim_property: list[dict],
    dim_entity: list[dict],
    fact_transaction: list[dict],
) -> None:
    """Check primary key uniqueness."""
    if dim_property:
        add_check(
            report,
            "UNIQUENESS",
            "dim_property.property_key is unique",
            len(set(p["property_key"] for p in dim_property)),
            len(dim_property),
        )
        # SCD Type 2 natural key
        unique_nk = set((p["tax_assessor_id"], p["valid_from"]) for p in dim_property)
        add_check(
            report,
            "UNIQUENESS",
            "dim_property (tax_assessor_id, valid_from) is unique",
            len(unique_nk),
            len(dim_property),
            message="SCD Type 2 natural key must be unique",
        )

    if dim_entity:
        add_check(
            report,
            "UNIQUENESS",
            "dim_entity.entity_key is unique",
            len(set(e["entity_key"] for e in dim_entity)),
            len(dim_entity),
        )

    if fact_transaction:
        add_check(
            report,
            "UNIQUENESS",
            "fact_transaction.transaction_key is unique",
            len(set(t["transaction_key"] for t in fact_transaction)),
            len(fact_transaction),
        )
        add_check(
            report,
            "UNIQUENESS",
            "fact_transaction.recorder_id is unique",
            len(set(t["recorder_id"] for t in fact_transaction)),
            len(fact_transaction),
        )


def check_referential_integrity(
    report: DQReport,
    dim_property: list[dict],
    dim_entity: list[dict],
    fact_transaction: list[dict],
    bridge_transaction_party: list[dict],
    bridge_property_owner: list[dict],
) -> None:
    """Check foreign key relationships."""
    valid_property_keys = set(p["property_key"] for p in dim_property)
    valid_entity_keys = set(e["entity_key"] for e in dim_entity)
    valid_txn_keys = set(t["transaction_key"] for t in fact_transaction)

    # fact_transaction.property_key → dim_property
    if fact_transaction:
        txns_with_prop_key = [t for t in fact_transaction if t.get("property_key")]
        txns_without = len(fact_transaction) - len(txns_with_prop_key)

        if txns_with_prop_key:
            valid_fk = sum(
                1 for t in txns_with_prop_key if t["property_key"] in valid_property_keys
            )
            add_check(
                report,
                "REFERENTIAL_INTEGRITY",
                "fact_transaction.property_key → dim_property",
                valid_fk,
                len(txns_with_prop_key),
                message=f"{valid_fk}/{len(txns_with_prop_key)} match ({txns_without} NULL)",
            )
        else:
            add_stat(
                report,
                "COVERAGE",
                "Transactions with property_key",
                f"0/{len(fact_transaction)} (0.0%)",
                description="Expected: most transactions don't match MF properties",
            )

    # bridge_transaction_party → fact_transaction
    if bridge_transaction_party:
        valid_bridge = sum(
            1 for b in bridge_transaction_party if b["transaction_key"] in valid_txn_keys
        )
        add_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_transaction_party.transaction_key → fact_transaction",
            valid_bridge,
            len(bridge_transaction_party),
        )

    # bridge_property_owner → dim_property/dim_entity
    if bridge_property_owner:
        valid_prop = sum(
            1 for b in bridge_property_owner if b["property_key"] in valid_property_keys
        )
        add_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_property_owner.property_key → dim_property",
            valid_prop,
            len(bridge_property_owner),
        )
        valid_ent = sum(1 for b in bridge_property_owner if b["entity_key"] in valid_entity_keys)
        add_check(
            report,
            "REFERENTIAL_INTEGRITY",
            "bridge_property_owner.entity_key → dim_entity",
            valid_ent,
            len(bridge_property_owner),
        )


def check_consistency(
    report: DQReport,
    dim_property: list[dict],
    fact_transaction: list[dict],
    bridge_transaction_party: list[dict],
    start_date: str | None = None,
    end_date: str | None = None,
) -> None:
    """Check data consistency rules."""
    # SCD Type 2: one current row per natural key
    if dim_property:
        current_props = sum(1 for p in dim_property if p["is_current"])
        unique_current = len(set(p["tax_assessor_id"] for p in dim_property if p["is_current"]))
        add_check(
            report,
            "CONSISTENCY",
            "dim_property: 1 current row per tax_assessor_id",
            unique_current,
            current_props if current_props > 0 else 1,
            message=f"{current_props - unique_current} duplicates"
            if current_props != unique_current
            else "OK",
        )

    # Bridge counts match fact aggregates
    if fact_transaction and bridge_transaction_party:
        bridge_grantor = sum(
            1 for b in bridge_transaction_party if b.get("party_role") == "grantor"
        )
        fact_grantor = sum(t.get("grantor_count", 0) for t in fact_transaction)
        matches = bridge_grantor == fact_grantor
        add_check(
            report,
            "CONSISTENCY",
            "bridge grantor count = fact.grantor_count sum",
            fact_grantor if matches else 0,
            fact_grantor if fact_grantor > 0 else 1,
            message=f"Bridge: {bridge_grantor:,}, Fact: {fact_grantor:,}",
        )

    # Date range validation
    if fact_transaction and start_date and end_date:
        txn_dates = [t["transaction_date"] for t in fact_transaction if t.get("transaction_date")]
        if txn_dates:
            min_date, max_date = min(txn_dates), max(txn_dates)
            in_range = sum(1 for d in txn_dates if start_date <= d <= end_date)
            add_check(
                report,
                "CONSISTENCY",
                "Transactions within date range",
                in_range,
                len(txn_dates),
                message=f"Expected: {start_date} to {end_date}, Actual: {min_date} to {max_date}",
            )


def check_business_logic(
    report: DQReport,
    dim_property: list[dict],
    fact_transaction: list[dict],
) -> None:
    """Check business rules."""
    settings = get_settings()

    if fact_transaction:
        # Sales must have amount > 0
        sales = [t for t in fact_transaction if t.get("is_sale")]
        if sales:
            with_amount = sum(
                1 for t in sales if t.get("document_amount") and t["document_amount"] > 0
            )
            add_check(
                report, "BUSINESS_LOGIC", "Sales have document_amount > 0", with_amount, len(sales)
            )

        # Transactions should have parties
        with_parties = sum(
            1
            for t in fact_transaction
            if t.get("grantor_count", 0) > 0 or t.get("grantee_count", 0) > 0
        )
        add_check(
            report,
            "BUSINESS_LOGIC",
            "Transactions have at least one party",
            with_parties,
            len(fact_transaction),
            threshold=95,
        )

    # Multifamily validation
    if dim_property:
        mf_codes = set(settings.mf_codes)
        valid_mf = sum(1 for p in dim_property if p.get("property_use_code") in mf_codes)
        add_check(
            report,
            "BUSINESS_LOGIC",
            "Properties have valid MF use codes",
            valid_mf,
            len(dim_property),
            threshold=95,
        )


def collect_statistics(
    report: DQReport,
    dim_property: list[dict],
    fact_transaction: list[dict],
) -> None:
    """Collect informational statistics."""
    # Completeness
    if dim_property:
        addr_complete = sum(1 for p in dim_property if p.get("property_address"))
        add_stat(
            report,
            "COMPLETENESS",
            "dim_property.property_address",
            f"{addr_complete:,}/{len(dim_property):,} ({addr_complete / len(dim_property) * 100:.1f}%)",
        )

    if fact_transaction:
        # Cardinality
        grantor_dist = Counter(t.get("grantor_count", 0) for t in fact_transaction)
        multi_grantor = sum(c for g, c in grantor_dist.items() if g >= 2)
        add_stat(
            report,
            "CARDINALITY",
            "Transactions with 2+ grantors",
            f"{multi_grantor:,}/{len(fact_transaction):,} ({multi_grantor / len(fact_transaction) * 100:.1f}%)",
        )

        grantee_dist = Counter(t.get("grantee_count", 0) for t in fact_transaction)
        multi_grantee = sum(c for g, c in grantee_dist.items() if g >= 2)
        add_stat(
            report,
            "CARDINALITY",
            "Transactions with 2+ grantees",
            f"{multi_grantee:,}/{len(fact_transaction):,} ({multi_grantee / len(fact_transaction) * 100:.1f}%)",
        )

        # Transaction types
        category_dist = Counter(t.get("transaction_category", "UNKNOWN") for t in fact_transaction)
        for cat, count in category_dist.items():
            add_stat(
                report,
                "TRANSACTION_TYPE",
                f"{cat} transactions",
                f"{count:,} ({count / len(fact_transaction) * 100:.1f}%)",
            )
