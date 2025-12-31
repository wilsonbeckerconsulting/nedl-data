"""
Data Quality Core
=================
Base classes and utilities for DQ checks.
"""

from dataclasses import dataclass, field


@dataclass
class DQReport:
    """Data quality report accumulator."""

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


def add_check(
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


def add_stat(
    report: DQReport,
    category: str,
    metric: str,
    value: str,
    description: str = "",
) -> None:
    """Record a statistic (informational, no pass/fail)."""
    report.statistics.append(
        {
            "category": category,
            "metric": metric,
            "value": value,
            "description": description,
        }
    )
