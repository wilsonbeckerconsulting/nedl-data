#!/usr/bin/env python3
"""
Backfill Script
===============
Runs the ETL pipeline for a historical date range, chunked by month.

Usage:
    python scripts/backfill.py --start 2024-01 --end 2024-12
    python scripts/backfill.py --start 2024-01 --end 2024-12 --dry-run
    python scripts/backfill.py --start 2024-01 --end 2024-12 --reset
"""

import argparse
import json
import sys
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

CHECKPOINT_FILE = Path(__file__).parent / ".backfill_checkpoint.json"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class Month:
    """A month with its date range."""

    key: str  # "2024-01"
    start_date: str  # "2024-01-01"
    end_date: str  # "2024-01-31"

    @classmethod
    def from_key(cls, key: str) -> "Month":
        """Create Month from YYYY-MM string."""
        year, month_num = map(int, key.split("-"))
        start_date = f"{year}-{month_num:02d}-01"
        last_day = monthrange(year, month_num)[1]
        end_date = f"{year}-{month_num:02d}-{last_day:02d}"
        return cls(key=key, start_date=start_date, end_date=end_date)


@dataclass
class Checkpoint:
    """Tracks backfill progress."""

    completed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)

    def is_done(self, month: str) -> bool:
        return month in self.completed

    def mark_complete(self, month: str) -> None:
        if month not in self.completed:
            self.completed.append(month)
        if month in self.failed:
            self.failed.remove(month)

    def mark_failed(self, month: str) -> None:
        if month not in self.failed:
            self.failed.append(month)

    def to_dict(self) -> dict:
        return {"completed": self.completed, "failed": self.failed}

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        return cls(
            completed=data.get("completed", []),
            failed=data.get("failed", []),
        )


@dataclass
class BackfillResult:
    """Result of processing a single month."""

    month: str
    success: bool
    extracted: int = 0
    transformed: int = 0
    error: str | None = None


# =============================================================================
# Pure Functions (easy to test)
# =============================================================================


def generate_months(start: str, end: str) -> list[str]:
    """
    Generate list of month keys between start and end (inclusive).

    Args:
        start: Start month as YYYY-MM
        end: End month as YYYY-MM

    Returns:
        List like ["2024-01", "2024-02", "2024-03"]
    """
    start_dt = datetime.strptime(start, "%Y-%m")
    end_dt = datetime.strptime(end, "%Y-%m")

    months = []
    current = start_dt

    while current <= end_dt:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)

    return months


def get_pending_months(all_months: list[str], checkpoint: Checkpoint) -> list[str]:
    """Filter to months not yet completed."""
    return [m for m in all_months if not checkpoint.is_done(m)]


def validate_month_format(value: str) -> bool:
    """Check if string is valid YYYY-MM format."""
    try:
        datetime.strptime(value, "%Y-%m")
        return True
    except ValueError:
        return False


# =============================================================================
# IO Functions (checkpoint persistence)
# =============================================================================


def load_checkpoint(path: Path = CHECKPOINT_FILE) -> Checkpoint:
    """Load checkpoint from disk."""
    if path.exists():
        data = json.loads(path.read_text())
        return Checkpoint.from_dict(data)
    return Checkpoint()


def save_checkpoint(checkpoint: Checkpoint, path: Path = CHECKPOINT_FILE) -> None:
    """Save checkpoint to disk."""
    path.write_text(json.dumps(checkpoint.to_dict(), indent=2))


def reset_checkpoint(path: Path = CHECKPOINT_FILE) -> None:
    """Delete checkpoint file."""
    if path.exists():
        path.unlink()


# =============================================================================
# ETL Runner
# =============================================================================


def process_month(month: Month, transform: bool = True) -> BackfillResult:
    """
    Run ETL for a single month.

    Args:
        month: Month to process
        transform: Whether to run transform after extract

    Returns:
        BackfillResult with counts or error
    """
    from src.flows.extract import extract_flow
    from src.flows.transform_analytics import transform_analytics_flow

    try:
        # Extract
        extract_result = extract_flow(start_date=month.start_date, end_date=month.end_date)
        extracted = sum(extract_result["counts"].values())

        # Transform
        transformed = 0
        if transform:
            transform_result = transform_analytics_flow()
            transformed = sum(transform_result["counts"].values())

        return BackfillResult(
            month=month.key,
            success=True,
            extracted=extracted,
            transformed=transformed,
        )

    except Exception as e:
        return BackfillResult(
            month=month.key,
            success=False,
            error=str(e),
        )


# =============================================================================
# Main Orchestrator
# =============================================================================


def run_backfill(
    start: str,
    end: str,
    dry_run: bool = False,
    extract_only: bool = False,
    stop_on_error: bool = False,
) -> list[BackfillResult]:
    """
    Run backfill for a range of months.

    Args:
        start: Start month (YYYY-MM)
        end: End month (YYYY-MM)
        dry_run: Print plan without executing
        extract_only: Skip transform step
        stop_on_error: Stop on first failure

    Returns:
        List of results for each month processed
    """
    all_months = generate_months(start, end)
    checkpoint = load_checkpoint()
    pending = get_pending_months(all_months, checkpoint)

    # Print summary
    print(f"\n{'=' * 60}")
    print(f"BACKFILL: {start} to {end}")
    print(f"{'=' * 60}")
    print(f"Total months:      {len(all_months)}")
    print(f"Already completed: {len(checkpoint.completed)}")
    print(f"Pending:           {len(pending)}")
    print()

    if not pending:
        print("✅ All months already completed!")
        return []

    if dry_run:
        print("🔍 DRY RUN — would process:")
        for key in pending:
            month = Month.from_key(key)
            print(f"   {key}: {month.start_date} → {month.end_date}")
        return []

    # Process each month
    results = []

    for i, key in enumerate(pending, 1):
        month = Month.from_key(key)

        print(f"\n[{i}/{len(pending)}] {month.key}")
        print(f"    {month.start_date} → {month.end_date}")

        result = process_month(month, transform=not extract_only)
        results.append(result)

        if result.success:
            checkpoint.mark_complete(month.key)
            save_checkpoint(checkpoint)
            print(f"    ✅ Extracted: {result.extracted:,}, Transformed: {result.transformed:,}")
        else:
            checkpoint.mark_failed(month.key)
            save_checkpoint(checkpoint)
            print(f"    ❌ Error: {result.error}")

            if stop_on_error:
                print("\nStopping on error. Run again to resume.")
                break

    # Final summary
    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    print(f"\n{'=' * 60}")
    print(f"COMPLETE: {succeeded} succeeded, {failed} failed")
    print(f"{'=' * 60}")

    return results


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(description="Backfill ETL for historical data")

    parser.add_argument("--start", required=True, help="Start month (YYYY-MM)")
    parser.add_argument("--end", required=True, help="End month (YYYY-MM)")
    parser.add_argument("--dry-run", action="store_true", help="Show plan without running")
    parser.add_argument("--extract-only", action="store_true", help="Skip transform step")
    parser.add_argument("--stop-on-error", action="store_true", help="Stop on first failure")
    parser.add_argument("--reset", action="store_true", help="Reset checkpoint")

    args = parser.parse_args()

    # Validate
    for value, name in [(args.start, "--start"), (args.end, "--end")]:
        if not validate_month_format(value):
            print(f"Error: {name} must be YYYY-MM format (got: {value})")
            sys.exit(1)

    if args.reset:
        reset_checkpoint()
        print("✓ Checkpoint reset")

    run_backfill(
        start=args.start,
        end=args.end,
        dry_run=args.dry_run,
        extract_only=args.extract_only,
        stop_on_error=args.stop_on_error,
    )


if __name__ == "__main__":
    main()
