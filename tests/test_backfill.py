"""
Tests for backfill script.
"""

# Import the modules we're testing
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from backfill import (
    Checkpoint,
    Month,
    generate_months,
    get_pending_months,
    load_checkpoint,
    save_checkpoint,
    validate_month_format,
)

# =============================================================================
# Month Tests
# =============================================================================


class TestMonth:
    def test_from_key_january(self):
        month = Month.from_key("2024-01")
        assert month.key == "2024-01"
        assert month.start_date == "2024-01-01"
        assert month.end_date == "2024-01-31"

    def test_from_key_february_leap_year(self):
        month = Month.from_key("2024-02")
        assert month.start_date == "2024-02-01"
        assert month.end_date == "2024-02-29"  # 2024 is a leap year

    def test_from_key_february_non_leap_year(self):
        month = Month.from_key("2023-02")
        assert month.end_date == "2023-02-28"

    def test_from_key_december(self):
        month = Month.from_key("2024-12")
        assert month.start_date == "2024-12-01"
        assert month.end_date == "2024-12-31"


# =============================================================================
# generate_months Tests
# =============================================================================


class TestGenerateMonths:
    def test_single_month(self):
        months = generate_months("2024-06", "2024-06")
        assert months == ["2024-06"]

    def test_same_year(self):
        months = generate_months("2024-01", "2024-03")
        assert months == ["2024-01", "2024-02", "2024-03"]

    def test_cross_year(self):
        months = generate_months("2023-11", "2024-02")
        assert months == ["2023-11", "2023-12", "2024-01", "2024-02"]

    def test_full_year(self):
        months = generate_months("2024-01", "2024-12")
        assert len(months) == 12
        assert months[0] == "2024-01"
        assert months[-1] == "2024-12"


# =============================================================================
# Checkpoint Tests
# =============================================================================


class TestCheckpoint:
    def test_empty_checkpoint(self):
        cp = Checkpoint()
        assert cp.completed == []
        assert cp.failed == []

    def test_is_done(self):
        cp = Checkpoint(completed=["2024-01", "2024-02"])
        assert cp.is_done("2024-01") is True
        assert cp.is_done("2024-03") is False

    def test_mark_complete(self):
        cp = Checkpoint()
        cp.mark_complete("2024-01")
        assert "2024-01" in cp.completed

    def test_mark_complete_removes_from_failed(self):
        cp = Checkpoint(failed=["2024-01"])
        cp.mark_complete("2024-01")
        assert "2024-01" in cp.completed
        assert "2024-01" not in cp.failed

    def test_mark_complete_idempotent(self):
        cp = Checkpoint()
        cp.mark_complete("2024-01")
        cp.mark_complete("2024-01")
        assert cp.completed.count("2024-01") == 1

    def test_mark_failed(self):
        cp = Checkpoint()
        cp.mark_failed("2024-01")
        assert "2024-01" in cp.failed

    def test_to_dict_and_from_dict(self):
        cp = Checkpoint(completed=["2024-01"], failed=["2024-02"])
        data = cp.to_dict()
        restored = Checkpoint.from_dict(data)
        assert restored.completed == ["2024-01"]
        assert restored.failed == ["2024-02"]


class TestGetPendingMonths:
    def test_all_pending(self):
        months = ["2024-01", "2024-02", "2024-03"]
        cp = Checkpoint()
        pending = get_pending_months(months, cp)
        assert pending == months

    def test_some_completed(self):
        months = ["2024-01", "2024-02", "2024-03"]
        cp = Checkpoint(completed=["2024-01"])
        pending = get_pending_months(months, cp)
        assert pending == ["2024-02", "2024-03"]

    def test_all_completed(self):
        months = ["2024-01", "2024-02"]
        cp = Checkpoint(completed=["2024-01", "2024-02"])
        pending = get_pending_months(months, cp)
        assert pending == []


# =============================================================================
# Validation Tests
# =============================================================================


class TestValidateMonthFormat:
    def test_valid_formats(self):
        assert validate_month_format("2024-01") is True
        assert validate_month_format("2024-12") is True
        assert validate_month_format("1999-06") is True

    def test_invalid_formats(self):
        assert validate_month_format("2024-13") is False  # Invalid month
        assert validate_month_format("2024/01") is False  # Wrong separator
        assert validate_month_format("January 2024") is False
        assert validate_month_format("not-a-date") is False


# =============================================================================
# Checkpoint Persistence Tests
# =============================================================================


class TestCheckpointPersistence:
    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "checkpoint.json"

            # Save
            cp = Checkpoint(completed=["2024-01"], failed=["2024-02"])
            save_checkpoint(cp, path)

            # Verify file exists
            assert path.exists()

            # Load
            loaded = load_checkpoint(path)
            assert loaded.completed == ["2024-01"]
            assert loaded.failed == ["2024-02"]

    def test_load_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nonexistent.json"
            cp = load_checkpoint(path)
            assert cp.completed == []
            assert cp.failed == []
