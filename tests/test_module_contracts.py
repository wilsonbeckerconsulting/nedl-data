"""
Module Contract Tests
=====================
Validates that all raw and analytics modules follow the defined contracts.
"""

import importlib
import pkgutil


def test_raw_modules_have_required_attributes():
    """All raw modules must have TABLE_NAME and sync function."""
    from src import raw

    required_attrs = ["TABLE_NAME", "sync"]

    for _, name, _ in pkgutil.iter_modules(raw.__path__):
        if name.startswith("_") or name == "cherre_client":
            continue

        module = importlib.import_module(f"src.raw.{name}")

        for attr in required_attrs:
            assert hasattr(module, attr), f"raw.{name} missing required attribute: {attr}"


def test_analytics_modules_have_required_attributes():
    """All analytics modules must have TABLE_NAME, SOURCE_TABLES, and build function."""
    from src import analytics

    required_attrs = ["TABLE_NAME", "SOURCE_TABLES", "build"]

    for _, name, _ in pkgutil.iter_modules(analytics.__path__):
        if name.startswith("_"):
            continue

        module = importlib.import_module(f"src.analytics.{name}")

        for attr in required_attrs:
            assert hasattr(module, attr), f"analytics.{name} missing required attribute: {attr}"


def test_raw_table_names_have_correct_prefix():
    """All raw module TABLE_NAMEs must start with 'raw.'"""
    from src import raw

    for _, name, _ in pkgutil.iter_modules(raw.__path__):
        if name.startswith("_") or name == "cherre_client":
            continue

        module = importlib.import_module(f"src.raw.{name}")

        assert module.TABLE_NAME.startswith(
            "raw."
        ), f"raw.{name}.TABLE_NAME must start with 'raw.', got: {module.TABLE_NAME}"


def test_analytics_table_names_have_correct_prefix():
    """All analytics module TABLE_NAMEs must start with 'analytics.'"""
    from src import analytics

    for _, name, _ in pkgutil.iter_modules(analytics.__path__):
        if name.startswith("_"):
            continue

        module = importlib.import_module(f"src.analytics.{name}")

        assert module.TABLE_NAME.startswith(
            "analytics."
        ), f"analytics.{name}.TABLE_NAME must start with 'analytics.', got: {module.TABLE_NAME}"


def test_analytics_source_tables_are_valid():
    """All analytics SOURCE_TABLES must reference valid table prefixes."""
    from src import analytics

    valid_prefixes = ("raw.", "analytics.", "app.")

    for _, name, _ in pkgutil.iter_modules(analytics.__path__):
        if name.startswith("_"):
            continue

        module = importlib.import_module(f"src.analytics.{name}")

        for source in module.SOURCE_TABLES:
            assert any(
                source.startswith(p) for p in valid_prefixes
            ), f"analytics.{name}.SOURCE_TABLES contains invalid table: {source}"
