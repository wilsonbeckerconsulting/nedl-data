"""
Smoke Tests
===========
Basic sanity checks for imports and configuration.
"""


def test_imports():
    """Verify core modules can be imported."""
    from src import config
    from src.flows import extract, transform_analytics, validate

    assert hasattr(config, "get_settings")
    assert hasattr(extract, "extract_flow")
    assert hasattr(transform_analytics, "transform_analytics_flow")
    assert hasattr(validate, "validate_flow")


def test_settings_loads(settings):
    """Verify settings can be instantiated (uses fixture from conftest.py)."""
    assert settings.cherre_api_url == "https://graphql.cherre.com/graphql"
    assert settings.environment in ("dev", "prod", "staging")


def test_raw_modules_importable():
    """Verify raw modules can be imported."""
    from src.raw import cherre_grantees, cherre_grantors, cherre_properties, cherre_transactions

    assert cherre_transactions.TABLE_NAME == "raw.cherre_transactions"
    assert cherre_properties.TABLE_NAME == "raw.cherre_properties"
    assert cherre_grantors.TABLE_NAME == "raw.cherre_grantors"
    assert cherre_grantees.TABLE_NAME == "raw.cherre_grantees"


def test_analytics_modules_importable():
    """Verify analytics modules can be imported."""
    from src.analytics import dim_entity, dim_property, fact_transaction

    assert dim_property.TABLE_NAME == "analytics.dim_property"
    assert dim_entity.TABLE_NAME == "analytics.dim_entity"
    assert fact_transaction.TABLE_NAME == "analytics.fact_transaction"
