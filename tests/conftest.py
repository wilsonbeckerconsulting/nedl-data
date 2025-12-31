"""
Pytest Configuration
====================
Shared fixtures for all tests.
"""

import os

import pytest


@pytest.fixture(scope="session", autouse=True)
def set_test_env():
    """Set up test environment variables before any tests run."""
    # Set required env vars for testing
    os.environ["CHERRE_API_KEY"] = os.environ.get("CHERRE_API_KEY", "test-key")
    os.environ["CHERRE_API_URL"] = os.environ.get(
        "CHERRE_API_URL", "https://graphql.cherre.com/graphql"
    )
    os.environ["SUPABASE_URL"] = os.environ.get("SUPABASE_URL", "https://test.supabase.co")
    os.environ["SUPABASE_SERVICE_KEY"] = os.environ.get("SUPABASE_SERVICE_KEY", "test-key")
    os.environ["ENVIRONMENT"] = os.environ.get("ENVIRONMENT", "dev")

    # Clear any cached settings
    from src.config import get_settings

    get_settings.cache_clear()

    yield

    # Clean up after tests
    get_settings.cache_clear()


@pytest.fixture
def settings():
    """Provide settings instance for tests."""
    from src.config import get_settings

    return get_settings()
