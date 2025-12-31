"""
Smoke tests - basic sanity checks.
"""

def test_imports():
    """Verify core modules can be imported."""
    from src import config
    from src.flows import daily_ownership
    
    assert hasattr(config, 'get_settings')
    assert hasattr(daily_ownership, 'daily_ownership_flow')


def test_settings_loads():
    """Verify settings can be instantiated (with defaults/env vars)."""
    import os
    
    # Set required env vars for test
    os.environ.setdefault('CHERRE_API_KEY', 'test-key')
    os.environ.setdefault('SUPABASE_URL', 'https://test.supabase.co')
    os.environ.setdefault('SUPABASE_SERVICE_KEY', 'test-key')
    
    from src.config import get_settings
    
    settings = get_settings()
    assert settings.cherre_api_key == 'test-key' or len(settings.cherre_api_key) > 0

