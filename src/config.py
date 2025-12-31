"""
NEDL ETL Configuration
======================
Environment-aware settings using pydantic-settings.
Loads from environment variables or .env files.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Environment: 'dev' routes all tables to dev schema, 'prod' uses defined schemas
    environment: str = "dev"

    # Cherre API
    cherre_api_key: str = ""
    cherre_api_url: str = "https://graphql.cherre.com/graphql"

    # Supabase
    supabase_url: str = ""
    supabase_service_key: str = ""

    # Pipeline settings
    batch_size: int = 500
    page_size: int = 500
    max_retries: int = 3

    # Multifamily property use codes
    mf_codes: list[str] = ["1104", "1105", "1106", "1107", "1108", "1110", "1112"]


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
