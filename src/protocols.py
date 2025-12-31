"""
Module Protocols
================
Type contracts for raw and analytics modules.

These define the expected interface for each module type.
Actual enforcement is done via test_module_contracts.py.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RawTableModule(Protocol):
    """
    Contract for raw layer modules.

    Each module extracts from an external source and loads to raw schema.

    Required attributes:
        TABLE_NAME: Full table name (e.g., 'raw.cherre_transactions')

    Required methods:
        sync(): Extract and load data. Returns dict with results or int count.
    """

    TABLE_NAME: str

    def sync(self, *args: Any, **kwargs: Any) -> Any:
        """Extract and load. Returns result dict or row count."""
        ...


@runtime_checkable
class AnalyticsTableModule(Protocol):
    """
    Contract for analytics layer modules.

    Each module reads from raw/other tables and transforms to dimensional model.

    Required attributes:
        TABLE_NAME: Full table name (e.g., 'analytics.dim_property')
        SOURCE_TABLES: List of source table names

    Required methods:
        build(): Full pipeline (read → transform → load). Returns row count.
    """

    TABLE_NAME: str
    SOURCE_TABLES: list[str]

    def build(self) -> int:
        """Full pipeline: read → transform → load. Returns row count."""
        ...
