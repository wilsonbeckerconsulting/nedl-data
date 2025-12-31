"""
Module Protocols
================
Type contracts for raw, app, and analytics modules.
"""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RawTableModule(Protocol):
    """
    Contract for raw layer modules.

    Each module extracts from an external source and loads to raw schema.
    """

    TABLE_NAME: str

    def extract(self, start_date: str, end_date: str) -> list[dict]:
        """Extract records from external source."""
        ...

    def load(self, records: list[dict], batch_id: str) -> int:
        """Load records to raw table. Returns row count."""
        ...

    def sync(self, start_date: str, end_date: str, batch_id: str) -> int:
        """Extract and load. Returns row count."""
        ...


@runtime_checkable
class TransformTableModule(Protocol):
    """
    Contract for app and analytics layer modules.

    Each module reads from raw/other tables and writes to its target table.
    """

    TABLE_NAME: str
    SOURCE_TABLES: list[str]

    def read_source(self) -> Any:
        """Read from source tables."""
        ...

    def transform(self, *args: Any) -> list[dict]:
        """Transform source data to target format."""
        ...

    def load(self, records: list[dict]) -> int:
        """Load records to target table. Returns row count."""
        ...

    def build(self) -> int:
        """Full pipeline: read → transform → load. Returns row count."""
        ...
