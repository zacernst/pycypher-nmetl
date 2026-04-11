"""Data source introspection for TUI preview and schema detection.

``DataSourceIntrospector`` provides lightweight schema detection, data
sampling, and column statistics without requiring a full pipeline build.

Usage example::

    introspector = DataSourceIntrospector("data/people.csv")
    schema = introspector.get_schema()       # column names + types
    sample = introspector.sample(n=10)       # first 10 rows as DataFrame
    stats = introspector.get_column_stats()  # null counts, uniques, etc.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pyarrow as pa

LOGGER = logging.getLogger(__name__)

from pycypher.ingestion.data_sources import data_source_from_uri

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class SchemaInfo:
    """Schema information for a data source.

    Attributes:
        columns: List of dicts with ``name`` and ``type`` keys.
        row_count: Total number of rows (may be ``None`` if unknown).
    """

    columns: list[dict[str, str]] = field(default_factory=list)
    row_count: int | None = None


@dataclass
class ColumnStats:
    """Statistics for a single column.

    Attributes:
        name: Column name.
        dtype: Data type as a string.
        null_count: Number of null values.
        unique_count: Number of distinct non-null values.
        min_value: Minimum value (for ordered types), or ``None``.
        max_value: Maximum value (for ordered types), or ``None``.
    """

    name: str
    dtype: str
    null_count: int = 0
    unique_count: int = 0
    min_value: Any = None
    max_value: Any = None


# ---------------------------------------------------------------------------
# Introspector
# ---------------------------------------------------------------------------


class DataSourceIntrospector:
    """Lightweight introspection of data sources for TUI previews.

    Accepts any source that :func:`data_source_from_uri` supports: file
    paths (CSV, Parquet, JSON), Arrow tables, or pandas DataFrames.

    Args:
        source: A URI string, ``pa.Table``, or ``pd.DataFrame``.
        query: Optional SQL query (for file-based sources).
    """

    def __init__(
        self,
        source: str | pa.Table | pd.DataFrame,
        *,
        query: str | None = None,
    ) -> None:
        self._source = source
        self._query = query
        self._table: pa.Table | None = None

    def _load(self) -> pa.Table:
        """Load the full Arrow table (cached after first call)."""
        if self._table is None:
            ds = data_source_from_uri(self._source, query=self._query)
            self._table = ds.read()
        return self._table

    # -- Schema detection ---------------------------------------------------

    def get_schema(self) -> SchemaInfo:
        """Detect the schema of the data source.

        Returns:
            A :class:`SchemaInfo` with column names, types, and row count.
        """
        table = self._load()
        columns = [
            {"name": f.name, "type": str(f.type)}
            for f in table.schema
        ]
        return SchemaInfo(columns=columns, row_count=len(table))

    # -- Sampling -----------------------------------------------------------

    def sample(self, n: int = 100) -> pd.DataFrame:
        """Return the first *n* rows as a pandas DataFrame.

        Args:
            n: Maximum number of rows to return.

        Returns:
            A ``pd.DataFrame`` with at most *n* rows.
        """
        table = self._load()
        sliced = table.slice(0, min(n, len(table)))
        return sliced.to_pandas()

    # -- Column statistics --------------------------------------------------

    def get_column_stats(self) -> dict[str, ColumnStats]:
        """Compute per-column statistics.

        Returns:
            Dict mapping column name to :class:`ColumnStats`.
        """
        table = self._load()
        result: dict[str, ColumnStats] = {}

        for i, f in enumerate(table.schema):
            col = table.column(i)
            null_count = col.null_count

            # Unique count (excluding nulls)
            try:
                unique_values = col.drop_null().unique()
                unique_count = len(unique_values)
            except Exception:  # noqa: BLE001 — best-effort stats
                LOGGER.debug(
                    "Failed to compute unique count for column %r", f.name,
                    exc_info=True,
                )
                unique_count = 0

            # Min/max for numeric and string types
            min_val: Any = None
            max_val: Any = None
            try:
                import pyarrow.compute as pc

                non_null = col.drop_null()
                if len(non_null) > 0 and _is_ordered_type(f.type):
                    min_val = pc.min(non_null).as_py()
                    max_val = pc.max(non_null).as_py()
            except Exception:  # noqa: BLE001 — best-effort stats
                LOGGER.debug(
                    "Failed to compute min/max for column %r", f.name,
                    exc_info=True,
                )

            result[f.name] = ColumnStats(
                name=f.name,
                dtype=str(f.type),
                null_count=null_count,
                unique_count=unique_count,
                min_value=min_val,
                max_value=max_val,
            )

        return result


def _is_ordered_type(arrow_type: pa.DataType) -> bool:
    """Return True if the Arrow type supports min/max comparison."""
    return (
        pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_decimal(arrow_type)
        or pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_date(arrow_type)
        or pa.types.is_timestamp(arrow_type)
    )
