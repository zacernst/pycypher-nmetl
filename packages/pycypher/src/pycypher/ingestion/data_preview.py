"""Data preview infrastructure for TUI integration.

Provides efficient data sampling, schema introspection, column statistics,
and query testing against sample data — all without requiring full dataset loads.

Classes:
    SamplingStrategy: Enum of supported sampling methods.
    SchemaInfo: Lightweight schema descriptor (column names, types, row count).
    ColumnStats: Per-column statistics (nulls, uniques, min/max).
    PreviewCache: LRU cache for preview results.
    DataSampler: Core sampling engine with pluggable strategies.
    QueryResult: Container for query execution results with timing.
    QueryTester: Execute Cypher queries against sampled data.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa

from pycypher.ingestion.data_sources import (
    _SQL_SCHEMES,
    _SUPPORTED_EXTENSIONS,
    _uri_to_duckdb_path,
    _validate_sql_string_literal,
    data_source_from_uri,
)
from pycypher.ingestion.security import escape_sql_string_literal


# ---------------------------------------------------------------------------
# Enums and data classes
# ---------------------------------------------------------------------------


class SamplingStrategy(Enum):
    """Supported data sampling strategies."""

    HEAD = "head"
    TAIL = "tail"
    RANDOM = "random"


@dataclass(frozen=True)
class SchemaInfo:
    """Lightweight schema descriptor returned by introspection."""

    column_names: list[str]
    column_types: list[str]
    row_count: int


@dataclass
class ColumnStats:
    """Per-column statistics."""

    name: str
    dtype: str
    null_count: int
    unique_count: int
    min_value: Any = None
    max_value: Any = None


@dataclass
class QueryResult:
    """Container for query execution results."""

    table: pa.Table | None = None
    elapsed_ms: float = 0.0
    error: str | None = None


# ---------------------------------------------------------------------------
# PreviewCache
# ---------------------------------------------------------------------------


class PreviewCache:
    """Simple LRU cache for Arrow table preview results.

    Args:
        max_size: Maximum number of cached entries before eviction.
    """

    def __init__(self, max_size: int = 32) -> None:
        self._store: OrderedDict[str, pa.Table] = OrderedDict()
        self._max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> pa.Table | None:
        if key in self._store:
            self._store.move_to_end(key)
            self.hits += 1
            return self._store[key]
        self.misses += 1
        return None

    def put(self, key: str, table: pa.Table) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        else:
            if len(self._store) >= self._max_size:
                self._store.popitem(last=False)
        self._store[key] = table


# ---------------------------------------------------------------------------
# DataSampler
# ---------------------------------------------------------------------------


class DataSampler:
    """Core sampling engine for data preview.

    Accepts a URI string, ``pd.DataFrame``, or ``pa.Table`` and provides
    efficient sampling, schema introspection, and column statistics using
    DuckDB under the hood.

    Args:
        source: File path/URI, pandas DataFrame, or Arrow table.
        cache: Optional :class:`PreviewCache` for caching sample results.
    """

    def __init__(
        self,
        source: str | pd.DataFrame | pa.Table,
        *,
        cache: PreviewCache | None = None,
    ) -> None:
        self._source = source
        self._cache = cache
        # Lazily resolved Arrow table for in-memory sources
        self._resolved_table: pa.Table | None = None
        if isinstance(source, pa.Table):
            self._resolved_table = source
        elif isinstance(source, pd.DataFrame):
            self._resolved_table = pa.Table.from_pandas(source)

    # -- internal helpers ---------------------------------------------------

    def _is_file_source(self) -> bool:
        return isinstance(self._source, str)

    def _file_ext(self) -> str:
        """Return lowercase file extension for file sources."""
        if not isinstance(self._source, str):
            return ""
        parsed = urlparse(self._source)
        path = parsed.path.lower()
        for ext in _SUPPORTED_EXTENSIONS:
            if path.endswith(ext):
                return ext
        return ""

    def _duckdb_read_fn(self, path: str) -> str:
        """Return the DuckDB read function call for the file type."""
        _validate_sql_string_literal(path, "path")
        escaped = escape_sql_string_literal(path)
        ext = self._file_ext()
        if ext == ".csv":
            return f"read_csv_auto({escaped})"
        if ext == ".parquet":
            return f"read_parquet({escaped})"
        if ext == ".json":
            return f"read_json_auto({escaped})"
        msg = f"Unsupported file extension: {ext!r}"
        raise ValueError(msg)

    def _cache_key(self, n: int, strategy: SamplingStrategy) -> str:
        source_id = self._source if isinstance(self._source, str) else id(self._source)
        return f"{source_id}:{n}:{strategy.value}"

    # -- public API ---------------------------------------------------------

    def sample(
        self,
        n: int = 50,
        strategy: SamplingStrategy = SamplingStrategy.HEAD,
    ) -> pa.Table:
        """Return a sample of *n* rows using the given strategy.

        Args:
            n: Number of rows to return.
            strategy: Sampling method (HEAD, TAIL, RANDOM).

        Returns:
            Arrow table with at most *n* rows.
        """
        if self._cache is not None:
            key = self._cache_key(n, strategy)
            cached = self._cache.get(key)
            if cached is not None:
                return cached

        result = self._sample_impl(n, strategy)

        if self._cache is not None:
            self._cache.put(self._cache_key(n, strategy), result)

        return result

    def _sample_impl(self, n: int, strategy: SamplingStrategy) -> pa.Table:
        if self._resolved_table is not None:
            return self._sample_arrow(self._resolved_table, n, strategy)
        return self._sample_file(n, strategy)

    def _sample_arrow(
        self, table: pa.Table, n: int, strategy: SamplingStrategy
    ) -> pa.Table:
        """Sample from an in-memory Arrow table."""
        total = len(table)
        if n >= total:
            return table

        if strategy == SamplingStrategy.HEAD:
            return table.slice(0, n)
        if strategy == SamplingStrategy.TAIL:
            return table.slice(total - n, n)
        if strategy == SamplingStrategy.RANDOM:
            import duckdb

            with duckdb.connect() as con:
                con.register("source", table)
                return con.execute(
                    f"SELECT * FROM source USING SAMPLE {n} ROWS (reservoir)"
                ).to_arrow_table()

        msg = f"Unknown strategy: {strategy}"
        raise ValueError(msg)

    def _sample_file(self, n: int, strategy: SamplingStrategy) -> pa.Table:
        """Sample from a file source using DuckDB SQL."""
        import duckdb

        assert isinstance(self._source, str)
        path = _uri_to_duckdb_path(self._source)
        read_fn = self._duckdb_read_fn(path)

        with duckdb.connect() as con:
            con.execute(f"CREATE VIEW source AS SELECT * FROM {read_fn}")  # nosec B608
            total = con.execute("SELECT COUNT(*) FROM source").fetchone()[0]

            if n >= total:
                return con.execute("SELECT * FROM source").to_arrow_table()

            if strategy == SamplingStrategy.HEAD:
                return con.execute(
                    f"SELECT * FROM source LIMIT {int(n)}"
                ).to_arrow_table()
            if strategy == SamplingStrategy.TAIL:
                offset = total - n
                return con.execute(
                    f"SELECT * FROM source OFFSET {int(offset)}"
                ).to_arrow_table()
            if strategy == SamplingStrategy.RANDOM:
                return con.execute(
                    f"SELECT * FROM source USING SAMPLE {int(n)} ROWS (reservoir)"
                ).to_arrow_table()

        msg = f"Unknown strategy: {strategy}"
        raise ValueError(msg)

    def schema(self) -> SchemaInfo:
        """Return schema information without loading the full dataset.

        Returns:
            :class:`SchemaInfo` with column names, types, and row count.
        """
        if self._resolved_table is not None:
            table = self._resolved_table
            return SchemaInfo(
                column_names=table.schema.names,
                column_types=[str(t) for t in table.schema.types],
                row_count=len(table),
            )

        import duckdb

        assert isinstance(self._source, str)
        path = _uri_to_duckdb_path(self._source)
        read_fn = self._duckdb_read_fn(path)

        with duckdb.connect() as con:
            con.execute(f"CREATE VIEW source AS SELECT * FROM {read_fn}")  # nosec B608
            # Get schema via DESCRIBE
            desc = con.execute("DESCRIBE source").fetchall()
            col_names = [row[0] for row in desc]
            col_types = [row[1] for row in desc]
            # Get row count
            row_count = con.execute("SELECT COUNT(*) FROM source").fetchone()[0]
            return SchemaInfo(
                column_names=col_names,
                column_types=col_types,
                row_count=row_count,
            )

    def column_stats(self, column: str) -> ColumnStats:
        """Compute statistics for a single column.

        Args:
            column: Column name.

        Returns:
            :class:`ColumnStats` with null count, unique count, min, max.

        Raises:
            ValueError: If *column* does not exist in the data source.
        """
        schema = self.schema()
        if column not in schema.column_names:
            msg = f"Column {column!r} not found in {schema.column_names}"
            raise ValueError(msg)

        if self._resolved_table is not None:
            return self._stats_from_arrow(self._resolved_table, column)
        return self._stats_from_file(column)

    def _stats_from_arrow(self, table: pa.Table, column: str) -> ColumnStats:
        import duckdb

        with duckdb.connect() as con:
            con.register("source", table)
            row = con.execute(
                f'SELECT COUNT(*) - COUNT("{column}") AS null_count, '
                f'COUNT(DISTINCT "{column}") AS unique_count, '
                f'MIN("{column}") AS min_val, '
                f'MAX("{column}") AS max_val '
                f"FROM source"
            ).fetchone()
            col_idx = table.schema.get_field_index(column)
            dtype = str(table.schema.types[col_idx])
            return ColumnStats(
                name=column,
                dtype=dtype,
                null_count=row[0],
                unique_count=row[1],
                min_value=row[2],
                max_value=row[3],
            )

    def _stats_from_file(self, column: str) -> ColumnStats:
        import duckdb

        assert isinstance(self._source, str)
        path = _uri_to_duckdb_path(self._source)
        read_fn = self._duckdb_read_fn(path)

        with duckdb.connect() as con:
            con.execute(f"CREATE VIEW source AS SELECT * FROM {read_fn}")  # nosec B608
            # Get dtype
            desc = con.execute("DESCRIBE source").fetchall()
            dtype = "unknown"
            for row in desc:
                if row[0] == column:
                    dtype = row[1]
                    break

            row = con.execute(
                f'SELECT COUNT(*) - COUNT("{column}") AS null_count, '
                f'COUNT(DISTINCT "{column}") AS unique_count, '
                f'MIN("{column}") AS min_val, '
                f'MAX("{column}") AS max_val '
                f"FROM source"
            ).fetchone()
            return ColumnStats(
                name=column,
                dtype=dtype,
                null_count=row[0],
                unique_count=row[1],
                min_value=row[2],
                max_value=row[3],
            )

    def all_column_stats(self) -> dict[str, ColumnStats]:
        """Compute statistics for all columns.

        Returns:
            Dict mapping column name to :class:`ColumnStats`.
        """
        schema = self.schema()
        return {col: self.column_stats(col) for col in schema.column_names}


# ---------------------------------------------------------------------------
# QueryTester
# ---------------------------------------------------------------------------


class QueryTester:
    """Execute Cypher queries against sampled data for TUI preview.

    Args:
        sample_size: Number of rows to sample per entity when building
            the test context.  ``None`` means use full data.
    """

    def __init__(self, sample_size: int | None = None) -> None:
        self._sample_size = sample_size
        self._entities: list[tuple[str, str | pd.DataFrame | pa.Table, str | None]] = []
        self._relationships: list[
            tuple[str, str | pd.DataFrame | pa.Table, str, str, str | None]
        ] = []

    def add_entity(
        self,
        entity_type: str,
        source: str | pd.DataFrame | pa.Table,
        *,
        id_col: str | None = None,
    ) -> QueryTester:
        """Register an entity type for query testing.

        Args:
            entity_type: Label for the entity (e.g. ``"Person"``).
            source: File path, DataFrame, or Arrow table.
            id_col: Column to use as the entity ID.

        Returns:
            ``self`` for chaining.
        """
        self._entities.append((entity_type, source, id_col))
        return self

    def add_relationship(
        self,
        rel_type: str,
        source: str | pd.DataFrame | pa.Table,
        *,
        source_col: str,
        target_col: str,
        id_col: str | None = None,
    ) -> QueryTester:
        """Register a relationship type for query testing.

        Args:
            rel_type: Relationship label (e.g. ``"KNOWS"``).
            source: File path, DataFrame, or Arrow table.
            source_col: Column containing source node IDs.
            target_col: Column containing target node IDs.
            id_col: Optional ID column for the relationship itself.

        Returns:
            ``self`` for chaining.
        """
        self._relationships.append((rel_type, source, source_col, target_col, id_col))
        return self

    def run(self, cypher: str) -> QueryResult:
        """Execute a Cypher query against the registered sample data.

        Args:
            cypher: Cypher query string.

        Returns:
            :class:`QueryResult` with the result table, timing, and any error.
        """
        from pycypher.ingestion.context_builder import ContextBuilder

        try:
            builder = ContextBuilder()

            for entity_type, source, id_col in self._entities:
                if self._sample_size is not None:
                    sampler = DataSampler(source)
                    sampled = sampler.sample(
                        n=self._sample_size, strategy=SamplingStrategy.HEAD
                    )
                    builder.add_entity(entity_type, sampled, id_col=id_col)
                else:
                    builder.add_entity(entity_type, source, id_col=id_col)

            for rel_type, source, source_col, target_col, id_col in self._relationships:
                if self._sample_size is not None:
                    sampler = DataSampler(source)
                    sampled = sampler.sample(
                        n=self._sample_size, strategy=SamplingStrategy.HEAD
                    )
                    builder.add_relationship(
                        rel_type,
                        sampled,
                        source_col=source_col,
                        target_col=target_col,
                    )
                else:
                    builder.add_relationship(
                        rel_type,
                        source,
                        source_col=source_col,
                        target_col=target_col,
                    )

            context = builder.build()

            from pycypher.star import Star

            start = time.perf_counter()
            result_df = Star(context).execute_query(cypher)
            elapsed = (time.perf_counter() - start) * 1000

            result_table = pa.Table.from_pandas(result_df)
            return QueryResult(table=result_table, elapsed_ms=elapsed)

        except Exception as exc:
            return QueryResult(error=str(exc))
