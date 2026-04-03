"""DuckDB-based backend for analytical workloads.

Uses DuckDB's in-process OLAP engine for efficient columnar operations.
Particularly effective for:

- Large aggregations (GROUP BY with many groups)
- Join optimisation (DuckDB's query planner handles strategy selection)
- Sort/limit composition (DuckDB fuses ORDER BY + LIMIT efficiently)
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.backends._helpers import (
    _pandas_agg_to_sql,
    _to_pandas,
    validate_identifier,
)
from pycypher.constants import ID_COLUMN
from pycypher.cypher_types import BackendMask, SourceObject


class DuckDBLazyFrame:
    """Internal lazy wrapper around a DuckDB Relation.

    Holds a DuckDB Relation representing a pending query.  When passed
    back into a ``DuckDBBackend`` operation, the backend can compose SQL
    rather than materialising and re-registering.

    Transparent to callers: attribute access, iteration, and item access
    auto-materialise to a pandas DataFrame (cached).  ``columns`` and
    ``__len__`` are answered without full materialisation.
    """

    __slots__ = ("_relation", "_conn", "_materialised", "_backend_ref")

    def __init__(
        self,
        relation: Any,
        conn: Any,
        backend: Any = None,
    ) -> None:
        self._relation = relation
        self._conn = conn
        self._materialised: pd.DataFrame | None = None
        self._backend_ref = backend

    @property
    def relation(self) -> Any:
        """The underlying DuckDB Relation."""
        return self._relation

    @property
    def columns(self) -> list[str]:
        """Column names (O(1) from relation schema)."""
        return self._relation.columns

    def _materialise(self) -> pd.DataFrame:
        """Materialise the relation, caching the result."""
        if self._materialised is None:
            self._materialised = self._relation.fetchdf()
        return self._materialised

    def to_pandas(self) -> pd.DataFrame:
        """Materialise the lazy relation into a pandas DataFrame."""
        return self._materialise()

    def __len__(self) -> int:
        if self._materialised is not None:
            return len(self._materialised)
        try:
            row = self._relation.aggregate("COUNT(*) AS _cnt").fetchone()
            return row[0] if row else 0
        except Exception:  # noqa: BLE001 — graceful fallback to materialised count
            return len(self._materialise())

    def __contains__(self, item: str) -> bool:
        return item in self._relation.columns

    def __getattr__(self, name: str) -> Any:
        """Auto-materialise and delegate to pandas DataFrame."""
        return getattr(self._materialise(), name)

    def __getitem__(self, key: Any) -> Any:
        return self._materialise()[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._materialise()[key] = value

    def __iter__(self) -> Any:
        return iter(self._materialise())

    def __repr__(self) -> str:
        return f"DuckDBLazyFrame(columns={self._relation.columns!r})"


def _is_lazy(obj: Any) -> bool:
    """Check if *obj* is a DuckDBLazyFrame."""
    return isinstance(obj, DuckDBLazyFrame)


def _to_df(frame: Any) -> pd.DataFrame:
    """Materialise *frame* to pandas if lazy, otherwise convert."""
    if _is_lazy(frame):
        return frame.to_pandas()
    if isinstance(frame, pd.DataFrame):
        return frame
    return _to_pandas(frame)


class DuckDBBackend:
    """DuckDB-based backend for analytical workloads.

    Uses DuckDB's in-process OLAP engine for efficient columnar operations.
    Particularly effective for:

    - Large aggregations (GROUP BY with many groups)
    - Join optimisation (DuckDB's query planner handles strategy selection)
    - Sort/limit composition (DuckDB fuses ORDER BY + LIMIT efficiently)

    The ``sort()`` method returns a ``DuckDBLazyFrame`` so that a subsequent
    ``limit()`` can compose ORDER BY + LIMIT into a single DuckDB query.
    All other operations accept ``DuckDBLazyFrame`` inputs transparently
    and return ``pd.DataFrame`` for full backward compatibility.
    """

    def __init__(self) -> None:
        """Create a new DuckDB backend with an in-memory connection.

        The connection is created immediately and held for the lifetime of
        the backend.  Use as a context manager or call :meth:`close` to
        release the underlying DuckDB resources.
        """
        import duckdb

        self._conn: Any = duckdb.connect(":memory:")
        self._view_counter: int = 0

    def _next_view(self, prefix: str = "_v") -> str:
        """Generate a unique view name to avoid collisions."""
        self._view_counter += 1
        return f"{prefix}_{self._view_counter}"

    # -- Context manager & cleanup -----------------------------------------

    def close(self) -> None:
        """Explicitly close the DuckDB connection.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 — best-effort connection cleanup
                LOGGER.warning(
                    "DuckDB connection close raised; ignoring",
                    exc_info=True,
                )
            finally:
                self._conn = None

    def __enter__(self) -> DuckDBBackend:
        """Enter the context manager, returning this backend instance."""
        return self

    def __exit__(self, *_exc: object) -> None:
        """Exit the context manager, closing the DuckDB connection."""
        self.close()

    def __del__(self) -> None:
        """Release the DuckDB connection on garbage collection."""
        self.close()

    @property
    def name(self) -> str:
        """Return ``'duckdb'``."""
        return "duckdb"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _execute_sql(
        self,
        sql: str,
        views: dict[str, Any],
        params: list[Any] | None = None,
    ) -> pd.DataFrame:
        """Register *views*, execute *sql*, unregister, and return the result.

        Accepts both ``pd.DataFrame`` and ``DuckDBLazyFrame`` as view values.
        Lazy frames are materialised before registration.

        Args:
            sql: The SQL statement to execute.
            views: Mapping of view name → frame to register before execution.
            params: Optional positional parameters for the SQL statement
                (referenced as ``$1``, ``$2``, … in the query).

        Returns:
            The query result as a pandas DataFrame.

        """
        for view_name, frame in views.items():
            self._conn.register(view_name, _to_df(frame))
        try:
            result: pd.DataFrame = self._conn.execute(
                sql,
                params or [],
            ).fetchdf()
        finally:
            for view_name in views:
                try:
                    self._conn.unregister(view_name)
                except Exception:  # noqa: BLE001 — best-effort view cleanup
                    LOGGER.debug(
                        "Failed to unregister DuckDB view %r", view_name,
                        exc_info=True,
                    )
        return result

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: SourceObject,
        entity_type: str,
    ) -> pd.DataFrame:
        """Register source in DuckDB and return ID column."""
        df = _to_pandas(source_obj)
        view_name = f"_entity_{validate_identifier(entity_type)}"
        return self._execute_sql(
            f'SELECT "{ID_COLUMN}" FROM "{view_name}"',  # nosec B608 — view_name validated by validate_identifier
            {view_name: df},
        )

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def filter(self, frame: pd.DataFrame | DuckDBLazyFrame, mask: BackendMask) -> pd.DataFrame:
        """Boolean mask filter — delegates to pandas.

        Mask-based filtering cannot be expressed as lazy DuckDB SQL
        because the mask is computed externally as a numpy array.
        """
        return _to_df(frame).loc[mask].reset_index(drop=True)

    def join(
        self,
        left: Any,
        right: Any,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> pd.DataFrame:
        """Join via DuckDB SQL for optimal join strategy selection.

        The *strategy* parameter is accepted for protocol compatibility but
        ignored — DuckDB's query planner selects the optimal join algorithm
        internally based on table statistics.
        """
        left_df = _to_df(left)
        right_df = _to_df(right)
        lv = self._next_view("_jl")
        rv = self._next_view("_jr")

        if how == "cross":
            sql = f'SELECT * FROM "{lv}" CROSS JOIN "{rv}"'
        else:
            if isinstance(on, str):
                on = [on]
            for col in on:
                validate_identifier(col)
            join_cond = " AND ".join(
                f'"{lv}"."{col}" = "{rv}"."{col}"' for col in on
            )
            join_type = {"inner": "INNER", "left": "LEFT"}.get(how, "INNER")

            right_cols = [c for c in right_df.columns if c not in on]
            select_right = ", ".join(
                f'"{rv}"."{validate_identifier(c)}"' for c in right_cols
            )
            select_clause = f'"{lv}".*'
            if select_right:
                select_clause += f", {select_right}"

            sql = (
                f"SELECT {select_clause} "  # nosec B608 — all column names validated by validate_identifier
                f'FROM "{lv}" {join_type} JOIN "{rv}" ON {join_cond}'
            )

        return self._execute_sql(sql, {lv: left_df, rv: right_df})

    def rename(
        self,
        frame: Any,
        columns: dict[str, str],
    ) -> pd.DataFrame:
        """Rename columns — delegates to pandas (no SQL benefit)."""
        return _to_df(frame).rename(columns=columns)

    def concat(
        self,
        frames: list[Any],
        *,
        ignore_index: bool = True,
    ) -> pd.DataFrame:
        """Concatenate via pandas."""
        return pd.concat(
            [_to_df(f) for f in frames],
            ignore_index=ignore_index,
        )

    def distinct(self, frame: Any) -> pd.DataFrame:
        """Remove duplicate rows via DuckDB."""
        return self._execute_sql(
            "SELECT DISTINCT * FROM _distinct_input",
            {"_distinct_input": frame},
        )

    def assign_column(
        self,
        frame: Any,
        name: str,
        values: Any,
    ) -> pd.DataFrame:
        """Add or replace a column — delegates to pandas."""
        return _to_df(frame).assign(**{name: values})

    def drop_columns(
        self,
        frame: Any,
        columns: list[str],
    ) -> pd.DataFrame:
        """Drop columns, ignoring missing names."""
        df = _to_df(frame)
        existing = [c for c in columns if c in df.columns]
        if not existing:
            return df
        return df.drop(columns=existing)

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate(
        self,
        frame: Any,
        group_cols: list[str],
        agg_specs: dict[str, tuple[str, str]],
    ) -> pd.DataFrame:
        """Aggregation via DuckDB SQL."""
        agg_exprs = []
        for out_col, (src_col, func) in agg_specs.items():
            sql_func = _pandas_agg_to_sql(func)
            validate_identifier(src_col)
            validate_identifier(out_col)
            agg_exprs.append(f'{sql_func}("{src_col}") AS "{out_col}"')

        if group_cols:
            for col in group_cols:
                validate_identifier(col)
            group_clause = ", ".join(f'"{c}"' for c in group_cols)
            select = f"{group_clause}, {', '.join(agg_exprs)}"
            sql = f"SELECT {select} FROM _agg_input GROUP BY {group_clause}"  # nosec B608 — cols validated by validate_identifier
        else:
            sql = f"SELECT {', '.join(agg_exprs)} FROM _agg_input"  # nosec B608 — cols validated by validate_identifier

        return self._execute_sql(sql, {"_agg_input": frame})

    # ------------------------------------------------------------------
    # Order
    # ------------------------------------------------------------------

    def sort(
        self,
        frame: Any,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> DuckDBLazyFrame:
        """Sort via DuckDB — returns lazy for sort+limit fusion.

        Returns a ``DuckDBLazyFrame`` so that a subsequent ``limit()``
        can compose ORDER BY + LIMIT into a single DuckDB query instead
        of materialising the full sort result first.

        The ``DuckDBLazyFrame`` auto-materialises when accessed via pandas
        methods, so callers that don't chain ``limit()`` still get correct
        results transparently.
        """
        if ascending is None:
            ascending = [True] * len(by)

        vn = self._next_view("_sort")
        order_clauses = []
        for col, asc in zip(by, ascending, strict=True):
            validate_identifier(col)
            direction = "ASC" if asc else "DESC"
            order_clauses.append(f'"{col}" {direction}')

        sql = (
            f'SELECT * FROM "{vn}" '  # nosec B608 — cols validated
            f"ORDER BY {', '.join(order_clauses)}"
        )
        self._conn.register(vn, _to_df(frame))
        relation = self._conn.sql(sql)
        return DuckDBLazyFrame(relation, self._conn, backend=self)

    def limit(self, frame: Any, n: int) -> pd.DataFrame:
        """Limit via DuckDB.

        When *frame* is a ``DuckDBLazyFrame`` (e.g. from ``sort()``),
        the LIMIT is composed into the existing DuckDB relation,
        enabling ORDER BY + LIMIT fusion.
        """
        if not isinstance(n, int) or n < 0:
            msg = f"limit n must be a non-negative integer, got {n!r}"
            raise ValueError(msg)
        if _is_lazy(frame):
            return frame.relation.limit(n).fetchdf()
        return self._execute_sql(
            "SELECT * FROM _limit_input LIMIT $1",
            {"_limit_input": frame},
            params=[n],
        )

    def skip(self, frame: Any, n: int) -> pd.DataFrame:
        """Skip first *n* rows via DuckDB."""
        if not isinstance(n, int) or n < 0:
            msg = f"skip n must be a non-negative integer, got {n!r}"
            raise ValueError(msg)
        return self._execute_sql(
            "SELECT * FROM _skip_input OFFSET $1",
            {"_skip_input": _to_df(frame)},
            params=[n],
        )

    # ------------------------------------------------------------------
    # Materialise / inspect
    # ------------------------------------------------------------------

    def to_pandas(self, frame: Any) -> pd.DataFrame:
        """Materialise — executes the DuckDB query DAG if lazy."""
        if _is_lazy(frame):
            return frame.to_pandas()
        if isinstance(frame, pd.DataFrame):
            return frame
        return _to_pandas(frame)

    def row_count(self, frame: Any) -> int:
        """Row count — uses DuckDB COUNT(*) for lazy frames."""
        return len(frame)

    def is_empty(self, frame: Any) -> bool:
        """Check if frame has zero rows."""
        return len(frame) == 0

    def memory_estimate_bytes(self, frame: Any) -> int:
        """Estimate memory usage."""
        return int(_to_df(frame).memory_usage(deep=True).sum())
