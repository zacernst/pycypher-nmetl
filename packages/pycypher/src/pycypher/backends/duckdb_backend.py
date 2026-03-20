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


class DuckDBBackend:
    """DuckDB-based backend for analytical workloads.

    Uses DuckDB's in-process OLAP engine for efficient columnar operations.
    Particularly effective for:

    - Large aggregations (GROUP BY with many groups)
    - Join optimisation (DuckDB's query planner handles strategy selection)
    - Sort/limit composition (DuckDB fuses ORDER BY + LIMIT efficiently)

    Note: This backend currently materialises to pandas via ``fetchdf()`` at
    each operation boundary.  True lazy DuckDB relation composition requires
    a follow-up where operations build a query DAG and materialise once at
    ``to_pandas()``.
    """

    def __init__(self) -> None:
        """Create a new DuckDB backend with an in-memory connection.

        The connection is created immediately and held for the lifetime of
        the backend.  Use as a context manager or call :meth:`close` to
        release the underlying DuckDB resources.
        """
        import duckdb

        self._conn: Any = duckdb.connect(":memory:")

    # -- Context manager & cleanup -----------------------------------------

    def close(self) -> None:
        """Explicitly close the DuckDB connection.

        Safe to call multiple times — subsequent calls are no-ops.
        """
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                LOGGER.warning(
                    "DuckDB connection close raised; ignoring", exc_info=True
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
        views: dict[str, pd.DataFrame],
        params: list[Any] | None = None,
    ) -> pd.DataFrame:
        """Register *views*, execute *sql*, unregister, and return the result.

        This eliminates the repetitive register/try/execute/finally/unregister
        boilerplate that appears in every SQL-delegated method.

        Args:
            sql: The SQL statement to execute.
            views: Mapping of view name → DataFrame to register before execution.
            params: Optional positional parameters for the SQL statement
                (referenced as ``$1``, ``$2``, … in the query).

        Returns:
            The query result as a pandas DataFrame.

        """
        for view_name, df in views.items():
            self._conn.register(view_name, df)
        try:
            result: pd.DataFrame = self._conn.execute(
                sql,
                params or [],
            ).fetchdf()
        finally:
            for view_name in views:
                self._conn.unregister(view_name)
        return result

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: Any,
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

    def filter(self, frame: pd.DataFrame, mask: Any) -> pd.DataFrame:
        """Boolean mask filter — delegates to pandas.

        DuckDB predicate pushdown requires a future query-DAG layer where
        filter predicates are composed into the scan SQL.
        """
        return frame.loc[mask].reset_index(drop=True)

    def join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> pd.DataFrame:
        """Join via DuckDB SQL for optimal join strategy selection.

        The *strategy* parameter is accepted for protocol compatibility but
        ignored — DuckDB's query planner selects the optimal join algorithm
        internally based on table statistics.
        """
        if how == "cross":
            sql = "SELECT * FROM _left CROSS JOIN _right"
        else:
            if isinstance(on, str):
                on = [on]
            # Validate column names to prevent SQL injection
            for col in on:
                validate_identifier(col)
            join_cond = " AND ".join(
                f'_left."{col}" = _right."{col}"' for col in on
            )
            join_type = {"inner": "INNER", "left": "LEFT"}.get(how, "INNER")

            right_cols = [c for c in right.columns if c not in on]
            select_right = ", ".join(
                f'_right."{validate_identifier(c)}"' for c in right_cols
            )
            select_clause = "_left.*"
            if select_right:
                select_clause += f", {select_right}"

            sql = (
                f"SELECT {select_clause} "  # nosec B608 — all column names validated by validate_identifier
                f"FROM _left {join_type} JOIN _right ON {join_cond}"
            )

        return self._execute_sql(sql, {"_left": left, "_right": right})

    def rename(
        self,
        frame: pd.DataFrame,
        columns: dict[str, str],
    ) -> pd.DataFrame:
        """Rename columns — delegates to pandas (no SQL benefit)."""
        return frame.rename(columns=columns)

    def concat(
        self,
        frames: list[pd.DataFrame],
        *,
        ignore_index: bool = True,
    ) -> pd.DataFrame:
        """Concatenate via pandas."""
        return pd.concat(frames, ignore_index=ignore_index)

    def distinct(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows via DuckDB."""
        return self._execute_sql(
            "SELECT DISTINCT * FROM _distinct_input",
            {"_distinct_input": frame},
        )

    def assign_column(
        self,
        frame: pd.DataFrame,
        name: str,
        values: Any,
    ) -> pd.DataFrame:
        """Add or replace a column — delegates to pandas."""
        return frame.assign(**{name: values})

    def drop_columns(
        self,
        frame: pd.DataFrame,
        columns: list[str],
    ) -> pd.DataFrame:
        """Drop columns, ignoring missing names."""
        existing = [c for c in columns if c in frame.columns]
        if not existing:
            return frame
        return frame.drop(columns=existing)

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate(
        self,
        frame: pd.DataFrame,
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
        frame: pd.DataFrame,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> pd.DataFrame:
        """Sort via DuckDB."""
        if ascending is None:
            ascending = [True] * len(by)

        order_clauses = []
        for col, asc in zip(by, ascending, strict=True):
            validate_identifier(col)
            direction = "ASC" if asc else "DESC"
            order_clauses.append(f'"{col}" {direction}')

        sql = f"SELECT * FROM _sort_input ORDER BY {', '.join(order_clauses)}"  # nosec B608 — cols validated by validate_identifier
        return self._execute_sql(sql, {"_sort_input": frame})

    def limit(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Limit via DuckDB."""
        if not isinstance(n, int) or n < 0:
            msg = f"limit n must be a non-negative integer, got {n!r}"
            raise ValueError(msg)
        return self._execute_sql(
            "SELECT * FROM _limit_input LIMIT $1",
            {"_limit_input": frame},
            params=[n],
        )

    def skip(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Skip first *n* rows via DuckDB."""
        if not isinstance(n, int) or n < 0:
            msg = f"skip n must be a non-negative integer, got {n!r}"
            raise ValueError(msg)
        return self._execute_sql(
            "SELECT * FROM _skip_input OFFSET $1",
            {"_skip_input": frame},
            params=[n],
        )

    # ------------------------------------------------------------------
    # Materialise / inspect
    # ------------------------------------------------------------------

    def to_pandas(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return a copy so callers cannot mutate backend state."""
        return frame.copy()

    def row_count(self, frame: pd.DataFrame) -> int:
        """Row count."""
        return len(frame)

    def is_empty(self, frame: pd.DataFrame) -> bool:
        """Check if frame has zero rows."""
        return len(frame) == 0

    def memory_estimate_bytes(self, frame: pd.DataFrame) -> int:
        """Estimate memory usage."""
        return int(frame.memory_usage(deep=True).sum())
