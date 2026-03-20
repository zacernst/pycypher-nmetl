"""Pandas-based backend — wraps existing pandas operations.

This is the default backend and provides identical behaviour to the
current codebase.  It serves as:

1. The reference implementation for the ``BackendEngine`` protocol.
2. The fallback when other backends are unavailable or inappropriate.
3. A zero-cost abstraction — no overhead compared to raw pandas calls.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from pycypher.backends._helpers import _to_pandas
from pycypher.constants import ID_COLUMN


class PandasBackend:
    """Pandas-based backend — wraps existing pandas operations.

    This is the default backend and provides identical behaviour to the
    current codebase.  It serves as:

    1. The reference implementation for the ``BackendEngine`` protocol.
    2. The fallback when other backends are unavailable or inappropriate.
    3. A zero-cost abstraction — no overhead compared to raw pandas calls.
    """

    @property
    def name(self) -> str:
        """Return ``'pandas'``."""
        return "pandas"

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: Any,
        entity_type: str,
    ) -> pd.DataFrame:
        """Convert source to pandas and return ID column."""
        df = _to_pandas(source_obj)
        return df[[ID_COLUMN]]

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def filter(self, frame: pd.DataFrame, mask: Any) -> pd.DataFrame:
        """Boolean mask filter."""
        return frame.loc[mask].reset_index(drop=True)

    def join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> pd.DataFrame:
        """Merge via pandas with optional strategy routing.

        Strategy effects on PandasBackend:

        - ``'auto'`` / ``'hash'``: default ``pd.merge`` (uses hash internally).
        - ``'broadcast'``: swap left/right so the smaller side is the build
          table — reduces hash table memory for asymmetric joins.
        - ``'merge'``: sort both sides on the join key first, then merge.
          Optimal when inputs are already sorted.
        """
        if how == "cross":
            return left.merge(right, how="cross")

        if strategy == "broadcast" and len(right) > len(left):
            # Swap so smaller side is right (build side for hash table).
            # For inner joins this is semantically equivalent.
            if how == "inner":
                return right.merge(left, on=on, how="inner")

        if strategy == "merge":
            key = [on] if isinstance(on, str) else on
            left_sorted = left.sort_values(key)
            right_sorted = right.sort_values(key)
            return left_sorted.merge(right_sorted, on=on, how=how)  # type: ignore[arg-type]  # pandas Literal stub

        return left.merge(right, on=on, how=how)  # type: ignore[arg-type]  # pandas Literal stub

    def rename(
        self,
        frame: pd.DataFrame,
        columns: dict[str, str],
    ) -> pd.DataFrame:
        """Rename columns via pandas."""
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
        """Remove duplicate rows via pandas."""
        return frame.drop_duplicates().reset_index(drop=True)

    def assign_column(
        self,
        frame: pd.DataFrame,
        name: str,
        values: Any,
    ) -> pd.DataFrame:
        """Add or replace a column."""
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
        """Grouped aggregation via pandas groupby."""
        if not group_cols:
            result: dict[str, Any] = {}
            for out_col, (src_col, func) in agg_specs.items():
                result[out_col] = [getattr(frame[src_col], func)()]
            return pd.DataFrame(result)

        agg_dict: dict[str, str] = {}
        rename_map: dict[str, str] = {}
        for out_col, (src_col, func) in agg_specs.items():
            agg_dict[src_col] = func
            rename_map[src_col] = out_col

        grouped = frame.groupby(group_cols, sort=False).agg(agg_dict)
        return grouped.rename(columns=rename_map).reset_index()

    # ------------------------------------------------------------------
    # Order
    # ------------------------------------------------------------------

    def sort(
        self,
        frame: pd.DataFrame,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> pd.DataFrame:
        """Sort via pandas."""
        if ascending is None:
            ascending = [True] * len(by)
        return frame.sort_values(by=by, ascending=ascending).reset_index(
            drop=True,
        )

    def limit(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Head via pandas."""
        return frame.head(n)

    def skip(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Skip first *n* rows via pandas."""
        return frame.iloc[n:].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Materialise / inspect
    # ------------------------------------------------------------------

    def to_pandas(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Return a copy so callers cannot mutate backend state."""
        return frame.copy()

    def row_count(self, frame: pd.DataFrame) -> int:
        """len() on DataFrame."""
        return len(frame)

    def is_empty(self, frame: pd.DataFrame) -> bool:
        """Check if DataFrame has zero rows."""
        return len(frame) == 0

    def memory_estimate_bytes(self, frame: pd.DataFrame) -> int:
        """Use pandas memory_usage for estimation."""
        return int(frame.memory_usage(deep=True).sum())
