"""Polars-based backend for single-machine scaling (1GB–50GB).

Uses Polars' Apache Arrow-native columnar format with lazy evaluation
and query optimisation.  Key advantages over pandas:

- **Arrow-native**: Zero-copy interop with Arrow, Parquet, IPC.
- **Lazy evaluation**: Builds a query plan, optimises, then executes.
- **Multi-threaded**: Automatic parallelism across CPU cores.
- **Memory-efficient**: Columnar + lazy means only materialised data
  is held in memory.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from pycypher.backends._helpers import _polars_agg_func, _to_pandas
from pycypher.constants import ID_COLUMN


class PolarsBackend:
    """Polars-based backend for single-machine scaling (1GB–50GB).

    Uses Polars' Apache Arrow-native columnar format with lazy evaluation
    and query optimisation.  Key advantages over pandas:

    - **Arrow-native**: Zero-copy interop with Arrow, Parquet, IPC.
    - **Lazy evaluation**: Builds a query plan, optimises, then executes.
    - **Multi-threaded**: Automatic parallelism across CPU cores.
    - **Memory-efficient**: Columnar + lazy means only materialised data
      is held in memory.

    The backend accepts pandas DataFrames at API boundaries (to match the
    protocol) and converts internally.  For maximum performance, future
    work will accept Polars LazyFrames directly from data sources.
    """

    def __init__(self) -> None:
        """Create a new Polars backend.

        Imports the ``polars`` library on first use and stores a module
        reference for internal conversion between pandas and Polars frames.
        """
        import polars as pl

        self._pl = pl

    @property
    def name(self) -> str:
        """Return ``'polars'``."""
        return "polars"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _to_pl(self, frame: pd.DataFrame) -> Any:
        """Convert a pandas DataFrame to a Polars DataFrame."""
        return self._pl.from_pandas(frame)

    def _from_pl(self, pl_frame: Any) -> pd.DataFrame:
        """Convert a Polars DataFrame to a pandas DataFrame."""
        return pl_frame.to_pandas()

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def scan_entity(
        self,
        source_obj: Any,
        entity_type: str,
    ) -> pd.DataFrame:
        """Extract ID column from source."""
        df = _to_pandas(source_obj)
        return df[[ID_COLUMN]]

    # ------------------------------------------------------------------
    # Transform
    # ------------------------------------------------------------------

    def filter(self, frame: pd.DataFrame, mask: Any) -> pd.DataFrame:
        """Boolean mask filter via Polars."""
        pl_frame = self._to_pl(frame)
        pl_mask = self._pl.Series(mask)
        return self._from_pl(pl_frame.filter(pl_mask))

    def join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        on: str | list[str],
        how: str = "inner",
        strategy: str = "auto",
    ) -> pd.DataFrame:
        """Join via Polars.

        The *strategy* parameter is accepted for protocol compatibility but
        ignored — Polars' query engine handles join optimisation internally.
        """
        pl_left = self._to_pl(left)
        pl_right = self._to_pl(right)

        if how == "cross":
            result = pl_left.join(pl_right, how="cross")
        else:
            if isinstance(on, str):
                on = [on]
            result = pl_left.join(pl_right, on=on, how=how)

        return self._from_pl(result)

    def rename(
        self,
        frame: pd.DataFrame,
        columns: dict[str, str],
    ) -> pd.DataFrame:
        """Rename columns via Polars."""
        return self._from_pl(self._to_pl(frame).rename(columns))

    def concat(
        self,
        frames: list[pd.DataFrame],
        *,
        ignore_index: bool = True,
    ) -> pd.DataFrame:
        """Concatenate via Polars."""
        pl_frames = [self._to_pl(f) for f in frames]
        return self._from_pl(self._pl.concat(pl_frames))

    def distinct(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows via Polars."""
        return self._from_pl(self._to_pl(frame).unique())

    def assign_column(
        self,
        frame: pd.DataFrame,
        name: str,
        values: Any,
    ) -> pd.DataFrame:
        """Add or replace a column via Polars."""
        pl = self._pl
        pl_frame = self._to_pl(frame)
        if isinstance(values, pd.Series):
            col = pl.Series(name, values.to_list())
        elif isinstance(values, (list, range)):
            col = pl.Series(name, list(values))
        else:
            # Scalar broadcast
            col = pl.Series(name, [values] * len(pl_frame))
        return self._from_pl(pl_frame.with_columns(col))

    def drop_columns(
        self,
        frame: pd.DataFrame,
        columns: list[str],
    ) -> pd.DataFrame:
        """Drop columns, ignoring missing names."""
        pl_frame = self._to_pl(frame)
        existing = [c for c in columns if c in pl_frame.columns]
        if not existing:
            return frame
        return self._from_pl(pl_frame.drop(existing))

    # ------------------------------------------------------------------
    # Aggregate
    # ------------------------------------------------------------------

    def aggregate(
        self,
        frame: pd.DataFrame,
        group_cols: list[str],
        agg_specs: dict[str, tuple[str, str]],
    ) -> pd.DataFrame:
        """Grouped aggregation via Polars."""
        pl = self._pl
        pl_frame = self._to_pl(frame)

        agg_exprs = []
        for out_col, (src_col, func) in agg_specs.items():
            col_expr = pl.col(src_col)
            agg_fn = _polars_agg_func(col_expr, func)
            agg_exprs.append(agg_fn.alias(out_col))

        if not group_cols:
            result = pl_frame.select(agg_exprs)
        else:
            result = pl_frame.group_by(group_cols).agg(agg_exprs)

        return self._from_pl(result)

    # ------------------------------------------------------------------
    # Order
    # ------------------------------------------------------------------

    def sort(
        self,
        frame: pd.DataFrame,
        by: list[str],
        ascending: list[bool] | None = None,
    ) -> pd.DataFrame:
        """Sort via Polars."""
        if ascending is None:
            ascending = [True] * len(by)
        pl_frame = self._to_pl(frame)
        descending = [not a for a in ascending]
        return self._from_pl(pl_frame.sort(by, descending=descending))

    def limit(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Return first *n* rows via Polars."""
        return self._from_pl(self._to_pl(frame).head(n))

    def skip(self, frame: pd.DataFrame, n: int) -> pd.DataFrame:
        """Skip first *n* rows via Polars."""
        return self._from_pl(self._to_pl(frame).slice(n))

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
