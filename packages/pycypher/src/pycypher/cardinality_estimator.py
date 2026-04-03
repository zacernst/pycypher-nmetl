"""Column and table statistics for cardinality estimation.

Provides reusable statistical primitives that can be consumed by the query
planner, query plan analyzer, or any other component that needs selectivity
estimates.  Extracted from ``query_planner.py`` to reduce module coupling
and enable independent testing.

Key classes:

- :class:`ColumnStatistics` — per-column NDV, null fraction, histograms.
- :class:`TableStatistics` — lazily computed column statistics for a table.
- :class:`CardinalityFeedbackStore` — accumulates actual-vs-estimated
  ratios for self-correcting estimates.

Usage::

    stats = TableStatistics(df)
    col = stats.column_stats("age")
    if col is not None:
        sel = col.equality_selectivity()  # 1/NDV adjusted for nulls
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from shared.logger import LOGGER

__all__ = [
    "CardinalityFeedbackStore",
    "ColumnStatistics",
    "TableStatistics",
]

# ---------------------------------------------------------------------------
# Tuning constants
# ---------------------------------------------------------------------------

#: Maximum number of rows to sample when computing column statistics.
STATS_SAMPLE_SIZE: int = 10_000

#: Number of equi-width bins for histogram-based range selectivity.
HISTOGRAM_BINS: int = 64

#: Minimum non-null rows required to build a histogram.
HISTOGRAM_MIN_ROWS: int = 10

#: Default selectivity factor for WHERE predicates when no statistics are
#: available.  Assumes an equality filter keeps ~33% of rows.
DEFAULT_FILTER_SELECTIVITY: float = 0.33

#: Average bytes per cell for memory estimation when the actual DataFrame
#: is not available.  Conservative estimate for mixed-type columns.
AVG_BYTES_PER_CELL: int = 64

#: Maximum rolling window per entity type in the feedback store.
_MAX_HISTORY: int = 32


# ---------------------------------------------------------------------------
# Column statistics
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column, used for selectivity estimation.

    Attributes:
        ndv: Number of distinct values (excluding nulls).
        null_fraction: Fraction of rows that are null (0.0-1.0).
        min_value: Minimum non-null value (numeric columns only).
        max_value: Maximum non-null value (numeric columns only).
        row_count: Total rows in the table at time of collection.
        histogram_edges: Bin edges for equi-width histogram (numeric only).
            Length is ``num_bins + 1``.
        histogram_counts: Row counts per histogram bin (numeric only).
            Length is ``num_bins``.

    """

    ndv: int
    null_fraction: float
    min_value: float | None = None
    max_value: float | None = None
    row_count: int = 0
    histogram_edges: tuple[float, ...] | None = None
    histogram_counts: tuple[int, ...] | None = None

    def equality_selectivity(self) -> float:
        """Selectivity for ``col = value``: 1/NDV, adjusted for nulls."""
        if self.ndv <= 0:
            return DEFAULT_FILTER_SELECTIVITY
        return (1.0 - self.null_fraction) / self.ndv

    def range_selectivity(
        self,
        low: float | None = None,
        high: float | None = None,
    ) -> float:
        """Selectivity for range predicates (``col > low``, ``col < high``).

        When a histogram is available, estimates selectivity by summing the
        fraction of rows in bins that overlap the query range.  Falls back
        to a uniform distribution assumption when no histogram is present.
        """
        if self.min_value is None or self.max_value is None:
            return DEFAULT_FILTER_SELECTIVITY
        span = self.max_value - self.min_value
        if span <= 0:
            return DEFAULT_FILTER_SELECTIVITY

        lo = low if low is not None else self.min_value
        hi = high if high is not None else self.max_value
        lo = max(lo, self.min_value)
        hi = min(hi, self.max_value)

        if lo >= hi:
            return 1.0 / max(self.row_count, 1)

        # Use histogram when available for more accurate estimation.
        if (
            self.histogram_edges is not None
            and self.histogram_counts is not None
            and len(self.histogram_counts) > 0
        ):
            sel = self._histogram_range_selectivity(lo, hi)
        else:
            # Uniform distribution fallback.
            sel = (hi - lo) / span

        sel *= 1.0 - self.null_fraction
        return max(sel, 1.0 / max(self.row_count, 1))

    def _histogram_range_selectivity(
        self,
        lo: float,
        hi: float,
    ) -> float:
        """Estimate range selectivity from histogram bins.

        For each bin that overlaps [lo, hi], count the proportional fraction
        of its rows that fall within the range.
        """
        assert self.histogram_edges is not None
        assert self.histogram_counts is not None

        edges = self.histogram_edges
        counts = self.histogram_counts
        total_rows = sum(counts)
        if total_rows == 0:
            return DEFAULT_FILTER_SELECTIVITY

        matching_rows = 0.0
        for i, count in enumerate(counts):
            bin_lo = edges[i]
            bin_hi = edges[i + 1]
            bin_width = bin_hi - bin_lo
            if bin_width <= 0 or count == 0:
                continue

            # Compute overlap between [lo, hi] and [bin_lo, bin_hi].
            overlap_lo = max(lo, bin_lo)
            overlap_hi = min(hi, bin_hi)
            if overlap_lo >= overlap_hi:
                continue

            # Fraction of this bin covered by the query range.
            fraction = (overlap_hi - overlap_lo) / bin_width
            matching_rows += count * fraction

        return matching_rows / total_rows


# ---------------------------------------------------------------------------
# Table statistics
# ---------------------------------------------------------------------------


class TableStatistics:
    """Collects and caches column-level statistics for an entity or
    relationship table.

    Statistics are computed lazily on first access and cached.  For large
    tables, a random sample of ``STATS_SAMPLE_SIZE`` rows is used.
    """

    def __init__(self, source_obj: pd.DataFrame | Any) -> None:
        self._source = source_obj
        self._columns: dict[str, ColumnStatistics] = {}
        self._row_count: int | None = None

    @property
    def row_count(self) -> int:
        """Return the number of rows in the source table."""
        if self._row_count is None:
            if hasattr(self._source, "__len__"):
                self._row_count = len(self._source)
            else:
                self._row_count = 0
        return self._row_count

    def column_stats(self, column: str) -> ColumnStatistics | None:
        """Return cached statistics for *column*, computing on first call."""
        if column in self._columns:
            return self._columns[column]
        stats = self._compute_column_stats(column)
        if stats is not None:
            self._columns[column] = stats
        return stats

    def _compute_column_stats(self, column: str) -> ColumnStatistics | None:
        """Compute statistics for a single column from the source data."""
        try:
            if isinstance(self._source, pd.DataFrame):
                df = self._source
            elif hasattr(self._source, "to_pandas"):
                df = self._source.to_pandas()
            else:
                return None

            if column not in df.columns:
                return None

            # Sample for large tables
            n = len(df)
            if n > STATS_SAMPLE_SIZE:
                sample = df[column].sample(
                    n=STATS_SAMPLE_SIZE,
                    random_state=42,
                )
            else:
                sample = df[column]

            null_count = int(sample.isna().sum())
            null_fraction = null_count / max(len(sample), 1)
            non_null = sample.dropna()
            ndv = int(non_null.nunique())

            min_val: float | None = None
            max_val: float | None = None
            hist_edges: tuple[float, ...] | None = None
            hist_counts: tuple[int, ...] | None = None
            if len(non_null) > 0 and pd.api.types.is_numeric_dtype(non_null):
                min_val = float(non_null.min())
                max_val = float(non_null.max())
                # Build equi-width histogram for range selectivity.
                if min_val < max_val and len(non_null) >= HISTOGRAM_MIN_ROWS:
                    num_bins = min(HISTOGRAM_BINS, ndv)
                    try:
                        counts, edges = np.histogram(
                            non_null.values,
                            bins=num_bins,
                        )
                        hist_edges = tuple(float(e) for e in edges)
                        hist_counts = tuple(int(c) for c in counts)
                    except (ValueError, TypeError):
                        pass  # Non-histogrammable data; use uniform fallback.

            return ColumnStatistics(
                ndv=ndv,
                null_fraction=null_fraction,
                min_value=min_val,
                max_value=max_val,
                row_count=n,
                histogram_edges=hist_edges,
                histogram_counts=hist_counts,
            )
        except (TypeError, ValueError, ArithmeticError) as _stats_exc:
            LOGGER.debug(
                "Failed to compute statistics for column %r: %s",
                column,
                _stats_exc,
                exc_info=True,
            )
            return None


# ---------------------------------------------------------------------------
# CardinalityFeedbackStore — learns from execution history
# ---------------------------------------------------------------------------


class CardinalityFeedbackStore:
    """Accumulates actual vs estimated cardinality ratios per entity type.

    After each query execution, call :meth:`record` with the entity types
    involved and the (estimated, actual) row counts.  Before a future
    estimate, call :meth:`correction_factor` to get a multiplicative
    adjustment derived from historical accuracy.

    Thread-safe via a simple lock.  History is bounded to the most recent
    ``_MAX_HISTORY`` observations per entity type.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # entity_type -> deque of (estimated, actual) tuples
        self._history: dict[str, deque[tuple[int, int]]] = {}

    def record(
        self,
        entity_type: str,
        estimated: int,
        actual: int,
    ) -> None:
        """Record an (estimated, actual) observation for *entity_type*."""
        if estimated <= 0 and actual <= 0:
            return
        with self._lock:
            if entity_type not in self._history:
                self._history[entity_type] = deque(maxlen=_MAX_HISTORY)
            self._history[entity_type].append((estimated, actual))

    def correction_factor(self, entity_type: str) -> float:
        """Return a multiplicative correction for *entity_type*.

        If the estimator consistently overestimates by 2x, this returns
        ~0.5 so the caller can multiply the heuristic estimate by it.
        Returns 1.0 when no history is available.
        """
        with self._lock:
            history = self._history.get(entity_type)
            if not history:
                return 1.0

        # Compute mean(actual / estimated) with clamp.
        ratios = [act / max(est, 1) for est, act in history]
        avg_ratio = sum(ratios) / len(ratios)
        # Clamp to [0.01, 100] to prevent runaway corrections.
        return max(0.01, min(100.0, avg_ratio))

    @property
    def entity_types_tracked(self) -> list[str]:
        """Return entity types with recorded history."""
        with self._lock:
            return list(self._history.keys())

    def clear(self) -> None:
        """Drop all recorded history."""
        with self._lock:
            self._history.clear()
