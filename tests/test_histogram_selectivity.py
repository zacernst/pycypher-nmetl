"""Tests for histogram-based range selectivity estimation.

Verifies:
1. Histogram construction during column statistics computation.
2. Histogram-based selectivity is more accurate than uniform assumption.
3. Graceful fallback to uniform when histogram not available.
4. Edge cases (single bin, out-of-range queries, skewed data).

Run with:
    uv run pytest tests/test_histogram_selectivity.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.cardinality_estimator import ColumnStatistics, TableStatistics


class TestColumnStatisticsHistogram:
    """Test ColumnStatistics with histogram data."""

    def test_uniform_fallback_without_histogram(self) -> None:
        """Without histogram, uses uniform distribution."""
        stats = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=1000,
        )
        # Uniform: (50 - 0) / (100 - 0) = 0.5
        sel = stats.range_selectivity(low=0.0, high=50.0)
        assert abs(sel - 0.5) < 0.01

    def test_histogram_selectivity_uniform_data(self) -> None:
        """Histogram on uniform data should give similar results to uniform."""
        # 4 bins, each with 25 rows
        edges = (0.0, 25.0, 50.0, 75.0, 100.0)
        counts = (25, 25, 25, 25)
        stats = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=100,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        sel = stats.range_selectivity(low=0.0, high=50.0)
        assert abs(sel - 0.5) < 0.01

    def test_histogram_selectivity_skewed_data(self) -> None:
        """Histogram captures data skew that uniform misses."""
        # Most data concentrated in first bin
        edges = (0.0, 25.0, 50.0, 75.0, 100.0)
        counts = (90, 5, 3, 2)  # 100 total, heavily left-skewed
        stats = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=100,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        # Query: col > 50 → bins [50,75) and [75,100)
        sel = stats.range_selectivity(low=50.0, high=100.0)
        # Histogram: (3 + 2) / 100 = 0.05
        assert abs(sel - 0.05) < 0.01

        # Uniform would give 0.5 — much less accurate
        stats_no_hist = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=100,
        )
        sel_uniform = stats_no_hist.range_selectivity(low=50.0, high=100.0)
        assert abs(sel_uniform - 0.5) < 0.01
        # Histogram-based is 10x more accurate for this distribution
        assert sel < sel_uniform

    def test_partial_bin_overlap(self) -> None:
        """Correctly handles partial bin overlap."""
        edges = (0.0, 50.0, 100.0)
        counts = (80, 20)
        stats = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=100,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        # Query: 25 to 75 → half of bin 0 (40 rows) + half of bin 1 (10 rows) = 50/100
        sel = stats.range_selectivity(low=25.0, high=75.0)
        assert abs(sel - 0.5) < 0.01

    def test_null_fraction_applied(self) -> None:
        """Null fraction is applied on top of histogram selectivity."""
        edges = (0.0, 100.0)
        counts = (80,)  # 80 non-null rows
        stats = ColumnStatistics(
            ndv=80,
            null_fraction=0.2,
            min_value=0.0,
            max_value=100.0,
            row_count=100,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        # Full range: histogram gives 1.0, times (1 - 0.2) = 0.8
        sel = stats.range_selectivity(low=0.0, high=100.0)
        assert abs(sel - 0.8) < 0.01

    def test_out_of_range_query(self) -> None:
        """Query range outside data range returns minimum selectivity."""
        edges = (10.0, 50.0, 90.0)
        counts = (50, 50)
        stats = ColumnStatistics(
            ndv=100,
            null_fraction=0.0,
            min_value=10.0,
            max_value=90.0,
            row_count=100,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        # Query: 200 to 300 — completely outside range
        sel = stats.range_selectivity(low=200.0, high=300.0)
        assert sel == pytest.approx(1.0 / 100)  # Minimum selectivity

    def test_empty_histogram_counts(self) -> None:
        """All-zero histogram falls back gracefully."""
        edges = (0.0, 50.0, 100.0)
        counts = (0, 0)
        stats = ColumnStatistics(
            ndv=0,
            null_fraction=0.0,
            min_value=0.0,
            max_value=100.0,
            row_count=0,
            histogram_edges=edges,
            histogram_counts=counts,
        )
        sel = stats.range_selectivity(low=0.0, high=50.0)
        # Should use fallback, not crash
        assert 0.0 < sel <= 1.0


class TestTableStatisticsHistogramComputation:
    """Test that TableStatistics builds histograms for numeric columns."""

    def test_numeric_column_gets_histogram(self) -> None:
        """Numeric column with enough rows gets histogram."""
        df = pd.DataFrame({"age": list(range(100))})
        ts = TableStatistics(df)
        stats = ts.column_stats("age")
        assert stats is not None
        assert stats.histogram_edges is not None
        assert stats.histogram_counts is not None
        assert len(stats.histogram_counts) > 0
        assert len(stats.histogram_edges) == len(stats.histogram_counts) + 1
        assert sum(stats.histogram_counts) == 100

    def test_string_column_no_histogram(self) -> None:
        """String columns don't get histograms."""
        df = pd.DataFrame({"name": [f"user_{i}" for i in range(100)]})
        ts = TableStatistics(df)
        stats = ts.column_stats("name")
        assert stats is not None
        assert stats.histogram_edges is None
        assert stats.histogram_counts is None

    def test_small_numeric_column_no_histogram(self) -> None:
        """Too-small numeric columns don't get histograms."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        ts = TableStatistics(df)
        stats = ts.column_stats("x")
        assert stats is not None
        # Below _HISTOGRAM_MIN_ROWS threshold
        assert stats.histogram_edges is None

    def test_single_value_column_no_histogram(self) -> None:
        """Constant column (min == max) doesn't get histogram."""
        df = pd.DataFrame({"x": [42] * 100})
        ts = TableStatistics(df)
        stats = ts.column_stats("x")
        assert stats is not None
        assert stats.histogram_edges is None

    def test_skewed_data_histogram_accuracy(self) -> None:
        """Histogram on skewed data gives better selectivity than uniform."""
        # Exponentially distributed data — heavy left skew
        rng = np.random.default_rng(42)
        values = rng.exponential(scale=10.0, size=10000)
        df = pd.DataFrame({"val": values})
        ts = TableStatistics(df)
        stats = ts.column_stats("val")
        assert stats is not None
        assert stats.histogram_edges is not None

        # Query for upper tail (should be small fraction)
        median = float(np.median(values))
        max_val = stats.max_value
        assert max_val is not None
        sel_hist = stats.range_selectivity(low=median * 3, high=max_val)

        # With uniform assumption this would be ~0.25, but exponential
        # has thin upper tail, so histogram should estimate much lower
        stats_uniform = ColumnStatistics(
            ndv=stats.ndv,
            null_fraction=0.0,
            min_value=stats.min_value,
            max_value=stats.max_value,
            row_count=stats.row_count,
        )
        sel_uniform = stats_uniform.range_selectivity(
            low=median * 3, high=max_val
        )

        # Histogram estimate should be smaller (more accurate for thin tail)
        assert sel_hist < sel_uniform
