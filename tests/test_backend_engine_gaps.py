"""Tests targeting uncovered lines in backend_engine.py.

Covers InstrumentedBackend timing, select_backend validation,
_try_create with unknown backends, _to_pandas type errors,
and _polars_agg_func with unsupported function names.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backend_engine import (
    InstrumentedBackend,
    _try_create,
    select_backend,
)
from pycypher.backends._helpers import _polars_agg_func, _to_pandas
from pycypher.backends.pandas_backend import PandasBackend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_frame() -> pd.DataFrame:
    """Return a small test DataFrame."""
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


# ---------------------------------------------------------------------------
# InstrumentedBackend -- rename, concat, distinct, assign_column, drop_columns
# ---------------------------------------------------------------------------


class TestInstrumentedBackendTransformTiming:
    """Lines 1313-1344: instrumented transform operations record timings."""

    def setup_method(self) -> None:
        self.inner = PandasBackend()
        self.backend = InstrumentedBackend(self.inner)

    def test_rename_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.rename(df, {"a": "x"})
        assert "x" in result.columns
        assert "rename" in self.backend.operation_timings
        assert self.backend.operation_counts["rename"] == 1

    def test_concat_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.concat([df, df])
        assert len(result) == 6
        assert "concat" in self.backend.operation_timings
        assert self.backend.operation_counts["concat"] == 1

    def test_distinct_records_timing(self) -> None:
        df = pd.DataFrame({"a": [1, 1, 2], "b": [3, 3, 4]})
        result = self.backend.distinct(df)
        assert len(result) == 2
        assert "distinct" in self.backend.operation_timings

    def test_assign_column_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.assign_column(df, "c", [7, 8, 9])
        assert "c" in result.columns
        assert "assign_column" in self.backend.operation_timings

    def test_drop_columns_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.drop_columns(df, ["b"])
        assert "b" not in result.columns
        assert "drop_columns" in self.backend.operation_timings


# ---------------------------------------------------------------------------
# InstrumentedBackend -- limit, skip, to_pandas
# ---------------------------------------------------------------------------


class TestInstrumentedBackendLimitSkipToPandas:
    """Lines 1376-1395: limit, skip, to_pandas instrumented."""

    def setup_method(self) -> None:
        self.inner = PandasBackend()
        self.backend = InstrumentedBackend(self.inner)

    def test_limit_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.limit(df, 2)
        assert len(result) == 2
        assert "limit" in self.backend.operation_timings

    def test_skip_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.skip(df, 1)
        assert len(result) == 2
        assert "skip" in self.backend.operation_timings

    def test_to_pandas_records_timing(self) -> None:
        df = _make_frame()
        result = self.backend.to_pandas(df)
        assert isinstance(result, pd.DataFrame)
        assert "to_pandas" in self.backend.operation_timings


# ---------------------------------------------------------------------------
# InstrumentedBackend -- row_count, is_empty, memory_estimate_bytes
# ---------------------------------------------------------------------------


class TestInstrumentedBackendPassthrough:
    """Lines 1399, 1403, 1407: trivial pass-through delegates."""

    def setup_method(self) -> None:
        self.inner = PandasBackend()
        self.backend = InstrumentedBackend(self.inner)

    def test_row_count(self) -> None:
        df = _make_frame()
        assert self.backend.row_count(df) == 3

    def test_is_empty_false(self) -> None:
        df = _make_frame()
        assert self.backend.is_empty(df) is False

    def test_is_empty_true(self) -> None:
        df = pd.DataFrame({"a": []})
        assert self.backend.is_empty(df) is True

    def test_memory_estimate_bytes(self) -> None:
        df = _make_frame()
        estimate = self.backend.memory_estimate_bytes(df)
        assert isinstance(estimate, int)
        assert estimate > 0


# ---------------------------------------------------------------------------
# select_backend with invalid hint
# ---------------------------------------------------------------------------


class TestSelectBackendInvalidHint:
    """Lines 1448-1450: unknown hint raises ValueError."""

    def test_invalid_hint_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown backend hint"):
            select_backend(hint="nonexistent_backend")


# ---------------------------------------------------------------------------
# _try_create with unknown backend name
# ---------------------------------------------------------------------------


class TestTryCreateUnknown:
    """Line 1492: factory lookup returns None for unknown names."""

    def test_unknown_backend_returns_none(self) -> None:
        result = _try_create("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# _to_pandas with invalid type
# ---------------------------------------------------------------------------


class TestToPandasInvalidType:
    """Lines 1565-1566: non-convertible type raises TypeError."""

    def test_integer_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_pandas(42)

    def test_string_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Cannot convert"):
            _to_pandas("not a dataframe")

    def test_dataframe_passes_through(self) -> None:
        df = _make_frame()
        result = _to_pandas(df)
        assert result is df


# ---------------------------------------------------------------------------
# _polars_agg_func with unsupported function
# ---------------------------------------------------------------------------


class TestPolarsAggFuncInvalid:
    """Lines 1584-1585: unsupported aggregation name raises ValueError."""

    def test_unknown_func_raises(self) -> None:
        # We need a col_expr-like object; the error is raised before
        # getattr is called, so any object will do.
        with pytest.raises(
            ValueError,
            match="Unsupported aggregation function for Polars",
        ):
            _polars_agg_func(object(), "median_absolute_deviation")
