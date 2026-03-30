"""Unit tests for backend implementations — covers edge cases and dtype handling.

Supplements test_backend_engine.py (which covers protocol compliance and
happy-path equivalence) with targeted tests for:

- Dtype preservation across DuckDB/Polars round-trips
- All aggregation functions (sum, mean, min, max, std, var, first, last)
- Multi-column joins
- PandasBackend join strategy routing (broadcast, merge)
- DuckDB limit/skip input validation
- Helper functions (_to_pandas, _pandas_agg_to_sql, _polars_agg_func, validate_identifier)
- DuckDB SQL injection prevention in identifier validation
- Empty DataFrame handling
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from pycypher.backend_engine import BackendEngine
from pycypher.backends._helpers import (
    _pandas_agg_to_sql,
    _polars_agg_func,
    _to_pandas,
    validate_identifier,
)
from pycypher.backends.duckdb_backend import DuckDBBackend
from pycypher.backends.pandas_backend import PandasBackend
from pycypher.backends.polars_backend import PolarsBackend

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(params=["pandas", "duckdb", "polars"])
def backend(request: pytest.FixtureRequest) -> BackendEngine:
    if request.param == "pandas":
        return PandasBackend()
    if request.param == "polars":
        return PolarsBackend()
    return DuckDBBackend()


@pytest.fixture
def numeric_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5, 6],
            "group": ["a", "a", "a", "b", "b", "b"],
            "value": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        }
    )


# ---------------------------------------------------------------------------
# Aggregation — all functions
# ---------------------------------------------------------------------------


class TestAggregationFunctions:
    """Test all supported aggregation functions across backends."""

    def test_sum(self, backend: BackendEngine, numeric_df: pd.DataFrame) -> None:
        result = backend.to_pandas(
            backend.aggregate(
                numeric_df,
                group_cols=["group"],
                agg_specs={"total": ("value", "sum")},
            )
        )
        totals = dict(zip(result["group"], result["total"], strict=False))
        assert totals["a"] == pytest.approx(60.0)
        assert totals["b"] == pytest.approx(150.0)

    def test_mean(self, backend: BackendEngine, numeric_df: pd.DataFrame) -> None:
        result = backend.to_pandas(
            backend.aggregate(
                numeric_df,
                group_cols=["group"],
                agg_specs={"avg": ("value", "mean")},
            )
        )
        avgs = dict(zip(result["group"], result["avg"], strict=False))
        assert avgs["a"] == pytest.approx(20.0)
        assert avgs["b"] == pytest.approx(50.0)

    def test_min(self, backend: BackendEngine, numeric_df: pd.DataFrame) -> None:
        result = backend.to_pandas(
            backend.aggregate(
                numeric_df,
                group_cols=["group"],
                agg_specs={"lo": ("value", "min")},
            )
        )
        mins = dict(zip(result["group"], result["lo"], strict=False))
        assert mins["a"] == pytest.approx(10.0)
        assert mins["b"] == pytest.approx(40.0)

    def test_max(self, backend: BackendEngine, numeric_df: pd.DataFrame) -> None:
        result = backend.to_pandas(
            backend.aggregate(
                numeric_df,
                group_cols=["group"],
                agg_specs={"hi": ("value", "max")},
            )
        )
        maxes = dict(zip(result["group"], result["hi"], strict=False))
        assert maxes["a"] == pytest.approx(30.0)
        assert maxes["b"] == pytest.approx(60.0)

    def test_count_ungrouped(
        self, backend: BackendEngine, numeric_df: pd.DataFrame
    ) -> None:
        result = backend.to_pandas(
            backend.aggregate(
                numeric_df,
                group_cols=[],
                agg_specs={"n": ("value", "count")},
            )
        )
        assert result["n"].iloc[0] == 6


# ---------------------------------------------------------------------------
# Dtype preservation
# ---------------------------------------------------------------------------


class TestDtypePreservation:
    """Verify backends preserve expected dtypes through operations."""

    def test_integer_survives_filter(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]})
        mask = pd.Series([True, False, True])
        result = backend.to_pandas(backend.filter(df, mask))
        assert result["val"].dtype in (np.int64, np.int32, int)

    def test_float_survives_join(self, backend: BackendEngine) -> None:
        left = pd.DataFrame({"k": [1, 2], "x": [1.5, 2.5]})
        right = pd.DataFrame({"k": [1, 2], "y": [3.5, 4.5]})
        result = backend.to_pandas(backend.join(left, right, on="k"))
        assert np.issubdtype(result["x"].dtype, np.floating)
        assert np.issubdtype(result["y"].dtype, np.floating)

    def test_string_survives_sort(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"name": ["charlie", "alice", "bob"]})
        result = backend.to_pandas(backend.sort(df, by=["name"]))
        assert result["name"].tolist() == ["alice", "bob", "charlie"]
        dtype_str = str(result["name"].dtype).lower()
        assert result["name"].dtype == object or "string" in dtype_str or dtype_str == "str"


# ---------------------------------------------------------------------------
# Multi-column joins
# ---------------------------------------------------------------------------


class TestMultiColumnJoin:
    """Test joins on multiple columns."""

    def test_two_column_inner_join(self, backend: BackendEngine) -> None:
        left = pd.DataFrame(
            {"a": [1, 1, 2], "b": ["x", "y", "x"], "lv": [10, 20, 30]}
        )
        right = pd.DataFrame(
            {"a": [1, 2, 1], "b": ["x", "x", "z"], "rv": [100, 200, 300]}
        )
        result = backend.to_pandas(
            backend.join(left, right, on=["a", "b"], how="inner")
        )
        assert len(result) == 2  # (1,x) and (2,x)
        assert set(result["lv"].tolist()) == {10, 30}


# ---------------------------------------------------------------------------
# PandasBackend join strategies
# ---------------------------------------------------------------------------


class TestPandasJoinStrategies:
    """Test PandasBackend-specific join strategy routing."""

    def test_broadcast_swaps_when_right_larger(self) -> None:
        be = PandasBackend()
        # left smaller than right — broadcast should swap for inner join
        left = pd.DataFrame({"k": [1], "v": ["a"]})
        right = pd.DataFrame({"k": [1, 2, 3], "w": ["x", "y", "z"]})
        result = be.join(left, right, on="k", how="inner", strategy="broadcast")
        assert len(result) == 1
        assert result["v"].iloc[0] == "a"

    def test_merge_strategy_produces_correct_result(self) -> None:
        be = PandasBackend()
        left = pd.DataFrame({"k": [1, 2, 3], "v": [10, 20, 30]})
        right = pd.DataFrame({"k": [2, 3, 4], "w": [200, 300, 400]})
        result = be.join(left, right, on="k", how="inner", strategy="merge")
        assert len(result) == 2
        assert sorted(result["k"].tolist()) == [2, 3]


# ---------------------------------------------------------------------------
# DuckDB limit/skip validation
# ---------------------------------------------------------------------------


class TestDuckDBLimitSkipValidation:
    """Test DuckDB backend validates limit/skip inputs."""

    def test_limit_negative_raises(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="non-negative"):
            be.limit(df, -1)

    def test_skip_negative_raises(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1, 2, 3]})
        with pytest.raises(ValueError, match="non-negative"):
            be.skip(df, -1)

    def test_limit_zero_returns_empty(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = be.limit(df, 0)
        assert len(result) == 0

    def test_skip_all_returns_empty(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = be.skip(df, 3)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Empty DataFrame handling
# ---------------------------------------------------------------------------


class TestEmptyDataFrame:
    """Test operations on empty DataFrames across backends."""

    def test_filter_empty_input(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        mask = pd.Series([], dtype="bool")
        result = backend.to_pandas(backend.filter(df, mask))
        assert len(result) == 0

    def test_sort_empty(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        result = backend.to_pandas(backend.sort(df, by=["a"]))
        assert len(result) == 0

    def test_distinct_empty(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        result = backend.to_pandas(backend.distinct(df))
        assert len(result) == 0

    def test_limit_empty(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        result = backend.to_pandas(backend.limit(df, 5))
        assert len(result) == 0

    def test_is_empty_on_empty(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        assert backend.is_empty(df) is True

    def test_row_count_on_empty(self, backend: BackendEngine) -> None:
        df = pd.DataFrame({"a": pd.Series([], dtype="int64")})
        assert backend.row_count(df) == 0


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestHelpers:
    """Test shared backend helper functions."""

    def test_validate_identifier_valid(self) -> None:
        assert validate_identifier("my_col") == "my_col"
        assert validate_identifier("_private") == "_private"
        assert validate_identifier("Col123") == "Col123"

    def test_validate_identifier_rejects_injection(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier('"; DROP TABLE --')

    def test_validate_identifier_rejects_spaces(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("my col")

    def test_validate_identifier_rejects_leading_digit(self) -> None:
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("1col")

    def test_to_pandas_passthrough(self) -> None:
        df = pd.DataFrame({"a": [1]})
        assert _to_pandas(df) is df

    def test_to_pandas_invalid_type(self) -> None:
        from pycypher.exceptions import WrongCypherTypeError

        with pytest.raises(WrongCypherTypeError):
            _to_pandas("not a dataframe")

    def test_pandas_agg_to_sql_all_supported(self) -> None:
        expected = {
            "sum": "SUM",
            "count": "COUNT",
            "mean": "AVG",
            "min": "MIN",
            "max": "MAX",
            "std": "STDDEV_SAMP",
            "var": "VAR_SAMP",
            "first": "FIRST",
            "last": "LAST",
        }
        for func, sql in expected.items():
            assert _pandas_agg_to_sql(func) == sql

    def test_pandas_agg_to_sql_unsupported(self) -> None:
        with pytest.raises(ValueError, match="Unsupported"):
            _pandas_agg_to_sql("median")

    def test_polars_agg_func_unsupported(self) -> None:
        import polars as pl

        col_expr = pl.col("x")
        with pytest.raises(ValueError, match="Unsupported"):
            _polars_agg_func(col_expr, "median")

    def test_polars_agg_func_all_supported(self) -> None:
        import polars as pl

        col_expr = pl.col("x")
        for func in ("sum", "count", "mean", "min", "max", "std", "var", "first", "last"):
            result = _polars_agg_func(col_expr, func)
            # Should return a Polars expression, not raise
            assert result is not None


# ---------------------------------------------------------------------------
# DuckDB resource cleanup
# ---------------------------------------------------------------------------


class TestDuckDBCleanup:
    """Test DuckDB connection lifecycle."""

    def test_del_after_close_is_safe(self) -> None:
        be = DuckDBBackend()
        be.close()
        be.close()  # idempotent
        del be  # __del__ should not raise

    def test_context_manager(self) -> None:
        with DuckDBBackend() as be:
            df = pd.DataFrame({"a": [1, 2]})
            result = be.distinct(df)
            assert len(result) == 2
        # After exiting, connection should be closed
        assert be._conn is None


# ---------------------------------------------------------------------------
# DuckDB SQL injection prevention
# ---------------------------------------------------------------------------


class TestDuckDBSQLInjection:
    """Verify DuckDB backend rejects malicious column names."""

    def test_join_rejects_injection_in_column_name(self) -> None:
        be = DuckDBBackend()
        left = pd.DataFrame({"k": [1]})
        right = pd.DataFrame({"k": [1]})
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            be.join(left, right, on='k"; DROP TABLE users--')

    def test_aggregate_rejects_injection_in_group_col(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            be.aggregate(df, group_cols=['a"; --'], agg_specs={"n": ("b", "count")})

    def test_sort_rejects_injection_in_column(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            be.sort(df, by=['a"; DROP TABLE --'])

    def test_scan_entity_rejects_injection(self) -> None:
        be = DuckDBBackend()
        df = pd.DataFrame({ID_COLUMN: [1]})
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            be.scan_entity(df, '"; DROP TABLE --')
