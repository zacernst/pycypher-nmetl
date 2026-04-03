"""Tests for the BackendEngine abstraction layer.

Validates that both PandasBackend and DuckDBBackend produce identical
results for all operations, ensuring backend interchangeability.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backend_engine import (
    BackendEngine,
    CircuitBreaker,
    CircuitState,
    DuckDBBackend,
    PandasBackend,
    PolarsBackend,
    check_backend_health,
    get_circuit_breaker,
    select_backend,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Small sample entity DataFrame."""
    return pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 22],
            "dept": ["eng", "mktg", "eng", "sales", "eng"],
        },
    )


@pytest.fixture
def left_df() -> pd.DataFrame:
    """Left side for join tests."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4],
            "value_l": ["a", "b", "c", "d"],
        },
    )


@pytest.fixture
def right_df() -> pd.DataFrame:
    """Right side for join tests."""
    return pd.DataFrame(
        {
            "id": [2, 3, 4, 5],
            "value_r": ["x", "y", "z", "w"],
        },
    )


@pytest.fixture(params=["pandas", "duckdb", "polars"])
def backend(request: pytest.FixtureRequest) -> BackendEngine:
    """Parametrised fixture yielding each backend implementation."""
    if request.param == "pandas":
        return PandasBackend()
    if request.param == "polars":
        return PolarsBackend()
    return DuckDBBackend()


# ---------------------------------------------------------------------------
# Protocol compliance
# ---------------------------------------------------------------------------


class TestProtocolCompliance:
    """Verify both backends satisfy the BackendEngine protocol."""

    def test_pandas_is_backend(self) -> None:
        """PandasBackend satisfies BackendEngine protocol."""
        assert isinstance(PandasBackend(), BackendEngine)

    def test_duckdb_is_backend(self) -> None:
        """DuckDBBackend satisfies BackendEngine protocol."""
        assert isinstance(DuckDBBackend(), BackendEngine)

    def test_pandas_name(self) -> None:
        """PandasBackend reports correct name."""
        assert PandasBackend().name == "pandas"

    def test_duckdb_name(self) -> None:
        """DuckDBBackend reports correct name."""
        assert DuckDBBackend().name == "duckdb"

    def test_polars_is_backend(self) -> None:
        """PolarsBackend satisfies BackendEngine protocol."""
        assert isinstance(PolarsBackend(), BackendEngine)

    def test_polars_name(self) -> None:
        """PolarsBackend reports correct name."""
        assert PolarsBackend().name == "polars"


# ---------------------------------------------------------------------------
# scan_entity
# ---------------------------------------------------------------------------


class TestScanEntity:
    """Test entity scanning across backends."""

    def test_scan_returns_id_column(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """scan_entity returns a frame with __ID__ column."""
        result = backend.scan_entity(sample_df, "Person")
        result_pd = backend.to_pandas(result)
        assert ID_COLUMN in result_pd.columns
        assert len(result_pd) == 5

    def test_scan_ids_match(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """scan_entity returns all IDs from source."""
        result = backend.to_pandas(backend.scan_entity(sample_df, "Person"))
        assert sorted(result[ID_COLUMN].tolist()) == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# filter
# ---------------------------------------------------------------------------


class TestFilter:
    """Test filtering across backends."""

    def test_filter_basic(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Filter keeps matching rows."""
        mask = sample_df["age"] > 28
        result = backend.to_pandas(backend.filter(sample_df, mask))
        assert len(result) == 2  # Alice (30) and Carol (35)
        assert set(result["name"].tolist()) == {"Alice", "Carol"}

    def test_filter_empty(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Filter with no matches returns empty frame."""
        mask = sample_df["age"] > 100
        result = backend.to_pandas(backend.filter(sample_df, mask))
        assert len(result) == 0


# ---------------------------------------------------------------------------
# join
# ---------------------------------------------------------------------------


class TestJoin:
    """Test join operations across backends."""

    def test_inner_join(
        self,
        backend: BackendEngine,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
    ) -> None:
        """Inner join returns matching rows."""
        result = backend.to_pandas(
            backend.join(left_df, right_df, on="id", how="inner"),
        )
        assert len(result) == 3  # ids 2, 3, 4
        assert sorted(result["id"].tolist()) == [2, 3, 4]
        assert "value_l" in result.columns
        assert "value_r" in result.columns

    def test_left_join(
        self,
        backend: BackendEngine,
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
    ) -> None:
        """Left join preserves all left rows."""
        result = backend.to_pandas(
            backend.join(left_df, right_df, on="id", how="left"),
        )
        assert len(result) == 4  # all left rows

    def test_cross_join(self, backend: BackendEngine) -> None:
        """Cross join produces Cartesian product."""
        left = pd.DataFrame({"a": [1, 2]})
        right = pd.DataFrame({"b": [3, 4]})
        result = backend.to_pandas(
            backend.join(left, right, on=[], how="cross"),
        )
        assert len(result) == 4  # 2 x 2


# ---------------------------------------------------------------------------
# aggregate
# ---------------------------------------------------------------------------


class TestAggregate:
    """Test aggregation across backends."""

    def test_full_table_count(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Full-table COUNT aggregation."""
        result = backend.to_pandas(
            backend.aggregate(
                sample_df,
                group_cols=[],
                agg_specs={"total": (ID_COLUMN, "count")},
            ),
        )
        assert result["total"].iloc[0] == 5

    def test_grouped_count(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Grouped COUNT aggregation."""
        result = backend.to_pandas(
            backend.aggregate(
                sample_df,
                group_cols=["dept"],
                agg_specs={"cnt": (ID_COLUMN, "count")},
            ),
        )
        dept_counts = dict(zip(result["dept"], result["cnt"], strict=False))
        assert dept_counts["eng"] == 3
        assert dept_counts["mktg"] == 1
        assert dept_counts["sales"] == 1


# ---------------------------------------------------------------------------
# sort
# ---------------------------------------------------------------------------


class TestSort:
    """Test sorting across backends."""

    def test_sort_ascending(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Sort ascending by age."""
        result = backend.to_pandas(
            backend.sort(sample_df, by=["age"], ascending=[True]),
        )
        ages = result["age"].tolist()
        assert ages == sorted(ages)

    def test_sort_descending(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Sort descending by age."""
        result = backend.to_pandas(
            backend.sort(sample_df, by=["age"], ascending=[False]),
        )
        ages = result["age"].tolist()
        assert ages == sorted(ages, reverse=True)


# ---------------------------------------------------------------------------
# limit
# ---------------------------------------------------------------------------


class TestLimit:
    """Test limit across backends."""

    def test_limit(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Limit returns first N rows."""
        result = backend.to_pandas(backend.limit(sample_df, 3))
        assert len(result) == 3


# ---------------------------------------------------------------------------
# Sort + limit fusion
# ---------------------------------------------------------------------------


class TestSortLimitFusion:
    """Test sort followed by limit produces correct fused results."""

    def test_sort_then_limit(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Sort + limit returns top N sorted rows."""
        sorted_frame = backend.sort(
            sample_df, by=["age"], ascending=[True],
        )
        result = backend.to_pandas(backend.limit(sorted_frame, 2))
        assert len(result) == 2
        assert result["age"].tolist() == [22, 25]

    def test_sort_desc_then_limit(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Sort descending + limit returns top N sorted rows."""
        sorted_frame = backend.sort(
            sample_df, by=["age"], ascending=[False],
        )
        result = backend.to_pandas(backend.limit(sorted_frame, 3))
        assert len(result) == 3
        assert result["age"].tolist() == [35, 30, 28]


class TestDuckDBLazyFrame:
    """Test DuckDBLazyFrame auto-materialisation behaviour."""

    def test_sort_returns_lazy_for_duckdb(self) -> None:
        """DuckDB sort returns DuckDBLazyFrame for fusion."""
        from pycypher.backends.duckdb_backend import DuckDBLazyFrame

        db = DuckDBBackend()
        df = pd.DataFrame({"a": [3, 1, 2]})
        result = db.sort(df, by=["a"])
        assert isinstance(result, DuckDBLazyFrame)

    def test_lazy_frame_auto_materialises(self) -> None:
        """DuckDBLazyFrame auto-materialises for pandas method access."""
        db = DuckDBBackend()
        df = pd.DataFrame({"a": [3, 1, 2]})
        lazy = db.sort(df, by=["a"])
        # Access pandas method — should auto-materialise
        values = lazy.reset_index(drop=True)
        assert list(values["a"]) == [1, 2, 3]

    def test_lazy_frame_columns(self) -> None:
        """DuckDBLazyFrame provides column names without materialising."""
        db = DuckDBBackend()
        df = pd.DataFrame({"x": [1], "y": [2]})
        lazy = db.sort(df, by=["x"])
        assert "x" in lazy.columns
        assert "y" in lazy.columns

    def test_lazy_frame_len(self) -> None:
        """DuckDBLazyFrame provides len without full materialisation."""
        db = DuckDBBackend()
        df = pd.DataFrame({"a": range(100)})
        lazy = db.sort(df, by=["a"])
        assert len(lazy) == 100

    def test_lazy_limit_fusion_correctness(self) -> None:
        """Sort + limit via DuckDBLazyFrame produces correct results."""
        db = DuckDBBackend()
        df = pd.DataFrame({"a": [5, 3, 1, 4, 2]})
        sorted_lazy = db.sort(df, by=["a"], ascending=[True])
        limited = db.limit(sorted_lazy, 3)
        assert isinstance(limited, pd.DataFrame)
        assert list(limited["a"]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# memory_estimate
# ---------------------------------------------------------------------------


class TestMemoryEstimate:
    """Test memory estimation across backends."""

    def test_memory_positive(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Memory estimate returns positive value."""
        est = backend.memory_estimate_bytes(sample_df)
        assert est > 0

    def test_row_count(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Row count is accurate."""
        assert backend.row_count(sample_df) == 5


# ---------------------------------------------------------------------------
# Backend selection
# ---------------------------------------------------------------------------


class TestBackendSelection:
    """Test the select_backend heuristic."""

    def test_explicit_pandas(self) -> None:
        """Explicit pandas hint returns PandasBackend."""
        be = select_backend(hint="pandas")
        assert be.name == "pandas"

    def test_explicit_duckdb(self) -> None:
        """Explicit duckdb hint returns DuckDBBackend."""
        be = select_backend(hint="duckdb")
        assert be.name == "duckdb"

    def test_auto_small(self) -> None:
        """Auto selects pandas for small datasets."""
        be = select_backend(hint="auto", estimated_rows=1_000)
        assert be.name == "pandas"

    def test_auto_large(self) -> None:
        """Auto selects polars for large datasets (preferred over duckdb)."""
        be = select_backend(hint="auto", estimated_rows=500_000)
        assert be.name == "polars"

    def test_explicit_polars(self) -> None:
        """Explicit polars hint returns PolarsBackend."""
        be = select_backend(hint="polars")
        assert be.name == "polars"

    def test_invalid_hint(self) -> None:
        """Invalid hint raises ValueError."""
        with pytest.raises(ValueError, match="Unknown backend hint"):
            select_backend(hint="invalid_backend")


# ---------------------------------------------------------------------------
# rename (new operation)
# ---------------------------------------------------------------------------


class TestRename:
    """Test column renaming across backends."""

    def test_rename_single(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Rename a single column."""
        result = backend.to_pandas(
            backend.rename(sample_df, {"name": "person_name"}),
        )
        assert "person_name" in result.columns
        assert "name" not in result.columns
        assert len(result) == 5

    def test_rename_preserves_data(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Rename preserves all row data."""
        result = backend.to_pandas(backend.rename(sample_df, {"age": "years"}))
        assert sorted(result["years"].tolist()) == [22, 25, 28, 30, 35]


# ---------------------------------------------------------------------------
# concat (new operation)
# ---------------------------------------------------------------------------


class TestConcat:
    """Test vertical concatenation across backends."""

    def test_concat_two_frames(self, backend: BackendEngine) -> None:
        """Concatenate two DataFrames."""
        a = pd.DataFrame({"x": [1, 2]})
        b = pd.DataFrame({"x": [3, 4]})
        result = backend.to_pandas(backend.concat([a, b]))
        assert result["x"].tolist() == [1, 2, 3, 4]

    def test_concat_empty(self, backend: BackendEngine) -> None:
        """Concatenate with empty DataFrame."""
        a = pd.DataFrame({"x": [1]})
        b = pd.DataFrame({"x": pd.Series([], dtype=int)})
        result = backend.to_pandas(backend.concat([a, b]))
        assert len(result) == 1


# ---------------------------------------------------------------------------
# distinct (new operation)
# ---------------------------------------------------------------------------


class TestDistinct:
    """Test deduplication across backends."""

    def test_distinct_removes_duplicates(self, backend: BackendEngine) -> None:
        """Distinct removes duplicate rows."""
        df = pd.DataFrame({"a": [1, 2, 1, 2], "b": ["x", "y", "x", "y"]})
        result = backend.to_pandas(backend.distinct(df))
        assert len(result) == 2

    def test_distinct_preserves_unique(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Distinct on already-unique data is identity."""
        result = backend.to_pandas(backend.distinct(sample_df))
        assert len(result) == 5


# ---------------------------------------------------------------------------
# assign_column (new operation)
# ---------------------------------------------------------------------------


class TestAssignColumn:
    """Test column assignment across backends."""

    def test_assign_new_column(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Assign a new column."""
        result = backend.to_pandas(
            backend.assign_column(sample_df, "flag", True),
        )
        assert "flag" in result.columns
        assert all(result["flag"])

    def test_assign_series(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Assign a Series as column values."""
        ages = sample_df["age"] * 2
        result = backend.to_pandas(
            backend.assign_column(sample_df, "double_age", ages),
        )
        assert result["double_age"].tolist() == [60, 50, 70, 56, 44]


# ---------------------------------------------------------------------------
# drop_columns (new operation)
# ---------------------------------------------------------------------------


class TestDropColumns:
    """Test column dropping across backends."""

    def test_drop_existing(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Drop an existing column."""
        result = backend.to_pandas(backend.drop_columns(sample_df, ["dept"]))
        assert "dept" not in result.columns
        assert "name" in result.columns

    def test_drop_missing_is_noop(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Dropping a nonexistent column is a no-op."""
        result = backend.to_pandas(
            backend.drop_columns(sample_df, ["nonexistent"]),
        )
        assert len(result.columns) == len(sample_df.columns)


# ---------------------------------------------------------------------------
# skip (new operation)
# ---------------------------------------------------------------------------


class TestSkip:
    """Test row skipping across backends."""

    def test_skip_rows(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Skip first N rows."""
        result = backend.to_pandas(backend.skip(sample_df, 3))
        assert len(result) == 2

    def test_skip_zero(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Skip 0 is identity."""
        result = backend.to_pandas(backend.skip(sample_df, 0))
        assert len(result) == 5


# ---------------------------------------------------------------------------
# is_empty (new operation)
# ---------------------------------------------------------------------------


class TestIsEmpty:
    """Test emptiness check across backends."""

    def test_non_empty(
        self,
        backend: BackendEngine,
        sample_df: pd.DataFrame,
    ) -> None:
        """Non-empty frame returns False."""
        assert not backend.is_empty(sample_df)

    def test_empty(self, backend: BackendEngine) -> None:
        """Empty frame returns True."""
        empty = pd.DataFrame({"a": pd.Series([], dtype=int)})
        assert backend.is_empty(empty)


# ---------------------------------------------------------------------------
# Security: SQL identifier validation
# ---------------------------------------------------------------------------


class TestSQLIdentifierValidation:
    """Test that DuckDB backend rejects malicious identifiers."""

    def test_valid_identifiers(self) -> None:
        """Normal identifiers pass validation."""
        from pycypher.backend_engine import validate_identifier

        assert validate_identifier("name") == "name"
        assert validate_identifier("__ID__") == "__ID__"
        assert validate_identifier("Person") == "Person"
        assert validate_identifier("_anon_0") == "_anon_0"

    def test_reject_sql_injection(self) -> None:
        """Identifiers with SQL injection characters are rejected."""
        from pycypher.backend_engine import validate_identifier

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier('"; DROP TABLE users; --')

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("col name")

        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_identifier("col;name")


# ---------------------------------------------------------------------------
# DuckDB resource cleanup
# ---------------------------------------------------------------------------


class TestDuckDBResourceCleanup:
    """Verify DuckDB connection cleanup and view leak prevention."""

    def test_context_manager_closes_connection(self) -> None:
        """Connection is closed after context manager exit."""
        with DuckDBBackend() as be:
            assert be._conn is not None
        assert be._conn is None

    def test_close_idempotent(self) -> None:
        """Calling close() multiple times is safe."""
        be = DuckDBBackend()
        be.close()
        assert be._conn is None
        # Second close should not raise
        be.close()
        assert be._conn is None

    def test_scan_entity_cleans_up_view(self) -> None:
        """scan_entity unregisters its temporary view after use."""
        be = DuckDBBackend()
        df = pd.DataFrame({"__ID__": [1, 2, 3]})
        be.scan_entity(df, "TestEntity")
        # The view should be unregistered — querying it should fail
        with pytest.raises(Exception):
            be._conn.execute("SELECT * FROM _entity_TestEntity")
        be.close()

    def test_join_cleans_up_views_on_error(self) -> None:
        """Join unregisters views even when SQL execution fails."""
        be = DuckDBBackend()
        left = pd.DataFrame({"id": [1], "val": ["a"]})
        right = pd.DataFrame({"id": [1], "val": ["b"]})
        # Force a join on a nonexistent column to trigger an error
        with pytest.raises(Exception):
            be.join(left, right, on="nonexistent_col")
        # Views should still be cleaned up
        with pytest.raises(Exception):
            be._conn.execute("SELECT * FROM _left")
        with pytest.raises(Exception):
            be._conn.execute("SELECT * FROM _right")
        be.close()

    def test_operations_work_after_error_recovery(self) -> None:
        """Backend remains usable after a failed operation."""
        be = DuckDBBackend()
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["a", "b"]})

        # Force an error
        with pytest.raises(Exception):
            be.join(df, df, on="nonexistent_col")

        # Backend should still work for subsequent operations
        result = be.scan_entity(df, "Recovery")
        assert len(result) == 2
        be.close()


# ---------------------------------------------------------------------------
# Health checks
# ---------------------------------------------------------------------------


class TestHealthCheck:
    """Test backend health probe."""

    def test_pandas_healthy(self) -> None:
        """Pandas backend passes health check."""
        assert check_backend_health(PandasBackend())

    def test_duckdb_healthy(self) -> None:
        """DuckDB backend passes health check."""
        assert check_backend_health(DuckDBBackend())

    def test_polars_healthy(self) -> None:
        """Polars backend passes health check."""
        assert check_backend_health(PolarsBackend())

    def test_broken_backend_fails_health_check(self) -> None:
        """A backend whose scan_entity raises returns False."""

        class BrokenBackend(PandasBackend):
            @property
            def name(self) -> str:
                return "broken"

            def scan_entity(self, source_obj, entity_type):
                msg = "Simulated failure"
                raise RuntimeError(msg)

        assert not check_backend_health(BrokenBackend())


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:
    """Test circuit breaker state transitions."""

    def test_initial_state_is_closed(self) -> None:
        """New circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker()
        assert cb.state("duckdb") is CircuitState.CLOSED
        assert cb.is_available("duckdb")

    def test_failures_below_threshold_stay_closed(self) -> None:
        """Circuit stays closed when failures are below threshold."""
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        assert cb.state("duckdb") is CircuitState.CLOSED
        assert cb.is_available("duckdb")

    def test_failures_at_threshold_open_circuit(self) -> None:
        """Circuit opens when failure threshold is reached."""
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        assert cb.state("duckdb") is CircuitState.OPEN
        assert not cb.is_available("duckdb")

    def test_success_resets_failure_count(self) -> None:
        """A success resets the failure counter and closes the circuit."""
        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        cb.record_success("duckdb")
        assert cb.state("duckdb") is CircuitState.CLOSED
        # Need full threshold again to open
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        assert cb.state("duckdb") is CircuitState.CLOSED

    def test_half_open_after_timeout(self) -> None:
        """Circuit transitions to HALF_OPEN after recovery timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
        cb.record_failure("polars")
        assert cb.state("polars") is CircuitState.OPEN
        # Wait for timeout
        import time

        time.sleep(0.02)
        assert cb.state("polars") is CircuitState.HALF_OPEN
        assert cb.is_available("polars")

    def test_reset_single_backend(self) -> None:
        """Reset clears state for a specific backend."""
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("duckdb")
        cb.record_failure("polars")
        cb.reset("duckdb")
        assert cb.state("duckdb") is CircuitState.CLOSED
        assert cb.state("polars") is CircuitState.OPEN

    def test_reset_all(self) -> None:
        """Reset with no argument clears all backends."""
        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("duckdb")
        cb.record_failure("polars")
        cb.reset()
        assert cb.state("duckdb") is CircuitState.CLOSED
        assert cb.state("polars") is CircuitState.CLOSED

    def test_independent_backends(self) -> None:
        """Each backend has independent circuit state."""
        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("duckdb")
        cb.record_failure("duckdb")
        assert cb.state("duckdb") is CircuitState.OPEN
        assert cb.state("polars") is CircuitState.CLOSED
        assert cb.state("pandas") is CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Health-checked backend selection
# ---------------------------------------------------------------------------


class TestHealthCheckedSelection:
    """Test select_backend with health checks enabled."""

    def setup_method(self) -> None:
        """Reset the global circuit breaker before each test."""
        get_circuit_breaker().reset()

    def test_health_check_pandas(self) -> None:
        """Explicit pandas with health check succeeds."""
        be = select_backend(hint="pandas", run_health_check=True)
        assert be.name == "pandas"

    def test_health_check_duckdb(self) -> None:
        """Explicit duckdb with health check succeeds."""
        be = select_backend(hint="duckdb", run_health_check=True)
        assert be.name == "duckdb"

    def test_health_check_polars(self) -> None:
        """Explicit polars with health check succeeds."""
        be = select_backend(hint="polars", run_health_check=True)
        assert be.name == "polars"

    def test_auto_with_health_check(self) -> None:
        """Auto selection with health check returns a healthy backend."""
        be = select_backend(
            hint="auto",
            estimated_rows=500_000,
            run_health_check=True,
        )
        assert be.name in {"polars", "duckdb", "pandas"}

    def test_fallback_on_open_circuit(self) -> None:
        """Auto selection skips backends with open circuits."""
        cb = get_circuit_breaker()
        # Open the polars circuit
        for _ in range(cb.failure_threshold):
            cb.record_failure("polars")
        be = select_backend(hint="auto", estimated_rows=500_000)
        # Should skip polars and pick duckdb or pandas
        assert be.name in {"duckdb", "pandas"}

    def test_get_circuit_breaker(self) -> None:
        """Module-level circuit breaker is accessible."""
        cb = get_circuit_breaker()
        assert isinstance(cb, CircuitBreaker)

    def test_without_health_check_backward_compatible(self) -> None:
        """Default (no health check) behaves like original select_backend."""
        be = select_backend(hint="pandas")
        assert be.name == "pandas"
        be = select_backend(hint="auto", estimated_rows=1_000)
        assert be.name == "pandas"
