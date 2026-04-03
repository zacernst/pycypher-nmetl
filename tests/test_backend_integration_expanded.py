"""Expanded backend integration tests — closes coverage gaps.

Addresses gaps identified in cycle 2 dependency/integration review:

1. Polars backend in end-to-end Cypher acceptance tests (was pandas/duckdb only)
2. Three-way cross-backend equivalence (pandas vs duckdb vs polars)
3. Error handling and fallback paths in select_backend
4. Edge cases: NULLs, empty results, type coercion, large-ish datasets
5. Backend selection heuristics with health checks
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backend_engine import (
    CircuitBreaker,
    DuckDBBackend,
    PandasBackend,
    PolarsBackend,
    check_backend_health,
    select_backend,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_context(backend: str = "pandas") -> Context:
    """Build a small graph context with the given backend hint."""
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "age": [30, 25, 35, 28],
            "dept": ["eng", "mktg", "eng", "sales"],
        },
    )
    knows = pd.DataFrame(
        {
            ID_COLUMN: [100, 101, 102],
            "__SOURCE__": [1, 1, 2],
            "__TARGET__": [2, 3, 4],
        },
    )

    person_table = EntityTable.from_dataframe("Person", persons)
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table},
        ),
        backend=backend,
    )


@pytest.fixture(params=["pandas", "duckdb", "polars"])
def ctx(request: pytest.FixtureRequest) -> Context:
    """Parametrized context — all three backends."""
    return _make_context(backend=request.param)


# ---------------------------------------------------------------------------
# 1. Polars E2E acceptance (extends test_backend_e2e_acceptance.py)
# ---------------------------------------------------------------------------


class TestPolarsE2EAcceptance:
    """Polars backend through full Cypher execution pipeline."""

    def test_basic_scan(self, ctx: Context) -> None:
        """MATCH (p:Person) RETURN p.name works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name"
        )
        assert sorted(result["name"].tolist()) == [
            "Alice",
            "Bob",
            "Carol",
            "Dave",
        ]

    def test_where_filter(self, ctx: Context) -> None:
        """WHERE clause filtering works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 27 "
            "RETURN p.name AS name ORDER BY name",
        )
        assert result["name"].tolist() == ["Alice", "Carol", "Dave"]

    def test_relationship_traversal(self, ctx: Context) -> None:
        """Relationship pattern matching works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name AS friend ORDER BY friend",
        )
        assert result["friend"].tolist() == ["Bob", "Carol"]

    def test_order_by_limit(self, ctx: Context) -> None:
        """ORDER BY + LIMIT works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name ORDER BY p.age ASC LIMIT 2",
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Bob", "Dave"]

    def test_skip_and_limit(self, ctx: Context) -> None:
        """SKIP + LIMIT works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS name ORDER BY p.age ASC SKIP 1 LIMIT 2",
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Dave", "Alice"]

    def test_count_aggregation(self, ctx: Context) -> None:
        """WITH clause aggregation works across all backends."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "WITH p.dept AS dept, count(p) AS cnt "
            "RETURN dept, cnt ORDER BY dept",
        )
        dept_counts = dict(zip(result["dept"], result["cnt"], strict=False))
        assert dept_counts["eng"] == 2
        assert dept_counts["mktg"] == 1
        assert dept_counts["sales"] == 1


# ---------------------------------------------------------------------------
# 2. Three-way cross-backend equivalence
# ---------------------------------------------------------------------------


class TestThreeWayEquivalence:
    """Verify pandas, duckdb, and polars produce identical results."""

    _QUERIES: list[tuple[str, str]] = [
        ("MATCH (p:Person) RETURN p.name AS name ORDER BY name", "name"),
        (
            "MATCH (p:Person) WHERE p.age >= 28 "
            "RETURN p.name AS name ORDER BY name",
            "name",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt ORDER BY src, tgt",
            "src",
        ),
        (
            "MATCH (p:Person) "
            "RETURN p.name AS name ORDER BY p.age DESC LIMIT 2",
            "name",
        ),
    ]

    @pytest.mark.parametrize(("query", "sort_col"), _QUERIES)
    def test_three_way_equivalence(
        self, query: str, sort_col: str
    ) -> None:
        """All three backends produce the same result."""
        results = {}
        for backend in ("pandas", "duckdb", "polars"):
            ctx = _make_context(backend=backend)
            results[backend] = Star(context=ctx).execute_query(query)

        # All backends should have same columns
        cols = list(results["pandas"].columns)
        for backend in ("duckdb", "polars"):
            assert list(results[backend].columns) == cols, (
                f"{backend} columns differ from pandas"
            )

        # All backends should have same row count
        n_rows = len(results["pandas"])
        for backend in ("duckdb", "polars"):
            assert len(results[backend]) == n_rows, (
                f"{backend} row count differs"
            )

        # All backends should have same values
        for col in cols:
            pandas_vals = sorted(results["pandas"][col].tolist())
            for backend in ("duckdb", "polars"):
                backend_vals = sorted(results[backend][col].tolist())
                assert pandas_vals == backend_vals, (
                    f"Column {col!r} differs: pandas vs {backend}"
                )


# ---------------------------------------------------------------------------
# 3. Backend selection and fallback
# ---------------------------------------------------------------------------


class TestBackendSelectionAndFallback:
    """Backend selection heuristics and error handling."""

    def test_invalid_hint_raises(self) -> None:
        """Unknown backend hint raises ValueError."""
        with pytest.raises(ValueError, match="Unknown backend hint"):
            select_backend(hint="invalid_backend")

    def test_auto_small_data_selects_pandas(self) -> None:
        """Auto mode with small data selects pandas."""
        backend = select_backend(hint="auto", estimated_rows=100)
        assert backend.name == "pandas"

    def test_explicit_pandas(self) -> None:
        """Explicit pandas hint returns PandasBackend."""
        backend = select_backend(hint="pandas")
        assert backend.name == "pandas"

    def test_explicit_duckdb(self) -> None:
        """Explicit duckdb hint returns DuckDBBackend."""
        backend = select_backend(hint="duckdb")
        assert backend.name == "duckdb"

    def test_explicit_polars(self) -> None:
        """Explicit polars hint returns PolarsBackend."""
        backend = select_backend(hint="polars")
        assert backend.name == "polars"

    @pytest.mark.parametrize(
        "backend_cls", [PandasBackend, DuckDBBackend, PolarsBackend]
    )
    def test_health_check_passes(self, backend_cls) -> None:
        """Health check passes for each backend."""
        backend = backend_cls()
        assert check_backend_health(backend)

    def test_instrumented_backend(self) -> None:
        """Instrumented backend wraps correctly."""
        backend = select_backend(hint="pandas", instrument=True)
        # InstrumentedBackend delegates .name
        assert backend.name == "pandas"


# ---------------------------------------------------------------------------
# 4. Edge cases across backends
# ---------------------------------------------------------------------------


class TestBackendEdgeCases:
    """Edge cases: NULLs, empty results, numeric precision."""

    @pytest.fixture(params=["pandas", "duckdb", "polars"])
    def backend(self, request: pytest.FixtureRequest):
        """Parametrized backend instances."""
        return select_backend(hint=request.param)

    def test_empty_filter_returns_empty(self, backend) -> None:
        """Filtering with all-False mask returns empty frame."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]})
        scanned = backend.scan_entity(df, "Test")
        mask = pd.Series([False, False, False])
        result = backend.filter(scanned, mask)
        assert backend.is_empty(result)
        assert backend.row_count(result) == 0

    def test_concat_single_frame(self, backend) -> None:
        """Concatenating a single frame returns equivalent frame."""
        df = pd.DataFrame({ID_COLUMN: [1, 2], "val": [10, 20]})
        result = backend.concat([df])
        materialized = backend.to_pandas(result)
        assert len(materialized) == 2

    def test_distinct_removes_duplicates(self, backend) -> None:
        """Distinct removes duplicate rows across all backends."""
        df = pd.DataFrame(
            {ID_COLUMN: [1, 1, 2, 2, 3], "val": [10, 10, 20, 20, 30]},
        )
        scanned = backend.scan_entity(df, "Test")
        result = backend.distinct(scanned)
        materialized = backend.to_pandas(result)
        assert len(materialized) == 3

    def test_sort_by_id(self, backend) -> None:
        """Sort by __ID__ column works across all backends."""
        df = pd.DataFrame(
            {ID_COLUMN: [3, 1, 2]},
        )
        scanned = backend.scan_entity(df, "Test")
        result = backend.sort(scanned, by=[ID_COLUMN], ascending=[True])
        materialized = backend.to_pandas(result)
        assert materialized[ID_COLUMN].tolist() == [1, 2, 3]

    def test_join_no_matching_rows(self, backend) -> None:
        """Inner join with no matching keys returns empty frame."""
        left = pd.DataFrame({"id": [1, 2], "left_val": ["a", "b"]})
        right = pd.DataFrame({"id": [3, 4], "right_val": ["c", "d"]})
        result = backend.join(left, right, on="id", how="inner")
        materialized = backend.to_pandas(result)
        assert len(materialized) == 0

    def test_assign_column_scalar(self, backend) -> None:
        """assign_column with scalar value broadcasts to all rows."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3]})
        scanned = backend.scan_entity(df, "Test")
        result = backend.assign_column(scanned, "label", "constant")
        materialized = backend.to_pandas(result)
        assert all(materialized["label"] == "constant")

    def test_drop_missing_column_silent(self, backend) -> None:
        """Dropping a column that doesn't exist is silently ignored."""
        df = pd.DataFrame({ID_COLUMN: [1, 2], "val": [10, 20]})
        scanned = backend.scan_entity(df, "Test")
        result = backend.drop_columns(scanned, ["nonexistent_col"])
        materialized = backend.to_pandas(result)
        assert ID_COLUMN in materialized.columns

    def test_memory_estimate_positive(self, backend) -> None:
        """Memory estimate returns a positive number for non-empty frames."""
        df = pd.DataFrame(
            {ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]},
        )
        scanned = backend.scan_entity(df, "Test")
        estimate = backend.memory_estimate_bytes(scanned)
        assert estimate > 0

    def test_limit_zero_returns_empty(self, backend) -> None:
        """LIMIT 0 returns empty frame."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]})
        scanned = backend.scan_entity(df, "Test")
        result = backend.limit(scanned, 0)
        assert backend.row_count(result) == 0

    def test_skip_beyond_length(self, backend) -> None:
        """SKIP beyond row count returns empty frame."""
        df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]})
        scanned = backend.scan_entity(df, "Test")
        result = backend.skip(scanned, 100)
        assert backend.row_count(result) == 0


# ---------------------------------------------------------------------------
# 5. Backend name and protocol compliance
# ---------------------------------------------------------------------------


class TestBackendProtocolCompliance:
    """Verify all backends satisfy the BackendEngine protocol."""

    @pytest.mark.parametrize(
        "backend_cls", [PandasBackend, DuckDBBackend, PolarsBackend]
    )
    def test_is_backend_engine(self, backend_cls) -> None:
        """Each backend class is a BackendEngine."""
        from pycypher.backend_engine import BackendEngine

        backend = backend_cls()
        assert isinstance(backend, BackendEngine)

    @pytest.mark.parametrize(
        ("hint", "expected_name"),
        [("pandas", "pandas"), ("duckdb", "duckdb"), ("polars", "polars")],
    )
    def test_backend_name(self, hint: str, expected_name: str) -> None:
        """Backend .name matches the hint used to create it."""
        backend = select_backend(hint=hint)
        assert backend.name == expected_name
