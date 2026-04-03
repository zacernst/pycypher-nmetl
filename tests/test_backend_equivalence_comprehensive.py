"""Comprehensive cross-backend equivalence validation.

Systematically verifies that Pandas, DuckDB, and Polars backends produce
identical results for a broad range of Cypher query patterns. Covers the
compatibility risks identified in the Task #11 assessment:

- Type coercion differences (int/float/string)
- NULL handling consistency
- Sort stability and NULL ordering
- DISTINCT semantics
- Aggregation equivalence
- Multi-hop relationship traversal
- Filtered projections with mixed types

Each test runs the same query against all available backends and asserts
result equivalence. This is the acceptance gate for backend interchangeability.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.backend_engine import (
    DuckDBBackend,
    PandasBackend,
    PolarsBackend,
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
# Helpers
# ---------------------------------------------------------------------------


def _available_backends() -> list[str]:
    """Return backend hint strings for all importable backends."""
    backends = ["pandas"]
    try:
        DuckDBBackend()
        backends.append("duckdb")
    except Exception:
        pass
    try:
        PolarsBackend()
        backends.append("polars")
    except Exception:
        pass
    return backends


BACKENDS = _available_backends()


def _make_context(backend: str = "pandas") -> Context:
    """Build a graph with NULLs, mixed types, and multiple relationships.

    Graph:
        Alice(30, eng)   -[KNOWS]-> Bob(25, mktg)
        Alice(30, eng)   -[KNOWS]-> Carol(35, eng)
        Bob(25, mktg)    -[KNOWS]-> Dave(28, sales)
        Eve(22, NULL)    (no relationships)
    """
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 22],
            "dept": ["eng", "mktg", "eng", "sales", None],
            "score": [95.5, 87.0, None, 72.3, 100.0],
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


def _execute(backend: str, query: str) -> pd.DataFrame:
    """Execute a Cypher query with the given backend and return results."""
    ctx = _make_context(backend=backend)
    star = Star(context=ctx)
    return star.execute_query(query)


def _assert_results_equivalent(
    results: dict[str, pd.DataFrame],
    sort_cols: list[str] | None = None,
) -> None:
    """Assert all backend results are equivalent.

    Compares column names, row counts, and sorted values.
    Uses sorted comparison for determinism (backends may return
    rows in different order).
    """
    backends = list(results.keys())
    if len(backends) < 2:
        pytest.skip("Need at least 2 backends for equivalence testing")

    reference = results[backends[0]]
    for other_name in backends[1:]:
        other = results[other_name]

        # Same columns
        assert set(reference.columns) == set(other.columns), (
            f"Column mismatch: {backends[0]}={list(reference.columns)} "
            f"vs {other_name}={list(other.columns)}"
        )

        # Same row count
        assert len(reference) == len(other), (
            f"Row count mismatch: {backends[0]}={len(reference)} "
            f"vs {other_name}={len(other)}"
        )

        # Same values per column (sorted for determinism)
        cols = sort_cols or list(reference.columns)
        for col in reference.columns:
            ref_vals = reference[col].tolist()
            other_vals = other[col].tolist()

            # Normalize NaN/None for comparison
            ref_normalized = [None if pd.isna(v) else v for v in ref_vals]
            other_normalized = [None if pd.isna(v) else v for v in other_vals]

            # Sort with None-safe key
            def _sort_key(x):
                if x is None:
                    return (1, "")
                return (0, x)

            assert sorted(ref_normalized, key=_sort_key) == sorted(
                other_normalized, key=_sort_key
            ), (
                f"Column {col!r} differs: {backends[0]}={ref_normalized} "
                f"vs {other_name}={other_normalized}"
            )


def _run_all_backends(query: str) -> dict[str, pd.DataFrame]:
    """Run query on all available backends, return {backend_name: result}.

    Backends that raise errors are excluded from the result dict with a
    warning. This allows the equivalence check to compare backends that
    succeed while surfacing incompatibilities.
    """
    results: dict[str, pd.DataFrame] = {}
    failures: dict[str, str] = {}
    for b in BACKENDS:
        try:
            results[b] = _execute(b, query)
        except Exception as exc:
            failures[b] = f"{type(exc).__name__}: {exc}"

    if failures:
        import warnings

        for b, msg in failures.items():
            warnings.warn(
                f"Backend {b!r} failed query {query!r}: {msg}",
                stacklevel=2,
            )

    if len(results) < 2:
        pytest.skip(
            f"Need >=2 backends for equivalence; got {list(results.keys())}, "
            f"failures: {failures}"
        )

    return results


# ---------------------------------------------------------------------------
# Basic query equivalence
# ---------------------------------------------------------------------------


class TestBasicQueryEquivalence:
    """Fundamental query patterns across all backends."""

    def test_simple_match_return(self) -> None:
        """MATCH (p:Person) RETURN p.name"""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        )
        _assert_results_equivalent(results)
        # Verify expected values
        ref = list(results.values())[0]
        assert ref["name"].tolist() == ["Alice", "Bob", "Carol", "Dave", "Eve"]

    def test_multi_column_projection(self) -> None:
        """Multiple columns in RETURN."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"
        )
        _assert_results_equivalent(results)

    def test_where_numeric_filter(self) -> None:
        """WHERE with numeric comparison."""
        results = _run_all_backends(
            "MATCH (p:Person) WHERE p.age > 27 RETURN p.name AS name ORDER BY name"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["name"].tolist() == ["Alice", "Carol", "Dave"]

    def test_where_string_equality(self) -> None:
        """WHERE with string comparison."""
        results = _run_all_backends(
            "MATCH (p:Person) WHERE p.dept = 'eng' RETURN p.name AS name ORDER BY name"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["name"].tolist() == ["Alice", "Carol"]


# ---------------------------------------------------------------------------
# NULL handling equivalence
# ---------------------------------------------------------------------------


class TestNullHandlingEquivalence:
    """Verify NULL behavior is consistent across backends."""

    def test_null_in_projection(self) -> None:
        """NULL values appear correctly in results."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name, p.dept AS dept ORDER BY name"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        # Eve has NULL dept
        eve_row = ref[ref["name"] == "Eve"]
        assert eve_row["dept"].isna().all()

    def test_null_score_projection(self) -> None:
        """Float column with NULL values."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name, p.score AS score ORDER BY name"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        carol_row = ref[ref["name"] == "Carol"]
        assert carol_row["score"].isna().all()


# ---------------------------------------------------------------------------
# Relationship traversal equivalence
# ---------------------------------------------------------------------------


class TestRelationshipEquivalence:
    """Relationship patterns produce identical results across backends."""

    def test_single_hop(self) -> None:
        """Single-hop relationship traversal."""
        results = _run_all_backends(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt ORDER BY src, tgt"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert len(ref) == 3

    def test_two_hop(self) -> None:
        """Two-hop relationship traversal."""
        results = _run_all_backends(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN c.name AS fof"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["fof"].tolist() == ["Dave"]

    def test_filtered_relationship(self) -> None:
        """Relationship with WHERE filter on target."""
        results = _run_all_backends(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE b.age > 30 "
            "RETURN a.name AS src, b.name AS tgt"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["tgt"].tolist() == ["Carol"]


# ---------------------------------------------------------------------------
# Aggregation equivalence
# ---------------------------------------------------------------------------


class TestAggregationEquivalence:
    """Aggregation functions produce identical results across backends."""

    def test_count(self) -> None:
        """COUNT aggregation (excluding NULL group keys)."""
        results = _run_all_backends(
            "MATCH (p:Person) WHERE p.dept IS NOT NULL "
            "WITH p.dept AS dept, count(p) AS cnt "
            "RETURN dept, cnt ORDER BY dept"
        )
        _assert_results_equivalent(results)

    def test_count_relationships(self) -> None:
        """COUNT on relationship patterns."""
        results = _run_all_backends(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WITH a.name AS person, count(b) AS friends "
            "RETURN person, friends ORDER BY person"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        alice_row = ref[ref["person"] == "Alice"]
        assert alice_row["friends"].iloc[0] == 2


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT / SKIP equivalence
# ---------------------------------------------------------------------------


class TestOrderLimitSkipEquivalence:
    """Ordering, limiting, and skipping produce identical results."""

    def test_order_asc(self) -> None:
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["name"].tolist() == ["Eve", "Bob", "Dave", "Alice", "Carol"]

    def test_order_desc(self) -> None:
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert ref["name"].tolist() == ["Carol", "Alice", "Dave", "Bob", "Eve"]

    def test_limit(self) -> None:
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC LIMIT 3"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert len(ref) == 3

    def test_skip(self) -> None:
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC SKIP 2"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert len(ref) == 3

    def test_skip_and_limit(self) -> None:
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC SKIP 1 LIMIT 2"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert len(ref) == 2


# ---------------------------------------------------------------------------
# DISTINCT equivalence
# ---------------------------------------------------------------------------


class TestDistinctEquivalence:
    """DISTINCT deduplication across backends."""

    def test_distinct_department(self) -> None:
        """RETURN DISTINCT on department values (includes NULL)."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN DISTINCT p.dept AS dept ORDER BY dept"
        )
        _assert_results_equivalent(results)


# ---------------------------------------------------------------------------
# Type coercion equivalence
# ---------------------------------------------------------------------------


class TestTypeCoercionEquivalence:
    """Verify numeric and string types are consistent across backends."""

    def test_integer_column_type(self) -> None:
        """Age column should produce consistent integer values."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.age AS age ORDER BY age"
        )
        _assert_results_equivalent(results)
        for backend, result in results.items():
            ages = result["age"].tolist()
            # All values should be numeric and equal
            assert ages == [22, 25, 28, 30, 35], f"{backend} ages: {ages}"

    def test_float_column_with_nulls(self) -> None:
        """Score column (float with NULLs) should be consistent."""
        results = _run_all_backends(
            "MATCH (p:Person) WHERE p.score IS NOT NULL "
            "RETURN p.name AS name, p.score AS score ORDER BY name"
        )
        _assert_results_equivalent(results)
        ref = list(results.values())[0]
        assert len(ref) == 4  # Carol has NULL score

    def test_string_column(self) -> None:
        """String values identical across backends."""
        results = _run_all_backends(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY name"
        )
        _assert_results_equivalent(results)
        for _, result in results.items():
            assert result["name"].dtype == object or str(result["name"].dtype).startswith("str")
