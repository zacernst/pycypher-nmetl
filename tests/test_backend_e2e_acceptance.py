"""End-to-end acceptance tests for BackendEngine integration.

Verifies that ``Star.execute_query()`` produces identical results when
the ``Context`` is constructed with different backend engines.  This is
the acceptance gate for Task #23 (wire BackendEngine into pipeline).

Each test constructs a small graph, executes a Cypher query through
``Star``, and asserts the result DataFrame matches across backends.
Tests are parametrized on backend hint so both ``PandasBackend`` and
``DuckDBBackend`` are exercised automatically.

These tests validate the *contract* — same query, same data, same result
regardless of backend — rather than backend-specific optimisations.
"""

from __future__ import annotations

import pandas as pd
import pytest
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
    """Build a small graph context with the given backend hint.

    Graph:
        Alice(30, eng) -[KNOWS]-> Bob(25, mktg)
        Alice(30, eng) -[KNOWS]-> Carol(35, eng)
        Bob(25, mktg)  -[KNOWS]-> Dave(28, sales)
    """
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
            mapping={"KNOWS": knows_table}
        ),
        backend=backend,
    )


@pytest.fixture(params=["pandas", "duckdb"])
def ctx(request: pytest.FixtureRequest) -> Context:
    """Parametrized context fixture — one run per backend."""
    return _make_context(backend=request.param)


# ---------------------------------------------------------------------------
# Acceptance tests
# ---------------------------------------------------------------------------


class TestSimpleMatchReturn:
    """MATCH (p:Person) RETURN p.name — basic entity scan + projection."""

    def test_returns_all_names(self, ctx: Context) -> None:
        """All 4 person names returned."""
        star = Star(context=ctx)
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert sorted(result["name"].tolist()) == [
            "Alice",
            "Bob",
            "Carol",
            "Dave",
        ]

    def test_returns_correct_row_count(self, ctx: Context) -> None:
        """Both name and age columns present with 4 rows."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
        )
        assert len(result) == 4
        assert "name" in result.columns
        assert "age" in result.columns


class TestWhereFilter:
    """MATCH with WHERE predicate filtering."""

    def test_age_filter(self, ctx: Context) -> None:
        """WHERE p.age > 27 returns Alice, Carol, Dave."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 27 RETURN p.name AS name ORDER BY name",
        )
        assert result["name"].tolist() == ["Alice", "Carol", "Dave"]

    def test_string_equality(self, ctx: Context) -> None:
        """WHERE p.dept = 'eng' returns Alice, Carol."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.dept = 'eng' RETURN p.name AS name ORDER BY name",
        )
        assert result["name"].tolist() == ["Alice", "Carol"]


class TestRelationshipTraversal:
    """MATCH with relationship patterns."""

    def test_single_hop(self, ctx: Context) -> None:
        """Alice KNOWS Bob and Carol."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN b.name AS friend ORDER BY friend",
        )
        assert result["friend"].tolist() == ["Bob", "Carol"]

    def test_two_hop(self, ctx: Context) -> None:
        """Alice -> Bob -> Dave (two hops)."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) "
            "WHERE a.name = 'Alice' "
            "RETURN c.name AS friend_of_friend",
        )
        assert result["friend_of_friend"].tolist() == ["Dave"]


class TestWithAggregation:
    """WITH clause aggregation."""

    def test_count_aggregation(self, ctx: Context) -> None:
        """Group by dept, count persons."""
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


class TestOrderByLimitSkip:
    """ORDER BY, LIMIT, and SKIP in RETURN clause."""

    def test_order_by_asc(self, ctx: Context) -> None:
        """Order by age ascending."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC",
        )
        assert result["name"].tolist() == ["Bob", "Dave", "Alice", "Carol"]

    def test_limit(self, ctx: Context) -> None:
        """LIMIT 2 returns first 2 rows after sort."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC LIMIT 2",
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Bob", "Dave"]

    def test_skip(self, ctx: Context) -> None:
        """SKIP 2 returns last 2 rows after sort."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC SKIP 2",
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Alice", "Carol"]

    def test_skip_and_limit(self, ctx: Context) -> None:
        """SKIP 1 LIMIT 2 returns middle 2 rows."""
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC SKIP 1 LIMIT 2",
        )
        assert len(result) == 2
        assert result["name"].tolist() == ["Dave", "Alice"]


class TestBackendProperty:
    """Verify Context.backend is accessible and correct."""

    def test_pandas_backend_name(self) -> None:
        """Explicit pandas backend."""
        ctx = _make_context(backend="pandas")
        assert ctx.backend.name == "pandas"

    def test_duckdb_backend_name(self) -> None:
        """Explicit duckdb backend."""
        ctx = _make_context(backend="duckdb")
        assert ctx.backend.name == "duckdb"

    def test_auto_backend_small_data(self) -> None:
        """Auto with 4 rows selects pandas."""
        ctx = _make_context(backend="auto")
        assert ctx.backend.name == "pandas"

    def test_default_is_pandas(self) -> None:
        """No backend arg defaults to pandas."""
        ctx = _make_context()
        assert ctx.backend.name == "pandas"


class TestCrossBackendEquivalence:
    """Verify pandas and duckdb produce identical results for each query."""

    _QUERIES: list[tuple[str, str]] = [
        ("MATCH (p:Person) RETURN p.name AS name ORDER BY name", "name"),
        (
            "MATCH (p:Person) WHERE p.age >= 28 RETURN p.name AS name ORDER BY name",
            "name",
        ),
        (
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name AS src, b.name AS tgt ORDER BY src, tgt",
            "src",
        ),
    ]

    @pytest.mark.parametrize(("query", "sort_col"), _QUERIES)
    def test_equivalence(self, query: str, sort_col: str) -> None:
        """Pandas and DuckDB produce same result for the same query."""
        pandas_ctx = _make_context(backend="pandas")
        duckdb_ctx = _make_context(backend="duckdb")

        pandas_result = Star(context=pandas_ctx).execute_query(query)
        duckdb_result = Star(context=duckdb_ctx).execute_query(query)

        # Same columns
        assert list(pandas_result.columns) == list(duckdb_result.columns)
        # Same row count
        assert len(pandas_result) == len(duckdb_result)
        # Same values (sorted for determinism)
        for col in pandas_result.columns:
            assert sorted(pandas_result[col].tolist()) == sorted(
                duckdb_result[col].tolist()
            ), f"Column {col!r} differs between backends"
