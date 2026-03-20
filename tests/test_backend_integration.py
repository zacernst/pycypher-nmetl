"""Integration tests for BackendEngine wiring into the query pipeline.

Verifies that:
1. Context accepts backend= parameter and exposes .backend property
2. Queries produce identical results with PandasBackend (default)
3. The backend is accessible from Star and BindingFrame during execution
4. Context(backend="auto") selects the appropriate backend based on data size
"""

from __future__ import annotations

import pandas as pd
from pycypher.backend_engine import DuckDBBackend, PandasBackend
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

ID = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_social_context(**ctx_kwargs: object) -> tuple[Context, Star]:
    """Build a small social graph and return (context, star)."""
    persons_df = pd.DataFrame(
        {
            ID: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 22],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID: [1, 2, 3, 4],
            "__SOURCE__": [1, 2, 3, 1],
            "__TARGET__": [2, 3, 4, 5],
            "since": [2020, 2021, 2019, 2022],
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID, "__SOURCE__", "__TARGET__", "since"],
        source_obj_attribute_map={"since": "since"},
        attribute_map={"since": "since"},
        source_obj=knows_df,
        source_entity_type="Person",
        target_entity_type="Person",
    )
    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
        **ctx_kwargs,
    )
    return ctx, Star(context=ctx)


# ---------------------------------------------------------------------------
# Context backend wiring
# ---------------------------------------------------------------------------


class TestContextBackendWiring:
    """Verify Context accepts and exposes the backend parameter."""

    def test_default_backend_is_pandas(self) -> None:
        """Default Context uses PandasBackend."""
        ctx, _ = _build_social_context()
        assert isinstance(ctx.backend, PandasBackend)

    def test_explicit_pandas_backend(self) -> None:
        """Context(backend='pandas') uses PandasBackend."""
        ctx, _ = _build_social_context(backend="pandas")
        assert isinstance(ctx.backend, PandasBackend)

    def test_explicit_duckdb_backend(self) -> None:
        """Context(backend='duckdb') uses DuckDBBackend."""
        ctx, _ = _build_social_context(backend="duckdb")
        assert isinstance(ctx.backend, DuckDBBackend)

    def test_auto_backend_small_data(self) -> None:
        """Context(backend='auto') selects pandas for small datasets."""
        ctx, _ = _build_social_context(backend="auto")
        assert isinstance(ctx.backend, PandasBackend)

    def test_pre_constructed_backend(self) -> None:
        """Context accepts a pre-constructed BackendEngine instance."""
        engine = PandasBackend()
        ctx, _ = _build_social_context(backend=engine)
        assert ctx.backend is engine

    def test_backend_name_property(self) -> None:
        """Backend name is accessible via ctx.backend.name."""
        ctx, _ = _build_social_context(backend="pandas")
        assert ctx.backend.name == "pandas"

    def test_backend_accessible_from_star(self) -> None:
        """Star can access ctx.backend through its context reference."""
        ctx, star = _build_social_context(backend="pandas")
        assert star.context.backend is ctx.backend


# ---------------------------------------------------------------------------
# Query result equivalence
# ---------------------------------------------------------------------------


class TestQueryResultEquivalence:
    """Verify queries produce identical results regardless of backend."""

    def test_simple_scan(self) -> None:
        """MATCH (p:Person) RETURN p.name produces same results."""
        _, star = _build_social_context()
        result = star.execute_query("MATCH (p:Person) RETURN p.name")
        names = sorted(result["name"].tolist())
        assert names == ["Alice", "Bob", "Carol", "Dave", "Eve"]

    def test_filtered_scan(self) -> None:
        """WHERE clause filtering works with default backend."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 27 RETURN p.name",
        )
        names = sorted(result["name"].tolist())
        assert names == ["Alice", "Carol", "Dave"]

    def test_single_hop(self) -> None:
        """Single-hop relationship traversal."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "WHERE a.name = 'Alice' RETURN b.name",
        )
        names = sorted(result["name"].tolist())
        assert names == ["Bob", "Eve"]

    def test_aggregation(self) -> None:
        """Aggregation query with backend."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (p:Person) RETURN count(p) AS cnt",
        )
        assert result["cnt"].iloc[0] == 5

    def test_limit(self) -> None:
        """LIMIT clause works with backend wiring."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name LIMIT 3",
        )
        assert len(result) == 3

    def test_order_by(self) -> None:
        """ORDER BY works with backend."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name ORDER BY p.name",
        )
        names = result["name"].tolist()
        assert names == ["Alice", "Bob", "Carol", "Dave", "Eve"]

    def test_with_clause(self) -> None:
        """WITH clause piping works with backend."""
        _, star = _build_social_context()
        result = star.execute_query(
            "MATCH (p:Person) WITH p WHERE p.age >= 30 RETURN p.name",
        )
        names = sorted(result["name"].tolist())
        assert names == ["Alice", "Carol"]


# ---------------------------------------------------------------------------
# Backend operations via Context
# ---------------------------------------------------------------------------


class TestBackendOperationsDirect:
    """Verify BackendEngine operations work when called via context.backend."""

    def test_backend_scan_entity(self) -> None:
        """backend.scan_entity returns ID column."""
        ctx, _ = _build_social_context()
        persons_df = ctx.entity_mapping["Person"].source_obj
        result = ctx.backend.scan_entity(persons_df, "Person")
        assert "__ID__" in result.columns
        assert len(result) == 5

    def test_backend_filter(self) -> None:
        """backend.filter applies boolean mask."""
        ctx, _ = _build_social_context()
        df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
        mask = pd.Series([True, False, True, False, True])
        result = ctx.backend.filter(df, mask)
        assert list(result["a"]) == [1, 3, 5]

    def test_backend_join(self) -> None:
        """backend.join merges two frames."""
        ctx, _ = _build_social_context()
        left = pd.DataFrame({"id": [1, 2, 3], "val": ["a", "b", "c"]})
        right = pd.DataFrame({"id": [2, 3, 4], "val2": ["x", "y", "z"]})
        result = ctx.backend.join(left, right, on="id")
        assert len(result) == 2
        assert sorted(result["id"].tolist()) == [2, 3]

    def test_backend_limit(self) -> None:
        """backend.limit truncates rows."""
        ctx, _ = _build_social_context()
        df = pd.DataFrame({"a": range(10)})
        result = ctx.backend.limit(df, 3)
        assert len(result) == 3

    def test_backend_row_count(self) -> None:
        """backend.row_count returns correct count."""
        ctx, _ = _build_social_context()
        df = pd.DataFrame({"a": range(7)})
        assert ctx.backend.row_count(df) == 7

    def test_backend_to_pandas(self) -> None:
        """backend.to_pandas returns a pandas DataFrame."""
        ctx, _ = _build_social_context()
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = ctx.backend.to_pandas(df)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
