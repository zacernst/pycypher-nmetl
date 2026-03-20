"""Tests for lazy variable-length path expansion with LIMIT pushdown.

Verifies that:
1. Variable-length path queries still produce correct results
2. The row_limit parameter on _expand_variable_length_path works
3. LIMIT queries produce correct results (same rows as unlimited + truncation)
"""

from __future__ import annotations

import pandas as pd
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ID = "__ID__"


def _build_chain_graph(n_persons: int, n_edges: int) -> tuple[Context, Star]:
    """Build a linear chain graph: 1→2→3→...→n."""
    persons_df = pd.DataFrame(
        {
            ID: list(range(1, n_persons + 1)),
            "name": [f"P{i}" for i in range(1, n_persons + 1)],
        }
    )
    # Chain: 1→2, 2→3, ..., (n-1)→n
    actual_edges = min(n_edges, n_persons - 1)
    knows_df = pd.DataFrame(
        {
            ID: list(range(1, actual_edges + 1)),
            "__SOURCE__": list(range(1, actual_edges + 1)),
            "__TARGET__": list(range(2, actual_edges + 2)),
            "since": [2020] * actual_edges,
        }
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
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
    )
    return ctx, Star(context=ctx)


def _build_dense_graph(
    n_persons: int, edges_per_node: int = 5
) -> tuple[Context, Star]:
    """Build a dense random-ish graph for stress testing."""
    import numpy as np

    rng = np.random.default_rng(42)
    persons_df = pd.DataFrame(
        {
            ID: list(range(1, n_persons + 1)),
            "name": [f"P{i}" for i in range(1, n_persons + 1)],
        }
    )
    n_edges = n_persons * edges_per_node
    sources = rng.integers(1, n_persons + 1, size=n_edges)
    targets = rng.integers(1, n_persons + 1, size=n_edges)
    mask = sources != targets
    sources = sources[mask]
    targets = targets[mask]
    n_actual = len(sources)
    knows_df = pd.DataFrame(
        {
            ID: list(range(1, n_actual + 1)),
            "__SOURCE__": sources,
            "__TARGET__": targets,
            "since": [2020] * n_actual,
        }
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
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
    )
    return ctx, Star(context=ctx)


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


class TestVarLengthPathCorrectness:
    """Verify variable-length path queries return correct results."""

    def test_chain_1_to_2_hops(self) -> None:
        """Chain graph: 1→2→3→4. Path *1..2 from node 1."""
        ctx, star = _build_chain_graph(4, 3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'P1' RETURN a.name, b.name"
        )
        endpoints = sorted(result["b.name"].tolist())
        # 1 hop: 1→2; 2 hops: 1→2→3
        assert endpoints == ["P2", "P3"]

    def test_chain_1_to_3_hops(self) -> None:
        """Chain graph with 3-hop expansion."""
        ctx, star = _build_chain_graph(5, 4)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) "
            "WHERE a.name = 'P1' RETURN a.name, b.name"
        )
        endpoints = sorted(result["b.name"].tolist())
        assert endpoints == ["P2", "P3", "P4"]

    def test_empty_result_min_hops(self) -> None:
        """No paths of length >= 5 in a 4-node chain."""
        ctx, star = _build_chain_graph(4, 3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*5..6]->(b:Person) "
            "WHERE a.name = 'P1' RETURN a.name, b.name"
        )
        assert len(result) == 0

    def test_var_length_all_start_nodes(self) -> None:
        """All nodes as start points in a chain."""
        ctx, star = _build_chain_graph(4, 3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name"
        )
        # Node 1: reaches 2, 3; Node 2: reaches 3, 4; Node 3: reaches 4
        # Total: 5 rows
        assert len(result) == 5


class TestVarLengthPathWithLimit:
    """Verify LIMIT works correctly with variable-length paths."""

    def test_limit_on_varlength_path(self) -> None:
        """LIMIT should truncate var-length path results."""
        ctx, star = _build_dense_graph(100, edges_per_node=5)
        # Get full result count
        full = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name"
        )
        limited = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name LIMIT 10"
        )
        assert len(limited) == 10
        assert len(full) > 10  # Sanity check

    def test_limit_larger_than_result(self) -> None:
        """LIMIT larger than result set returns all rows."""
        ctx, star = _build_chain_graph(4, 3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "WHERE a.name = 'P1' RETURN a.name, b.name LIMIT 100"
        )
        assert len(result) == 2  # Only 2 reachable nodes

    def test_limit_1(self) -> None:
        """LIMIT 1 returns exactly one row."""
        ctx, star = _build_dense_graph(100, edges_per_node=5)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name LIMIT 1"
        )
        assert len(result) == 1


class TestLimitPushdownWiring:
    """Verify that LIMIT is automatically pushed down to BFS via _extract_limit_hint."""

    def test_limit_pushdown_activates(self) -> None:
        """Simple MATCH + RETURN LIMIT should trigger pushdown."""
        ctx, star = _build_dense_graph(200, edges_per_node=5)
        # This query has the safe pattern: MATCH ... RETURN ... LIMIT N
        # (no aggregation, no DISTINCT, no ORDER BY, no SKIP)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name LIMIT 10"
        )
        assert len(result) == 10

    def test_limit_pushdown_not_activated_with_distinct(self) -> None:
        """DISTINCT prevents pushdown — result should still be correct."""
        ctx, star = _build_dense_graph(50, edges_per_node=3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN DISTINCT a.name, b.name LIMIT 5"
        )
        assert len(result) <= 5

    def test_limit_pushdown_not_activated_with_aggregation(self) -> None:
        """Aggregation prevents pushdown — result should still be correct."""
        ctx, star = _build_dense_graph(50, edges_per_node=3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name, count(b) AS cnt LIMIT 5"
        )
        assert len(result) <= 5

    def test_limit_pushdown_not_activated_with_order_by(self) -> None:
        """ORDER BY prevents pushdown — result should still be correct."""
        ctx, star = _build_dense_graph(50, edges_per_node=3)
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) "
            "RETURN a.name, b.name ORDER BY a.name LIMIT 5"
        )
        assert len(result) == 5

    def test_extract_limit_hint_simple_case(self) -> None:
        """Directly test _extract_limit_hint on a simple query."""
        from pycypher.ast_models import ASTConverter

        converter = ASTConverter()
        query = converter.from_cypher(
            "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) RETURN a.name, b.name LIMIT 10"
        )
        ctx, star = _build_dense_graph(10, edges_per_node=2)
        hint = star._extract_limit_hint(query)
        assert hint == 10

    def test_extract_limit_hint_rejects_aggregation(self) -> None:
        """_extract_limit_hint should return None when aggregation present."""
        from pycypher.ast_models import ASTConverter

        converter = ASTConverter()
        query = converter.from_cypher(
            "MATCH (a:Person) RETURN a.dept, count(a) AS cnt LIMIT 10"
        )
        ctx, star = _build_dense_graph(10, edges_per_node=2)
        hint = star._extract_limit_hint(query)
        assert hint is None

    def test_extract_limit_hint_rejects_no_limit(self) -> None:
        """_extract_limit_hint should return None when no LIMIT present."""
        from pycypher.ast_models import ASTConverter

        converter = ASTConverter()
        query = converter.from_cypher("MATCH (a:Person) RETURN a.name")
        ctx, star = _build_dense_graph(10, edges_per_node=2)
        hint = star._extract_limit_hint(query)
        assert hint is None


class TestRowLimitParameter:
    """Directly test the row_limit parameter on _expand_variable_length_path."""

    def test_row_limit_caps_results(self) -> None:
        """row_limit should cap the total result rows from BFS."""
        from pycypher.binding_frame import EntityScan

        ctx, star = _build_dense_graph(200, edges_per_node=5)

        # Get the full expansion count
        scan = EntityScan(entity_type="Person", var_name="a")
        start_frame = scan.scan(ctx)

        from pycypher.ast_models import RelationshipDirection

        full_result = star._path_expander.expand_variable_length_path(
            start_frame=start_frame,
            start_var="a",
            rel_type="KNOWS",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Person",
            min_hops=1,
            max_hops=3,
            anon_counter=[0],
        )

        limited_result = star._path_expander.expand_variable_length_path(
            start_frame=start_frame,
            start_var="a",
            rel_type="KNOWS",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Person",
            min_hops=1,
            max_hops=3,
            anon_counter=[0],
            row_limit=50,
        )

        assert len(full_result.bindings) > 50
        assert len(limited_result.bindings) == 50

    def test_row_limit_none_returns_all(self) -> None:
        """row_limit=None should return all results (default behavior)."""
        from pycypher.binding_frame import EntityScan

        ctx, star = _build_chain_graph(5, 4)

        scan = EntityScan(entity_type="Person", var_name="a")
        start_frame = scan.scan(ctx)

        from pycypher.ast_models import RelationshipDirection

        result = star._path_expander.expand_variable_length_path(
            start_frame=start_frame,
            start_var="a",
            rel_type="KNOWS",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Person",
            min_hops=1,
            max_hops=3,
            anon_counter=[0],
            row_limit=None,
        )

        # Chain 1→2→3→4→5 with max 3 hops:
        # node 1 reaches {2,3,4}, node 2 reaches {3,4,5},
        # node 3 reaches {4,5}, node 4 reaches {5} = 3+3+2+1 = 9
        assert len(result.bindings) == 9
