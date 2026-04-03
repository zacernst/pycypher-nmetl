"""Optimizer regression guard tests.

Targeted invariant tests that prevent optimizer transforms from silently
producing wrong results.  Motivated by the shortestPath reordering bug
where MATCH clause reordering broke variable binding dependencies.

Guards cover:
  1. shortestPath / allShortestPaths reordering protection
  2. Cross-MATCH WHERE variable reference detection
  3. Filter fusion semantic equivalence
  4. Filter pushdown join-boundary safety
  5. End-to-end semantic preservation across all passes
"""

from __future__ import annotations

import copy

import pandas as pd
import pytest
from pycypher.ast_converter import _parse_cypher_cached
from pycypher.ast_models import Match
from pycypher.lazy_eval import (
    ComputationGraph,
    OpNode,
    OpType,
    fuse_filters,
    push_filters_down,
)
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_fresh(cypher: str):
    """Parse Cypher and return a mutable deep copy of the AST."""
    return copy.deepcopy(_parse_cypher_cached(cypher))


def _make_entity_table(
    label: str,
    n_rows: int,
    extra_cols: dict | None = None,
) -> EntityTable:
    """Create an EntityTable with n_rows synthetic rows."""
    data = {ID_COLUMN: list(range(1, n_rows + 1))}
    attr_map = {}
    if extra_cols:
        data.update(extra_cols)
        attr_map = {c: c for c in extra_cols}
    return EntityTable(
        entity_type=label,
        identifier=label,
        column_names=list(pd.DataFrame(data).columns),
        source_obj_attribute_map=attr_map,
        attribute_map=attr_map,
        source_obj=pd.DataFrame(data),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def asymmetric_context() -> Context:
    """Context with asymmetric table sizes: Small(5), Medium(50), Large(500)."""
    small = _make_entity_table("Small", 5, {"val": list(range(5))})
    medium = _make_entity_table("Medium", 50, {"val": list(range(50))})
    large = _make_entity_table("Large", 500, {"val": list(range(500))})
    return Context(
        entity_mapping=EntityMapping(
            mapping={"Small": small, "Medium": medium, "Large": large},
        ),
    )


@pytest.fixture
def diamond_context() -> Context:
    """Diamond graph: Alice->Bob->Dave, Alice->Carol->Dave."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]},
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [10, 11, 12, 13],
            "__SOURCE__": [1, 1, 2, 3],
            "__TARGET__": [2, 3, 4, 4],
        },
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


# ===================================================================
# 1. shortestPath reordering protection
# ===================================================================


class TestShortestPathReorderingGuard:
    """Ensure optimizer never reorders MATCHes containing shortestPath."""

    def test_shortest_path_match_not_reordered(
        self,
        diamond_context: Context,
    ) -> None:
        """MATCH run with shortestPath must be skipped by reordering."""
        ast = _parse_fresh(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops",
        )
        star = Star(context=diamond_context)
        original_clauses = [repr(c) for c in ast.clauses]
        star._apply_match_reordering(ast)
        reordered_clauses = [repr(c) for c in ast.clauses]
        assert original_clauses == reordered_clauses, (
            "shortestPath MATCH run was reordered — this breaks variable binding"
        )

    def test_all_shortest_paths_match_not_reordered(
        self,
        diamond_context: Context,
    ) -> None:
        """AllShortestPaths variant must also be protected from reordering."""
        ast = _parse_fresh(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = allShortestPaths((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops",
        )
        star = Star(context=diamond_context)
        original_clauses = [repr(c) for c in ast.clauses]
        star._apply_match_reordering(ast)
        reordered_clauses = [repr(c) for c in ast.clauses]
        assert original_clauses == reordered_clauses

    def test_shortest_path_result_correct_after_optimizer(
        self,
        diamond_context: Context,
    ) -> None:
        """End-to-end: shortestPath produces correct hop count through optimizer."""
        star = Star(context=diamond_context)
        result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'}) "
            "MATCH p = shortestPath((a)-[:KNOWS*]->(b)) "
            "RETURN length(p) AS hops",
        )
        assert len(result) >= 1
        assert result["hops"].iloc[0] == 2, (
            "Shortest path Alice->Dave should be 2 hops"
        )

    def test_non_shortest_path_still_reordered(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Normal MATCHes without shortestPath should still be reordered."""
        ast = _parse_fresh(
            "MATCH (a:Large) MATCH (b:Small) RETURN count(*) AS cnt",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        first_label = matches[0].pattern.paths[0].elements[0].labels[0]
        assert first_label == "Small", (
            "Normal MATCHes should still be reordered smallest-first"
        )


# ===================================================================
# 2. Cross-MATCH WHERE variable reference detection
# ===================================================================


class TestCrossMatchWhereGuard:
    """Ensure optimizer detects cross-MATCH variable references in WHERE."""

    def test_where_referencing_other_match_prevents_reorder(
        self,
        asymmetric_context: Context,
    ) -> None:
        """WHERE clause with cross-MATCH variable reference blocks reordering."""
        ast = _parse_fresh(
            "MATCH (a:Large) "
            "MATCH (b:Small) WHERE b.val = a.val "
            "RETURN a.val AS v LIMIT 5",
        )
        star = Star(context=asymmetric_context)
        original_clauses = [repr(c) for c in ast.clauses]
        star._apply_match_reordering(ast)
        reordered_clauses = [repr(c) for c in ast.clauses]
        assert original_clauses == reordered_clauses, (
            "Cross-MATCH WHERE reference should prevent reordering"
        )

    def test_where_referencing_own_match_allows_reorder(
        self,
        asymmetric_context: Context,
    ) -> None:
        """WHERE clause referencing only its own MATCH allows reordering."""
        ast = _parse_fresh(
            "MATCH (a:Large) WHERE a.val > 0 MATCH (b:Small) RETURN count(*) AS cnt",
        )
        star = Star(context=asymmetric_context)
        star._apply_match_reordering(ast)
        matches = [c for c in ast.clauses if isinstance(c, Match)]
        first_label = matches[0].pattern.paths[0].elements[0].labels[0]
        assert first_label == "Small", (
            "Self-referencing WHERE should not block reordering"
        )

    def test_cross_ref_preserves_result_correctness(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Cross-MATCH WHERE query produces correct results regardless."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Small) "
            "MATCH (b:Small) WHERE b.val = a.val "
            "RETURN count(*) AS cnt",
        )
        # Self-join on val: each of the 5 rows matches itself => 5
        assert result["cnt"].iloc[0] == 5


# ===================================================================
# 3. Filter fusion semantic equivalence
# ===================================================================


class TestFilterFusionGuard:
    """Ensure fuse_filters preserves predicate semantics."""

    def _make_filter_chain(self) -> ComputationGraph:
        """Build a SCAN -> FILTER(p1) -> FILTER(p2) graph."""
        g = ComputationGraph()
        scan = OpNode(
            op_type=OpType.SCAN,
            params={"table": "t"},
            inputs=[],
            estimated_rows=100,
        )
        scan_id = g.add_node(scan)
        f1 = OpNode(
            op_type=OpType.FILTER,
            params={"predicate": "x > 0"},
            inputs=[scan_id],
            estimated_rows=50,
        )
        f1_id = g.add_node(f1)
        f2 = OpNode(
            op_type=OpType.FILTER,
            params={"predicate": "y < 10"},
            inputs=[f1_id],
            estimated_rows=25,
        )
        f2_id = g.add_node(f2)
        g.output_node = f2_id
        return g

    def test_fused_graph_has_fewer_nodes(self) -> None:
        """Fusion should reduce node count (2 filters -> 1)."""
        g = self._make_filter_chain()
        assert len(g.nodes) == 3  # scan + 2 filters
        fused = fuse_filters(g)
        assert len(fused.nodes) == 2  # scan + 1 fused filter

    def test_fused_filter_combines_predicates(self) -> None:
        """Fused filter should contain both predicates."""
        g = self._make_filter_chain()
        fused = fuse_filters(g)
        fused_node = fused.nodes[fused.output_node]
        assert fused_node.op_type == OpType.FILTER
        preds = fused_node.params.get("predicates", [])
        assert "x > 0" in preds
        assert "y < 10" in preds

    def test_single_filter_not_changed(self) -> None:
        """A single filter should pass through unchanged."""
        g = ComputationGraph()
        scan_id = g.add_node(
            OpNode(
                op_type=OpType.SCAN, params={}, inputs=[], estimated_rows=100
            ),
        )
        f_id = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                params={"predicate": "x > 0"},
                inputs=[scan_id],
                estimated_rows=50,
            ),
        )
        g.output_node = f_id
        fused = fuse_filters(g)
        assert len(fused.nodes) == 2


# ===================================================================
# 4. Filter pushdown join-boundary safety
# ===================================================================


class TestFilterPushdownGuard:
    """Ensure filters only push below joins when safe."""

    def _make_join_with_filter(
        self,
        filter_cols: list[str],
        left_cols: list[str],
        right_cols: list[str],
    ) -> ComputationGraph:
        """Build a LEFT_SCAN -> JOIN <- RIGHT_SCAN -> FILTER graph."""
        g = ComputationGraph()
        left_id = g.add_node(
            OpNode(
                op_type=OpType.SCAN,
                params={"table": "left"},
                inputs=[],
                estimated_rows=100,
            ),
        )
        right_id = g.add_node(
            OpNode(
                op_type=OpType.SCAN,
                params={"table": "right"},
                inputs=[],
                estimated_rows=100,
            ),
        )
        join_id = g.add_node(
            OpNode(
                op_type=OpType.JOIN,
                params={
                    "left_columns": left_cols,
                    "right_columns": right_cols,
                },
                inputs=[left_id, right_id],
                estimated_rows=200,
            ),
        )
        filter_id = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                params={
                    "predicate": "col_filter",
                    "columns_referenced": filter_cols,
                },
                inputs=[join_id],
                estimated_rows=100,
            ),
        )
        g.output_node = filter_id
        return g

    def test_left_only_filter_pushes_below_join(self) -> None:
        """Filter referencing only left columns should push below join."""
        g = self._make_join_with_filter(
            filter_cols=["a"],
            left_cols=["a", "b"],
            right_cols=["c", "d"],
        )
        optimised = push_filters_down(g)
        # Filter should now be below the join, not above it
        output = optimised.nodes[optimised.output_node]
        assert output.op_type == OpType.JOIN, (
            "After pushdown, output should be the JOIN (filter moved below)"
        )

    def test_right_only_filter_pushes_below_join(self) -> None:
        """Filter referencing only right columns should push below join."""
        g = self._make_join_with_filter(
            filter_cols=["c"],
            left_cols=["a", "b"],
            right_cols=["c", "d"],
        )
        optimised = push_filters_down(g)
        output = optimised.nodes[optimised.output_node]
        assert output.op_type == OpType.JOIN

    def test_both_sides_filter_stays_above_join(self) -> None:
        """Filter referencing both sides of join must NOT push down."""
        g = self._make_join_with_filter(
            filter_cols=["a", "c"],
            left_cols=["a", "b"],
            right_cols=["c", "d"],
        )
        optimised = push_filters_down(g)
        output = optimised.nodes[optimised.output_node]
        assert output.op_type == OpType.FILTER, (
            "Filter with cross-join references must stay above join"
        )

    def test_empty_filter_cols_stays_above_join(self) -> None:
        """Filter with no column info stays above join (conservative)."""
        g = self._make_join_with_filter(
            filter_cols=[],
            left_cols=["a"],
            right_cols=["b"],
        )
        optimised = push_filters_down(g)
        output = optimised.nodes[optimised.output_node]
        assert output.op_type == OpType.FILTER


# ===================================================================
# 5. End-to-end semantic preservation
# ===================================================================


class TestOptimizerSemanticPreservation:
    """Verify query results are identical with and without optimizer."""

    def test_reordering_preserves_count(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Cross-join count is invariant under MATCH reordering."""
        star = Star(context=asymmetric_context)
        # Large(500) x Small(5) = 2500 regardless of order
        result = star.execute_query(
            "MATCH (a:Large) MATCH (b:Small) RETURN count(*) AS cnt",
        )
        assert result["cnt"].iloc[0] == 2500

    def test_reordering_preserves_all_values(
        self,
        asymmetric_context: Context,
    ) -> None:
        """All column values are preserved after reordering."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Small) MATCH (b:Small) RETURN a.val AS a_val, b.val AS b_val",
        )
        # 5 x 5 = 25 rows, all val combinations
        assert len(result) == 25
        assert set(result["a_val"]) == {0, 1, 2, 3, 4}
        assert set(result["b_val"]) == {0, 1, 2, 3, 4}

    def test_three_way_reordering_preserves_count(
        self,
        asymmetric_context: Context,
    ) -> None:
        """Three-way reordering preserves cardinality."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Large) MATCH (b:Medium) MATCH (c:Small) RETURN count(*) AS cnt",
        )
        assert result["cnt"].iloc[0] == 500 * 50 * 5

    def test_optional_match_semantics_preserved(
        self,
        asymmetric_context: Context,
    ) -> None:
        """OPTIONAL MATCH is never reordered — left-join semantics preserved."""
        star = Star(context=asymmetric_context)
        result = star.execute_query(
            "MATCH (a:Small) OPTIONAL MATCH (b:Large) RETURN count(*) AS cnt",
        )
        assert result["cnt"].iloc[0] == 5 * 500
