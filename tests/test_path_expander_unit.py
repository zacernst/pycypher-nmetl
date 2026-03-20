"""Unit tests for PathExpander — BFS-based path expansion engine.

Tests variable-length path expansion, shortest-path BFS, frontier
deduplication, and edge cases.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    NodePattern,
    PatternPath,
    RelationshipDirection,
    RelationshipPattern,
    Variable,
)
from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX, BindingFrame
from pycypher.path_expander import (
    _MAX_FRONTIER_ROWS,
    _MAX_UNBOUNDED_PATH_HOPS,
    _VL_TIP_COL,
    PathExpander,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_chain_context(n_nodes: int = 5) -> Context:
    """Create a linear chain: 1->2->3->...->n.

    Useful for testing hop-bounded BFS.
    """
    people_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_nodes + 1)),
            "name": [f"Node{i}" for i in range(1, n_nodes + 1)],
        }
    )
    edges = pd.DataFrame(
        {
            ID_COLUMN: list(range(101, 101 + n_nodes - 1)),
            "__SOURCE__": list(range(1, n_nodes)),
            "__TARGET__": list(range(2, n_nodes + 1)),
        }
    )
    person_table = EntityTable(
        entity_type="Node",
        identifier="Node",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    edge_table = RelationshipTable(
        relationship_type="NEXT",
        identifier="NEXT",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=edges,
        source_entity_type="Node",
        target_entity_type="Node",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Node": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"NEXT": edge_table}),
    )


def _make_cycle_context() -> Context:
    """Create a 3-node cycle: 1->2->3->1."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["A", "B", "C"],
        }
    )
    edges = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "__SOURCE__": [1, 2, 3],
            "__TARGET__": [2, 3, 1],
        }
    )
    person_table = EntityTable(
        entity_type="Node",
        identifier="Node",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    edge_table = RelationshipTable(
        relationship_type="NEXT",
        identifier="NEXT",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=edges,
        source_entity_type="Node",
        target_entity_type="Node",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Node": person_table}),
        relationship_mapping=RelationshipMapping(mapping={"NEXT": edge_table}),
    )


def _start_frame(ctx: Context, var: str = "a") -> BindingFrame:
    """Create a BindingFrame with all Node IDs."""
    from pycypher.binding_frame import EntityScan

    return EntityScan("Node", var).scan(ctx)


# ===========================================================================
# expand_variable_length_path tests
# ===========================================================================


class TestExpandVariableLengthPath:
    """Tests for hop-bounded BFS expansion."""

    def test_min1_max1_single_hop(self) -> None:
        """[*1..1] produces direct neighbors only."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=1,
            anon_counter=[0],
        )
        # Chain 1->2, 2->3, 3->4, 4->5 = 4 edges
        assert len(frame.bindings) == 4
        assert "b" in frame.var_names

    def test_min1_max2_two_hops(self) -> None:
        """[*1..2] produces 1-hop and 2-hop results."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=2,
            anon_counter=[0],
        )
        # 1-hop: 4 pairs, 2-hop: 3 pairs = 7 total
        assert len(frame.bindings) == 7

    def test_min2_max2_skips_hop1(self) -> None:
        """[*2..2] skips single-hop results."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=2,
            max_hops=2,
            anon_counter=[0],
        )
        # 2-hop: 1->3, 2->4, 3->5 = 3 pairs
        assert len(frame.bindings) == 3

    def test_left_direction(self) -> None:
        """LEFT direction traverses edges backwards."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.LEFT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=1,
            anon_counter=[0],
        )
        # Reverse: 2<-1, 3<-2, 4<-3, 5<-4 = 4 edges
        assert len(frame.bindings) == 4

    def test_path_length_col(self) -> None:
        """Path length column records hop count."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")
        hop_col = f"{PATH_HOP_COLUMN_PREFIX}p"

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=2,
            anon_counter=[0],
            path_length_col=hop_col,
        )
        assert hop_col in frame.bindings.columns
        hop_values = set(frame.bindings[hop_col].unique())
        assert hop_values == {1, 2}

    def test_empty_result_returns_empty_frame(self) -> None:
        """No reachable nodes returns empty frame with correct columns."""
        # Disconnected: only node 1, no edges
        people_df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alone"]})
        edges = pd.DataFrame(
            {
                ID_COLUMN: pd.Series(dtype=int),
                "__SOURCE__": pd.Series(dtype=int),
                "__TARGET__": pd.Series(dtype=int),
            }
        )
        person_table = EntityTable(
            entity_type="Node",
            identifier="Node",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=people_df,
        )
        edge_table = RelationshipTable(
            relationship_type="NEXT",
            identifier="NEXT",
            column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=edges,
            source_entity_type="Node",
            target_entity_type="Node",
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Node": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"NEXT": edge_table}
            ),
        )
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=3,
            anon_counter=[0],
        )
        assert frame.bindings.empty
        assert "b" in frame.bindings.columns

    def test_row_limit_stops_early(self) -> None:
        """Row limit truncates BFS results."""
        ctx = _make_chain_context(10)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=5,
            anon_counter=[0],
            row_limit=5,
        )
        assert len(frame.bindings) <= 5

    def test_cycle_deduplication(self) -> None:
        """Cycles are deduplicated so rows don't explode."""
        ctx = _make_cycle_context()
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=5,
            anon_counter=[0],
        )
        # 3-node cycle: each node reaches each node at various hops
        # but dedup means limited growth
        assert len(frame.bindings) <= 30  # bounded, not exponential

    def test_unbounded_max_uses_default_cap(self) -> None:
        """max_hops=None uses _MAX_UNBOUNDED_PATH_HOPS."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=None,
            anon_counter=[0],
        )
        # Chain of 5 nodes: max reachable hops = 4
        # All reachable pairs found
        assert len(frame.bindings) >= 4

    def test_end_type_in_registry(self) -> None:
        """end_type is correctly added to type_registry."""
        ctx = _make_chain_context(3)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type="Node",
            min_hops=1,
            max_hops=1,
            anon_counter=[0],
        )
        assert frame.type_registry.get("b") == "Node"

    def test_end_type_none_not_in_registry(self) -> None:
        """end_type=None omits end_var from type_registry."""
        ctx = _make_chain_context(3)
        expander = PathExpander(ctx)
        start = _start_frame(ctx, "a")

        frame = expander.expand_variable_length_path(
            start_frame=start,
            start_var="a",
            rel_type="NEXT",
            direction=RelationshipDirection.RIGHT,
            end_var="b",
            end_type=None,
            min_hops=1,
            max_hops=1,
            anon_counter=[0],
        )
        assert "b" not in frame.type_registry


# ===========================================================================
# shortest_path_to_binding_frame tests
# ===========================================================================


class TestShortestPath:
    """Tests for shortestPath / allShortestPaths BFS."""

    def _node_scanner(self, ctx: Context):
        """Factory for node_scanner callable."""
        from pycypher.binding_frame import EntityScan

        def scan(
            node: NodePattern,
            anon_counter: list[int],
            context_frame: BindingFrame | None = None,
        ) -> BindingFrame:
            var_name = (
                node.variable.name
                if node.variable
                else f"_anon_node_{anon_counter[0]}"
            )
            if node.variable is None:
                anon_counter[0] += 1
            label = node.labels[0] if node.labels else "Node"
            return EntityScan(label, var_name).scan(ctx)

        return scan

    def test_shortest_path_chain(self) -> None:
        """shortestPath on chain finds direct paths."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)

        from pycypher.ast_models import PathLength

        path = PatternPath(
            elements=[
                NodePattern(
                    variable=Variable(name="a"),
                    labels=["Node"],
                ),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["NEXT"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, unbounded=True),
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Node"],
                ),
            ],
            shortest_path_mode="one",
        )

        frame = expander.shortest_path_to_binding_frame(
            path=path,
            anon_counter=[0],
            node_scanner=self._node_scanner(ctx),
        )
        # Each pair has exactly one shortest path
        assert len(frame.bindings) > 0
        assert "a" in frame.var_names
        assert "b" in frame.var_names

    def test_shortest_path_wrong_element_count(self) -> None:
        """shortestPath with != 3 elements raises ValueError."""
        ctx = _make_chain_context(3)
        expander = PathExpander(ctx)

        path = PatternPath(
            elements=[
                NodePattern(variable=Variable(name="a"), labels=["Node"]),
            ],
            shortest_path_mode="one",
        )
        with pytest.raises(ValueError, match="3 pattern elements"):
            expander.shortest_path_to_binding_frame(
                path=path,
                anon_counter=[0],
                node_scanner=self._node_scanner(ctx),
            )

    def test_shortest_path_no_rel_type_raises(self) -> None:
        """shortestPath without relationship type raises ValueError."""
        ctx = _make_chain_context(3)
        expander = PathExpander(ctx)

        from pycypher.ast_models import PathLength

        path = PatternPath(
            elements=[
                NodePattern(
                    variable=Variable(name="a"),
                    labels=["Node"],
                ),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=[],  # No type
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, unbounded=True),
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Node"],
                ),
            ],
            shortest_path_mode="one",
        )
        with pytest.raises(ValueError, match="one relationship type"):
            expander.shortest_path_to_binding_frame(
                path=path,
                anon_counter=[0],
                node_scanner=self._node_scanner(ctx),
            )

    def test_all_shortest_paths(self) -> None:
        """allShortestPaths returns all shortest paths."""
        ctx = _make_chain_context(5)
        expander = PathExpander(ctx)

        from pycypher.ast_models import PathLength

        path = PatternPath(
            elements=[
                NodePattern(
                    variable=Variable(name="a"),
                    labels=["Node"],
                ),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["NEXT"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, unbounded=True),
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Node"],
                ),
            ],
            shortest_path_mode="all",
        )

        frame = expander.shortest_path_to_binding_frame(
            path=path,
            anon_counter=[0],
            node_scanner=self._node_scanner(ctx),
        )
        assert len(frame.bindings) > 0

    def test_shortest_path_empty_result(self) -> None:
        """shortestPath with no reachable endpoints returns empty frame."""
        # Single node, no edges
        people_df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alone"]})
        edges = pd.DataFrame(
            {
                ID_COLUMN: pd.Series(dtype=int),
                "__SOURCE__": pd.Series(dtype=int),
                "__TARGET__": pd.Series(dtype=int),
            }
        )
        person_table = EntityTable(
            entity_type="Node",
            identifier="Node",
            column_names=[ID_COLUMN, "name"],
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=people_df,
        )
        edge_table = RelationshipTable(
            relationship_type="NEXT",
            identifier="NEXT",
            column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=edges,
            source_entity_type="Node",
            target_entity_type="Node",
        )
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Node": person_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"NEXT": edge_table}
            ),
        )
        expander = PathExpander(ctx)

        from pycypher.ast_models import PathLength

        path = PatternPath(
            elements=[
                NodePattern(
                    variable=Variable(name="a"),
                    labels=["Node"],
                ),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["NEXT"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, unbounded=True),
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Node"],
                ),
            ],
            shortest_path_mode="one",
        )

        frame = expander.shortest_path_to_binding_frame(
            path=path,
            anon_counter=[0],
            node_scanner=lambda n, ac, context_frame=None: BindingFrame(
                bindings=pd.DataFrame({"a": [1]}),
                type_registry={"a": "Node"},
                context=ctx,
            ),
        )
        assert frame.bindings.empty

    def test_shortest_path_no_scanner_raises(self) -> None:
        """shortestPath without node_scanner when start not pre-bound raises."""
        ctx = _make_chain_context(3)
        expander = PathExpander(ctx)

        from pycypher.ast_models import PathLength

        path = PatternPath(
            elements=[
                NodePattern(
                    variable=Variable(name="a"),
                    labels=["Node"],
                ),
                RelationshipPattern(
                    variable=Variable(name="r"),
                    labels=["NEXT"],
                    direction=RelationshipDirection.RIGHT,
                    length=PathLength(min=1, unbounded=True),
                ),
                NodePattern(
                    variable=Variable(name="b"),
                    labels=["Node"],
                ),
            ],
            shortest_path_mode="one",
        )
        with pytest.raises(ValueError, match="node_scanner is required"):
            expander.shortest_path_to_binding_frame(
                path=path,
                anon_counter=[0],
                node_scanner=None,
            )


# ===========================================================================
# Constants tests
# ===========================================================================


class TestConstants:
    """Verify module-level constants are sane."""

    def test_max_unbounded_positive(self) -> None:
        assert _MAX_UNBOUNDED_PATH_HOPS > 0

    def test_max_frontier_rows_positive(self) -> None:
        assert _MAX_FRONTIER_ROWS > 0

    def test_vl_tip_col_is_private(self) -> None:
        assert _VL_TIP_COL.startswith("_")
