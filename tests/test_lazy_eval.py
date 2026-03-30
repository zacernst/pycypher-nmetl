"""Tests for the lazy evaluation computation graph engine.

Validates graph construction, optimisation passes (filter fusion,
predicate pushdown), and memory estimation.
"""

from __future__ import annotations

from pycypher.lazy_eval import (
    ComputationGraph,
    OpNode,
    OpType,
    estimate_memory,
    fuse_filters,
    push_filters_down,
)

# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------


class TestComputationGraph:
    """Verify basic graph construction and traversal."""

    def test_add_node(self) -> None:
        """Nodes can be added and retrieved."""
        g = ComputationGraph()
        nid = g.add_node(
            OpNode(op_type=OpType.SCAN, params={"table": "Person"}),
        )
        assert nid == 0
        assert g.nodes[0].op_type == OpType.SCAN

    def test_sequential_ids(self) -> None:
        """Node IDs are assigned sequentially."""
        g = ComputationGraph()
        id0 = g.add_node(OpNode(op_type=OpType.SCAN))
        id1 = g.add_node(OpNode(op_type=OpType.FILTER, inputs=[id0]))
        assert id0 == 0
        assert id1 == 1

    def test_output_node_tracks_last(self) -> None:
        """Output node is always the last added."""
        g = ComputationGraph()
        g.add_node(OpNode(op_type=OpType.SCAN))
        g.add_node(OpNode(op_type=OpType.FILTER, inputs=[0]))
        g.add_node(OpNode(op_type=OpType.PROJECT, inputs=[1]))
        assert g.output_node == 2

    def test_topological_order(self) -> None:
        """Topological order puts inputs before consumers."""
        g = ComputationGraph()
        scan = g.add_node(OpNode(op_type=OpType.SCAN))
        filt = g.add_node(OpNode(op_type=OpType.FILTER, inputs=[scan]))
        proj = g.add_node(OpNode(op_type=OpType.PROJECT, inputs=[filt]))
        order = g.topological_order()
        assert order == [scan, filt, proj]

    def test_topological_order_diamond(self) -> None:
        """Diamond DAG: two paths merge at a join."""
        g = ComputationGraph()
        scan_a = g.add_node(OpNode(op_type=OpType.SCAN, params={"table": "A"}))
        scan_b = g.add_node(OpNode(op_type=OpType.SCAN, params={"table": "B"}))
        join = g.add_node(
            OpNode(op_type=OpType.JOIN, inputs=[scan_a, scan_b]),
        )
        order = g.topological_order()
        # Both scans must appear before the join
        assert order.index(scan_a) < order.index(join)
        assert order.index(scan_b) < order.index(join)

    def test_get_inputs(self) -> None:
        """get_inputs returns the input OpNode objects."""
        g = ComputationGraph()
        scan = g.add_node(OpNode(op_type=OpType.SCAN))
        filt = g.add_node(OpNode(op_type=OpType.FILTER, inputs=[scan]))
        inputs = g.get_inputs(filt)
        assert len(inputs) == 1
        assert inputs[0].op_type == OpType.SCAN


# ---------------------------------------------------------------------------
# Filter fusion
# ---------------------------------------------------------------------------


class TestFilterFusion:
    """Verify consecutive filters are fused into one."""

    def test_two_filters_fused(self) -> None:
        """Two consecutive filters become one fused filter."""
        g = ComputationGraph()
        scan = g.add_node(OpNode(op_type=OpType.SCAN))
        f1 = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[scan],
                params={"predicate": "age > 20"},
            ),
        )
        f2 = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[f1],
                params={"predicate": "name STARTS WITH 'A'"},
            ),
        )

        optimised = fuse_filters(g)
        # Should have scan + one fused filter = 2 nodes
        filter_nodes = [
            n for n in optimised.nodes.values() if n.op_type == OpType.FILTER
        ]
        assert len(filter_nodes) == 1
        assert filter_nodes[0].params.get("fused") is True

    def test_non_consecutive_filters_not_fused(self) -> None:
        """Filters separated by a project are not fused."""
        g = ComputationGraph()
        scan = g.add_node(OpNode(op_type=OpType.SCAN))
        f1 = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[scan],
                params={"predicate": "a"},
            ),
        )
        proj = g.add_node(OpNode(op_type=OpType.PROJECT, inputs=[f1]))
        f2 = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[proj],
                params={"predicate": "b"},
            ),
        )

        optimised = fuse_filters(g)
        filter_nodes = [
            n for n in optimised.nodes.values() if n.op_type == OpType.FILTER
        ]
        assert len(filter_nodes) == 2  # Not fused

    def test_single_filter_unchanged(self) -> None:
        """A single filter passes through unchanged."""
        g = ComputationGraph()
        scan = g.add_node(OpNode(op_type=OpType.SCAN))
        g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[scan],
                params={"predicate": "x"},
            ),
        )

        optimised = fuse_filters(g)
        filter_nodes = [
            n for n in optimised.nodes.values() if n.op_type == OpType.FILTER
        ]
        assert len(filter_nodes) == 1
        assert filter_nodes[0].params.get("fused") is None


# ---------------------------------------------------------------------------
# Predicate pushdown
# ---------------------------------------------------------------------------


class TestPredicatePushdown:
    """Verify filters are pushed below joins when possible."""

    def test_filter_pushed_to_left(self) -> None:
        """Filter on left-only columns moves below join to left input."""
        g = ComputationGraph()
        scan_l = g.add_node(
            OpNode(op_type=OpType.SCAN, params={"table": "left"}),
        )
        scan_r = g.add_node(
            OpNode(op_type=OpType.SCAN, params={"table": "right"}),
        )
        join = g.add_node(
            OpNode(
                op_type=OpType.JOIN,
                inputs=[scan_l, scan_r],
                params={
                    "left_columns": ["a", "id"],
                    "right_columns": ["b", "id"],
                },
            ),
        )
        g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[join],
                params={
                    "predicate": "a > 10",
                    "columns_referenced": ["a"],
                },
            ),
        )

        optimised = push_filters_down(g)
        # The filter should now be an input to the join, not after it
        order = optimised.topological_order()
        filter_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.FILTER
        ]
        join_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.JOIN
        ]
        assert len(filter_nodes) >= 1
        assert len(join_nodes) >= 1
        # Filter should appear before join in execution order
        assert filter_nodes[0] < join_nodes[0]

    def test_filter_pushed_to_right(self) -> None:
        """Filter on right-only columns moves below join to right input."""
        g = ComputationGraph()
        scan_l = g.add_node(
            OpNode(op_type=OpType.SCAN, params={"table": "left"}),
        )
        scan_r = g.add_node(
            OpNode(op_type=OpType.SCAN, params={"table": "right"}),
        )
        join = g.add_node(
            OpNode(
                op_type=OpType.JOIN,
                inputs=[scan_l, scan_r],
                params={
                    "left_columns": ["a", "id"],
                    "right_columns": ["b", "id"],
                },
            ),
        )
        g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[join],
                params={
                    "predicate": "b > 10",
                    "columns_referenced": ["b"],
                },
            ),
        )

        optimised = push_filters_down(g)
        order = optimised.topological_order()
        filter_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.FILTER
        ]
        join_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.JOIN
        ]
        assert filter_nodes[0] < join_nodes[0]

    def test_cross_column_filter_not_pushed(self) -> None:
        """Filter referencing both sides of join stays above."""
        g = ComputationGraph()
        scan_l = g.add_node(OpNode(op_type=OpType.SCAN))
        scan_r = g.add_node(OpNode(op_type=OpType.SCAN))
        join = g.add_node(
            OpNode(
                op_type=OpType.JOIN,
                inputs=[scan_l, scan_r],
                params={
                    "left_columns": ["a"],
                    "right_columns": ["b"],
                },
            ),
        )
        filt = g.add_node(
            OpNode(
                op_type=OpType.FILTER,
                inputs=[join],
                params={
                    "predicate": "a > b",
                    "columns_referenced": ["a", "b"],
                },
            ),
        )

        optimised = push_filters_down(g)
        order = optimised.topological_order()
        filter_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.FILTER
        ]
        join_nodes = [
            nid for nid in order if optimised.nodes[nid].op_type == OpType.JOIN
        ]
        # Filter should still be AFTER join (not pushed down)
        assert filter_nodes[-1] > join_nodes[-1]


# ---------------------------------------------------------------------------
# Memory estimation
# ---------------------------------------------------------------------------


class TestMemoryEstimation:
    """Verify peak memory estimation."""

    def test_linear_pipeline(self) -> None:
        """Linear scan→filter→project has peak = max single op."""
        g = ComputationGraph()
        g.add_node(
            OpNode(op_type=OpType.SCAN, estimated_rows=10_000),
        )
        g.add_node(
            OpNode(op_type=OpType.FILTER, inputs=[0], estimated_rows=5_000),
        )
        g.add_node(
            OpNode(op_type=OpType.PROJECT, inputs=[1], estimated_rows=5_000),
        )

        peak = estimate_memory(g, avg_row_bytes=100)
        assert peak > 0
        # Peak should be at least the largest single operation
        assert peak >= 10_000 * 100

    def test_join_concurrent_memory(self) -> None:
        """Join has concurrent memory = left + right + output."""
        g = ComputationGraph()
        scan_l = g.add_node(
            OpNode(op_type=OpType.SCAN, estimated_rows=100_000),
        )
        scan_r = g.add_node(
            OpNode(op_type=OpType.SCAN, estimated_rows=50_000),
        )
        g.add_node(
            OpNode(
                op_type=OpType.JOIN,
                inputs=[scan_l, scan_r],
                estimated_rows=80_000,
            ),
        )

        peak = estimate_memory(g, avg_row_bytes=100)
        # Join peak should account for all three frames
        min_expected = (100_000 + 50_000 + 80_000) * 100
        assert peak >= min_expected

    def test_empty_graph(self) -> None:
        """Empty graph has zero memory."""
        g = ComputationGraph()
        peak = estimate_memory(g)
        assert peak == 0
