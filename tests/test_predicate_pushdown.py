"""Tests for RelationshipScan predicate pushdown.

Verifies that:
1. Predicate pushdown produces correct results (same as full scan + filter)
2. Filtered scans materialise fewer rows than unfiltered scans
3. Integration with star.py pattern traversal works correctly
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.binding_frame import RelationshipScan
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


@pytest.fixture()
def large_context() -> Context:
    """Context with enough data to demonstrate pushdown benefit."""
    n_entities = 1000
    n_rels = 5000

    import numpy as np

    rng = np.random.default_rng(42)

    entity_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_entities + 1)),
            "name": [f"node_{i}" for i in range(1, n_entities + 1)],
            "group": [f"g{rng.integers(0, 10)}" for _ in range(n_entities)],
        },
    )
    rel_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(1, n_rels + 1)),
            "__SOURCE__": rng.integers(
                1, n_entities + 1, size=n_rels
            ).tolist(),
            "__TARGET__": rng.integers(
                1, n_entities + 1, size=n_rels
            ).tolist(),
        },
    )

    entity_table = EntityTable(
        entity_type="Node",
        identifier="Node",
        column_names=list(entity_df.columns),
        source_obj_attribute_map={"name": "name", "group": "group"},
        attribute_map={"name": "name", "group": "group"},
        source_obj=entity_df,
    )
    rel_table = RelationshipTable(
        relationship_type="EDGE",
        identifier="EDGE",
        column_names=list(rel_df.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=rel_df,
        source_entity_type="Node",
        target_entity_type="Node",
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Node": entity_table}),
        relationship_mapping=RelationshipMapping(mapping={"EDGE": rel_table}),
    )


@pytest.fixture()
def large_star(large_context: Context) -> Star:
    """Star for large context."""
    return Star(context=large_context)


# ---------------------------------------------------------------------------
# Unit tests: RelationshipScan with predicate pushdown
# ---------------------------------------------------------------------------


class TestRelationshipScanPushdown:
    """Verify RelationshipScan predicate pushdown correctness."""

    def test_no_pushdown_returns_all(self, large_context: Context) -> None:
        """Without pushdown, scan returns all relationships."""
        rs = RelationshipScan("EDGE", "r")
        frame = rs.scan(large_context)
        assert len(frame) == 5000

    def test_source_pushdown_filters(self, large_context: Context) -> None:
        """Source ID pushdown reduces returned rows."""
        rs = RelationshipScan("EDGE", "r")
        # Only get relationships from entities 1-10
        source_ids = pd.Series(list(range(1, 11)))
        frame = rs.scan(large_context, source_ids=source_ids)
        assert len(frame) < 5000
        assert len(frame) > 0
        # Verify all returned sources are in the pushdown set
        src_col = rs.src_col
        actual_sources = set(frame.bindings[src_col].unique())
        assert actual_sources.issubset(set(range(1, 11)))

    def test_target_pushdown_filters(self, large_context: Context) -> None:
        """Target ID pushdown reduces returned rows."""
        rs = RelationshipScan("EDGE", "r")
        target_ids = pd.Series(list(range(1, 11)))
        frame = rs.scan(large_context, target_ids=target_ids)
        assert len(frame) < 5000
        assert len(frame) > 0
        tgt_col = rs.tgt_col
        actual_targets = set(frame.bindings[tgt_col].unique())
        assert actual_targets.issubset(set(range(1, 11)))

    def test_both_pushdown_filters(self, large_context: Context) -> None:
        """Both source and target pushdown compounds filtering."""
        rs = RelationshipScan("EDGE", "r")
        source_ids = pd.Series(list(range(1, 11)))
        target_ids = pd.Series(list(range(1, 11)))
        frame_both = rs.scan(
            large_context,
            source_ids=source_ids,
            target_ids=target_ids,
        )
        frame_source_only = rs.scan(
            large_context,
            source_ids=source_ids,
        )
        # Both filters should be at least as restrictive as source-only
        assert len(frame_both) <= len(frame_source_only)

    def test_pushdown_correctness(self, large_context: Context) -> None:
        """Pushdown results match full scan + manual filter."""
        rs = RelationshipScan("EDGE", "r")
        source_ids = pd.Series(list(range(1, 51)))

        # Method 1: pushdown
        pushed = rs.scan(large_context, source_ids=source_ids)

        # Method 2: full scan then filter
        full = rs.scan(large_context)
        mask = full.bindings[rs.src_col].isin(set(range(1, 51)))
        manual = full.bindings[mask].reset_index(drop=True)

        # Same rows (order may differ)
        pushed_sorted = pushed.bindings.sort_values(
            by=list(pushed.bindings.columns),
        ).reset_index(drop=True)
        manual_sorted = manual.sort_values(
            by=list(manual.columns),
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(pushed_sorted, manual_sorted)

    def test_pushdown_empty_ids(self, large_context: Context) -> None:
        """Empty pushdown ID set returns empty frame."""
        rs = RelationshipScan("EDGE", "r")
        source_ids = pd.Series([], dtype=object)
        frame = rs.scan(large_context, source_ids=source_ids)
        assert len(frame) == 0


# ---------------------------------------------------------------------------
# Integration: predicate pushdown through star.py pattern traversal
# ---------------------------------------------------------------------------


class TestPushdownIntegration:
    """Verify pushdown works correctly through full query execution."""

    def test_single_hop_still_correct(self, large_star: Star) -> None:
        """Single-hop traversal with pushdown returns correct results."""
        result = large_star.execute_query(
            "MATCH (a:Node)-[:EDGE]->(b:Node) RETURN a.name, b.name",
        )
        assert len(result) > 0
        assert "a.name" in result.columns
        assert "b.name" in result.columns

    def test_two_hop_still_correct(self, large_star: Star) -> None:
        """Two-hop traversal with pushdown returns correct results."""
        result = large_star.execute_query(
            "MATCH (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(c:Node) RETURN a.name, c.name",
        )
        assert len(result) > 0

    def test_filtered_query_correct(self, large_star: Star) -> None:
        """Query with WHERE clause after pushdown is correct."""
        result = large_star.execute_query(
            "MATCH (a:Node)-[:EDGE]->(b:Node) "
            "WHERE a.name = 'node_1' "
            "RETURN a.name, b.name",
        )
        assert len(result) > 0
        assert all(result["a.name"] == "node_1")

    def test_cyclic_pattern_correct(self, large_star: Star) -> None:
        """Cyclic pattern (a)->(b)->(a) with pushdown."""
        result = large_star.execute_query(
            "MATCH (a:Node)-[:EDGE]->(b:Node)-[:EDGE]->(a) RETURN a.name, b.name",
        )
        # May be empty if no cycles in random data, but should not error
        assert isinstance(result, pd.DataFrame)
