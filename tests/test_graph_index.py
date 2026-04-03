"""Tests for graph-native index structures (pycypher.graph_index).

Covers:
- AdjacencyIndex: build, outgoing/incoming lookups, batch lookups
- PropertyValueIndex: build, equality lookups, distinct values
- EntityLabelIndex: build, membership, count
- GraphIndexManager: lazy build, epoch invalidation, indexed scans
- Integration with RelationshipScan via Context.index_manager
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.graph_index import (
    AdjacencyIndex,
    EntityLabelIndex,
    PropertyValueIndex,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def relationship_df() -> pd.DataFrame:
    """A small relationship DataFrame for testing adjacency indexes."""
    return pd.DataFrame(
        {
            ID_COLUMN: ["r1", "r2", "r3", "r4", "r5"],
            RELATIONSHIP_SOURCE_COLUMN: ["a", "a", "b", "c", "c"],
            RELATIONSHIP_TARGET_COLUMN: ["b", "c", "c", "a", "d"],
        },
    )


@pytest.fixture
def entity_df() -> pd.DataFrame:
    """A small entity DataFrame for testing property/label indexes."""
    return pd.DataFrame(
        {
            ID_COLUMN: ["e1", "e2", "e3", "e4"],
            "name": ["Alice", "Bob", "Alice", "Charlie"],
            "age": [30, 25, 30, 35],
        },
    )


@pytest.fixture
def context_with_data():
    """A Context with entity and relationship data for integration tests."""
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
        RelationshipTable,
    )

    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4],
            "name": ["Alice", "Bob", "Charlie", "Diana"],
            "age": [30, 25, 35, 28],
        },
    )
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103, 104, 105],
            RELATIONSHIP_SOURCE_COLUMN: [1, 1, 2, 3, 4],
            RELATIONSHIP_TARGET_COLUMN: [2, 3, 3, 4, 1],
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(person_df.columns),
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=list(knows_df.columns),
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )

    ctx = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )
    return ctx


# ---------------------------------------------------------------------------
# AdjacencyIndex tests
# ---------------------------------------------------------------------------


class TestAdjacencyIndex:
    def test_build_basic(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        assert idx.rel_type == "KNOWS"
        assert idx.size == 5

    def test_outgoing_lookup(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        neighbors = idx.neighbors_outgoing("a")
        assert len(neighbors) == 2
        rel_ids = {n[0] for n in neighbors}
        assert rel_ids == {"r1", "r2"}

    def test_incoming_lookup(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        neighbors = idx.neighbors_incoming("c")
        assert len(neighbors) == 2
        src_ids = {n[1] for n in neighbors}
        assert src_ids == {"a", "b"}

    def test_nonexistent_node(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        assert len(idx.neighbors_outgoing("z")) == 0
        assert len(idx.neighbors_incoming("z")) == 0

    def test_outgoing_batch(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        rel_ids, src_ids, tgt_ids = idx.neighbors_outgoing_batch(
            np.array(["a", "b"]),
        )
        assert len(rel_ids) == 3  # a→b, a→c, b→c
        assert set(rel_ids) == {"r1", "r2", "r3"}

    def test_incoming_batch(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        rel_ids, src_ids, tgt_ids = idx.neighbors_incoming_batch(
            pd.Series(["c", "d"]),
        )
        # c←a(r2), c←b(r3), d←c(r5)
        assert len(rel_ids) == 3
        assert set(rel_ids) == {"r2", "r3", "r5"}

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(
            {
                ID_COLUMN: pd.Series(dtype=object),
                RELATIONSHIP_SOURCE_COLUMN: pd.Series(dtype=object),
                RELATIONSHIP_TARGET_COLUMN: pd.Series(dtype=object),
            },
        )
        idx = AdjacencyIndex.build("EMPTY", empty_df)
        assert idx.size == 0
        assert len(idx.neighbors_outgoing("x")) == 0

    def test_batch_with_series(self, relationship_df):
        idx = AdjacencyIndex.build("KNOWS", relationship_df)
        rel_ids, src_ids, tgt_ids = idx.neighbors_outgoing_batch(
            pd.Series(["c"]),
        )
        assert len(rel_ids) == 2  # c→a, c→d


# ---------------------------------------------------------------------------
# PropertyValueIndex tests
# ---------------------------------------------------------------------------


class TestPropertyValueIndex:
    def test_build_basic(self, entity_df):
        idx = PropertyValueIndex.build("Person", "name", entity_df)
        assert idx.entity_type == "Person"
        assert idx.property_name == "name"
        assert idx.size == 4

    def test_lookup_single_match(self, entity_df):
        idx = PropertyValueIndex.build("Person", "name", entity_df)
        result = idx.lookup("Bob")
        assert result == frozenset({"e2"})

    def test_lookup_multiple_matches(self, entity_df):
        idx = PropertyValueIndex.build("Person", "name", entity_df)
        result = idx.lookup("Alice")
        assert result == frozenset({"e1", "e3"})

    def test_lookup_no_match(self, entity_df):
        idx = PropertyValueIndex.build("Person", "name", entity_df)
        result = idx.lookup("NonExistent")
        assert result == frozenset()

    def test_distinct_values(self, entity_df):
        idx = PropertyValueIndex.build("Person", "name", entity_df)
        assert idx.distinct_values == 3  # Alice, Bob, Charlie

    def test_numeric_property(self, entity_df):
        idx = PropertyValueIndex.build("Person", "age", entity_df)
        result = idx.lookup(30)
        assert result == frozenset({"e1", "e3"})

    def test_missing_column(self, entity_df):
        idx = PropertyValueIndex.build("Person", "nonexistent", entity_df)
        assert idx.size == 0

    def test_null_values_excluded(self):
        df = pd.DataFrame(
            {
                ID_COLUMN: ["e1", "e2", "e3"],
                "name": ["Alice", None, "Bob"],
            },
        )
        idx = PropertyValueIndex.build("Person", "name", df)
        assert idx.distinct_values == 2
        assert idx.lookup(None) == frozenset()


# ---------------------------------------------------------------------------
# EntityLabelIndex tests
# ---------------------------------------------------------------------------


class TestEntityLabelIndex:
    def test_build_basic(self, entity_df):
        idx = EntityLabelIndex.build("Person", entity_df)
        assert idx.entity_type == "Person"
        assert idx.count() == 4

    def test_contains(self, entity_df):
        idx = EntityLabelIndex.build("Person", entity_df)
        assert idx.contains("e1") is True
        assert idx.contains("e4") is True
        assert idx.contains("nonexistent") is False

    def test_empty(self):
        empty_df = pd.DataFrame({ID_COLUMN: pd.Series(dtype=object)})
        idx = EntityLabelIndex.build("Empty", empty_df)
        assert idx.count() == 0
        assert idx.contains("x") is False


# ---------------------------------------------------------------------------
# GraphIndexManager tests
# ---------------------------------------------------------------------------


class TestGraphIndexManager:
    def test_lazy_adjacency_build(self, context_with_data):
        mgr = context_with_data.index_manager
        idx = mgr.get_adjacency_index("KNOWS")
        assert idx is not None
        assert idx.size == 5

    def test_lazy_caching(self, context_with_data):
        mgr = context_with_data.index_manager
        idx1 = mgr.get_adjacency_index("KNOWS")
        idx2 = mgr.get_adjacency_index("KNOWS")
        assert idx1 is idx2  # Same object — cached

    def test_nonexistent_type(self, context_with_data):
        mgr = context_with_data.index_manager
        assert mgr.get_adjacency_index("NONEXISTENT") is None

    def test_property_index(self, context_with_data):
        mgr = context_with_data.index_manager
        idx = mgr.get_property_index("Person", "name")
        assert idx is not None
        assert idx.lookup("Alice") == frozenset({1})

    def test_label_index(self, context_with_data):
        mgr = context_with_data.index_manager
        idx = mgr.get_label_index("Person")
        assert idx is not None
        assert idx.count() == 4
        assert idx.contains(1) is True
        assert idx.contains(99) is False

    def test_epoch_invalidation(self, context_with_data):
        mgr = context_with_data.index_manager
        idx1 = mgr.get_adjacency_index("KNOWS")
        assert idx1 is not None

        # Simulate a mutation commit
        context_with_data._data_epoch += 1

        idx2 = mgr.get_adjacency_index("KNOWS")
        assert idx2 is not idx1  # Rebuilt after epoch change

    def test_indexed_relationship_scan_outgoing(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_relationship_scan(
            "KNOWS",
            source_ids=pd.Series([1]),
        )
        assert result is not None
        assert len(result) == 2  # Person 1 → Person 2, Person 1 → Person 3
        assert set(result[RELATIONSHIP_TARGET_COLUMN]) == {2, 3}

    def test_indexed_relationship_scan_incoming(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_relationship_scan(
            "KNOWS",
            target_ids=pd.Series([3]),
        )
        assert result is not None
        assert len(result) == 2  # Person 1 → 3, Person 2 → 3
        assert set(result[RELATIONSHIP_SOURCE_COLUMN]) == {1, 2}

    def test_indexed_relationship_scan_both(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_relationship_scan(
            "KNOWS",
            source_ids=pd.Series([1]),
            target_ids=pd.Series([2]),
        )
        assert result is not None
        assert len(result) == 1  # Person 1 → Person 2 only

    def test_indexed_relationship_scan_no_pushdown(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_relationship_scan("KNOWS")
        assert result is None  # No benefit without pushdown

    def test_indexed_property_lookup(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_property_lookup("Person", "name", "Alice")
        assert result == frozenset({1})

    def test_indexed_property_lookup_no_match(self, context_with_data):
        mgr = context_with_data.index_manager
        result = mgr.indexed_property_lookup("Person", "name", "Unknown")
        assert result == frozenset()

    def test_stats(self, context_with_data):
        mgr = context_with_data.index_manager
        # Trigger index builds
        mgr.get_adjacency_index("KNOWS")
        mgr.get_property_index("Person", "name")
        mgr.get_label_index("Person")

        stats = mgr.stats()
        assert "adjacency_indexes" in stats
        assert "KNOWS" in stats["adjacency_indexes"]
        assert "property_indexes" in stats
        assert "Person.name" in stats["property_indexes"]
        assert "label_indexes" in stats
        assert "Person" in stats["label_indexes"]

    def test_invalidate(self, context_with_data):
        mgr = context_with_data.index_manager
        mgr.get_adjacency_index("KNOWS")
        assert len(mgr._adjacency) == 1

        mgr.invalidate()
        assert len(mgr._adjacency) == 0


# ---------------------------------------------------------------------------
# Integration: RelationshipScan uses index
# ---------------------------------------------------------------------------


class TestRelationshipScanIndexIntegration:
    """Verify that RelationshipScan.scan() uses the adjacency index path."""

    def test_scan_with_source_pushdown_uses_index(self, context_with_data):
        from pycypher.binding_frame import RelationshipScan

        rs = RelationshipScan("KNOWS", "r")
        frame = rs.scan(
            context_with_data,
            source_ids=pd.Series([1]),
        )
        # Person 1 has outgoing KNOWS to Person 2 and Person 3
        assert len(frame) == 2
        assert "r" in frame.var_names

    def test_scan_with_target_pushdown_uses_index(self, context_with_data):
        from pycypher.binding_frame import RelationshipScan

        rs = RelationshipScan("KNOWS", "r")
        frame = rs.scan(
            context_with_data,
            target_ids=pd.Series([1]),
        )
        # Person 4 → Person 1 (only incoming edge to Person 1)
        assert len(frame) == 1

    def test_scan_without_pushdown_skips_index(self, context_with_data):
        from pycypher.binding_frame import RelationshipScan

        rs = RelationshipScan("KNOWS", "r")
        frame = rs.scan(context_with_data)
        # Full scan — all 5 relationships
        assert len(frame) == 5

    def test_scan_results_consistent(self, context_with_data):
        """Index path should produce same results as table-scan path."""
        from pycypher.binding_frame import RelationshipScan

        rs = RelationshipScan("KNOWS", "r")

        # Index path
        indexed_frame = rs.scan(
            context_with_data,
            source_ids=pd.Series([1, 2]),
        )

        # Force table-scan by invalidating index
        context_with_data._index_manager = None
        context_with_data._data_epoch += 1
        rs2 = RelationshipScan("KNOWS", "r2")
        table_frame = rs2.scan(
            context_with_data,
            source_ids=pd.Series([1, 2]),
        )

        # Same number of results
        assert len(indexed_frame) == len(table_frame)

    def test_scan_during_shadow_falls_back(self, context_with_data):
        """During active mutations (shadow), scan falls back to table path."""
        from pycypher.binding_frame import RelationshipScan

        # Simulate active shadow mutation
        context_with_data._shadow_rels["KNOWS"] = pd.DataFrame()

        rs = RelationshipScan("KNOWS", "r")
        # Should not crash — falls back to table scan
        # (shadow is empty so it'll return 0 rows from the empty shadow,
        #  but the important thing is it doesn't use the stale index)
        frame = rs.scan(
            context_with_data,
            source_ids=pd.Series([1]),
        )
        # Verify it ran without error
        assert frame is not None


# ---------------------------------------------------------------------------
# IndexScanRule optimizer tests
# ---------------------------------------------------------------------------


class TestIndexScanRule:
    def test_detects_relationship_traversal(self):
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_optimizer import IndexScanRule

        rule = IndexScanRule()
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a"
        )
        result = rule.analyze(ast)
        assert result.applied is True
        assert result.hints["index_adjacency_candidates"] >= 1

    def test_detects_property_filter(self):
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_optimizer import IndexScanRule

        rule = IndexScanRule()
        ast = ASTConverter.from_cypher(
            "MATCH (a:Person {name: 'Alice'}) RETURN a"
        )
        result = rule.analyze(ast)
        assert result.applied is True
        assert result.hints["index_property_candidates"] >= 1

    def test_no_opportunities_for_simple_scan(self):
        from pycypher.ast_converter import ASTConverter
        from pycypher.query_optimizer import IndexScanRule

        rule = IndexScanRule()
        ast = ASTConverter.from_cypher("MATCH (a:Person) RETURN a")
        result = rule.analyze(ast)
        # Simple node scan has no relationship or property index opportunities
        assert result.hints.get("index_adjacency_candidates", 0) == 0
