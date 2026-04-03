"""Tests for predicate pushdown into EntityScan.

Covers:
- EntityScan.scan() with property_filters parameter
- Integration with PropertyValueIndex for O(1) equality lookups
- Pattern matching with inline property filters {prop: val}
- Multi-predicate intersection pushdown
- Shadow mutation bypass (pushdown disabled during mutations)
- Fallback to full scan when index unavailable
- End-to-end Cypher queries with predicate pushdown
- Performance: pushdown vs post-scan filter
"""

from __future__ import annotations

import time

import pandas as pd
import pytest
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def small_context():
    """A small Context for basic pushdown tests."""
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )

    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Alice", "Eve"],
            "age": [30, 25, 35, 30, 42],
            "city": ["NYC", "LA", "NYC", "Chicago", "LA"],
        },
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=list(person_df.columns),
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "city": "city",
        },
        attribute_map={"name": "name", "age": "age", "city": "city"},
        source_obj=person_df,
    )

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def graph_context():
    """A Context with entities and relationships for end-to-end tests."""
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

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )


@pytest.fixture
def large_context():
    """A large Context for performance testing."""
    from pycypher.relational_models import (
        Context,
        EntityMapping,
        EntityTable,
        RelationshipMapping,
    )

    n = 50_000
    person_df = pd.DataFrame(
        {
            ID_COLUMN: list(range(n)),
            "name": [f"Person{i}" for i in range(n)],
            "age": [20 + (i % 60) for i in range(n)],
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

    return Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ---------------------------------------------------------------------------
# EntityScan with property_filters
# ---------------------------------------------------------------------------


class TestEntityScanPushdown:
    def test_no_filters_returns_all(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(small_context)
        assert len(frame) == 5

    def test_single_equality_filter(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice"},
        )
        assert len(frame) == 2
        assert set(frame.bindings["a"]) == {1, 4}

    def test_numeric_equality_filter(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"age": 30},
        )
        assert len(frame) == 2
        assert set(frame.bindings["a"]) == {1, 4}

    def test_multi_predicate_intersection(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice", "age": 30},
        )
        assert len(frame) == 2  # Both Alices have age 30
        assert set(frame.bindings["a"]) == {1, 4}

    def test_multi_predicate_narrow(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice", "city": "NYC"},
        )
        assert len(frame) == 1
        assert set(frame.bindings["a"]) == {1}

    def test_no_match_returns_empty(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Nonexistent"},
        )
        assert len(frame) == 0

    def test_empty_filters_dict_returns_all(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={},
        )
        assert len(frame) == 5

    def test_type_registry_preserved(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Bob"},
        )
        assert frame.type_registry == {"a": "Person"}

    def test_properties_accessible_after_pushdown(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Bob"},
        )
        ages = frame.get_property("a", "age")
        assert list(ages) == [25]


# ---------------------------------------------------------------------------
# Shadow mutation bypass
# ---------------------------------------------------------------------------


class TestShadowBypassPushdown:
    def test_shadow_disables_pushdown(self, small_context):
        from pycypher.binding_frame import EntityScan

        # First scan without shadow — verifies pushdown works
        pushed = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice"},
        )
        assert len(pushed) == 2  # Pushdown active → only Alices

        # Now activate shadow — pushdown should be disabled
        shadow_df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3, 4, 5],
                "name": ["A", "B", "C", "D", "E"],
                "age": [1, 2, 3, 4, 5],
                "city": ["X", "Y", "Z", "X", "Y"],
            },
        )
        small_context._shadow = {"Person": shadow_df}

        # With shadow, pushdown disabled → falls back to full scan
        # (EntityScan reads IDs from cache, not shadow)
        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice"},
        )
        # Full scan returns all 5 entities (pushdown disabled, no filter applied)
        assert len(frame) == 5


# ---------------------------------------------------------------------------
# End-to-end Cypher integration
# ---------------------------------------------------------------------------


class TestCypherPushdownIntegration:
    def test_inline_property_filter(self, graph_context):
        from pycypher.star import Star

        star = Star(graph_context)
        result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'}) RETURN a.name, a.age",
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"
        assert result.iloc[0]["age"] == 30

    def test_inline_property_with_relationship(self, graph_context):
        from pycypher.star import Star

        star = Star(graph_context)
        result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) RETURN b.name",
        )
        assert len(result) == 2
        assert set(result["name"]) == {"Bob", "Charlie"}

    def test_inline_property_no_match(self, graph_context):
        from pycypher.star import Star

        star = Star(graph_context)
        result = star.execute_query(
            "MATCH (a:Person {name: 'Nonexistent'}) RETURN a.name",
        )
        assert len(result) == 0

    def test_where_and_inline_same_result(self, graph_context):
        """WHERE clause should produce same results as inline filter."""
        from pycypher.star import Star

        star = Star(graph_context)
        inline_result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'}) RETURN a.name, a.age",
        )
        where_result = star.execute_query(
            "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a.name, a.age",
        )
        assert len(inline_result) == len(where_result)
        assert set(inline_result["name"]) == set(where_result["name"])

    def test_inline_numeric_property(self, graph_context):
        from pycypher.star import Star

        star = Star(graph_context)
        result = star.execute_query(
            "MATCH (a:Person {age: 30}) RETURN a.name",
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Alice"

    def test_inline_with_traversal_and_target_filter(self, graph_context):
        """Both source and target have inline property filters."""
        from pycypher.star import Star

        star = Star(graph_context)
        result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'}) "
            "RETURN a.name, b.name",
        )
        assert len(result) == 1
        assert result.iloc[0]["a.name"] == "Alice"
        assert result.iloc[0]["b.name"] == "Bob"


# ---------------------------------------------------------------------------
# Performance: pushdown vs post-scan
# ---------------------------------------------------------------------------


class TestPushdownPerformance:
    def test_pushdown_reduces_scan_size(self, large_context):
        from pycypher.binding_frame import EntityScan

        full = EntityScan("Person", "a").scan(large_context)
        assert len(full) == 50_000

        pushed = EntityScan("Person", "a").scan(
            large_context,
            property_filters={"age": 30},
        )
        assert len(pushed) < len(full)
        expected = len([i for i in range(50_000) if 20 + (i % 60) == 30])
        assert len(pushed) == expected

    def test_pushdown_faster_than_full_scan_filter(self, large_context):
        from pycypher.ast_models import (
            Comparison,
            IntegerLiteral,
            PropertyLookup,
            Variable,
        )
        from pycypher.binding_frame import BindingFilter, EntityScan

        # Warm up index
        EntityScan("Person", "a").scan(
            large_context,
            property_filters={"age": 30},
        )

        # Time pushdown path
        t0 = time.perf_counter()
        for _ in range(20):
            EntityScan("Person", "a").scan(
                large_context,
                property_filters={"age": 30},
            )
        pushdown_time = time.perf_counter() - t0

        # Time full scan + post-filter
        predicate = Comparison(
            operator="=",
            left=PropertyLookup(
                expression=Variable(name="a"),
                property="age",
            ),
            right=IntegerLiteral(value=30),
        )
        t0 = time.perf_counter()
        for _ in range(20):
            frame = EntityScan("Person", "a").scan(large_context)
            BindingFilter(predicate=predicate).apply(frame)
        filter_time = time.perf_counter() - t0

        assert pushdown_time < filter_time, (
            f"Pushdown ({pushdown_time:.4f}s) not faster than "
            f"full scan + filter ({filter_time:.4f}s)"
        )

    def test_index_cached_across_scans(self, large_context):
        from pycypher.binding_frame import EntityScan

        EntityScan("Person", "a").scan(
            large_context,
            property_filters={"age": 30},
        )
        mgr = large_context.index_manager
        idx1 = mgr.get_property_index("Person", "age")

        EntityScan("Person", "a").scan(
            large_context,
            property_filters={"age": 25},
        )
        idx2 = mgr.get_property_index("Person", "age")
        assert idx1 is idx2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestPushdownEdgeCases:
    def test_null_filter_value(self, small_context):
        from pycypher.binding_frame import EntityScan

        frame = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": None},
        )
        assert len(frame) == 0

    def test_multiple_entity_types(self):
        from pycypher.binding_frame import EntityScan
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            EntityTable,
            RelationshipMapping,
        )

        person_df = pd.DataFrame(
            {ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]},
        )
        animal_df = pd.DataFrame(
            {ID_COLUMN: [10, 20], "name": ["Rex", "Spot"]},
        )

        person_table = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(person_df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=person_df,
        )
        animal_table = EntityTable(
            entity_type="Animal",
            identifier="Animal",
            column_names=list(animal_df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=animal_df,
        )

        ctx = Context(
            entity_mapping=EntityMapping(
                mapping={"Person": person_table, "Animal": animal_table},
            ),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        person_frame = EntityScan("Person", "p").scan(
            ctx,
            property_filters={"name": "Alice"},
        )
        assert len(person_frame) == 1
        assert set(person_frame.bindings["p"]) == {1}

        animal_frame = EntityScan("Animal", "a").scan(
            ctx,
            property_filters={"name": "Rex"},
        )
        assert len(animal_frame) == 1
        assert set(animal_frame.bindings["a"]) == {10}

    def test_pushdown_result_consistent_with_full_scan(self, small_context):
        """Pushdown results should contain same IDs as full scan + filter."""
        from pycypher.binding_frame import EntityScan

        full = EntityScan("Person", "a").scan(small_context)
        pushed = EntityScan("Person", "a").scan(
            small_context,
            property_filters={"name": "Alice"},
        )

        # Get IDs from full scan that have name=Alice
        full_names = full.get_property("a", "name")
        expected_ids = set(full.bindings["a"][full_names == "Alice"])
        actual_ids = set(pushed.bindings["a"])
        assert actual_ids == expected_ids
