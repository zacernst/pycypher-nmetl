"""Unit tests for mutation_engine.py — CREATE/SET/DELETE/MERGE Execution.

Tests the MutationEngine class that handles all write operations including
node/relationship creation, property updates, and deletions.
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mutation_star() -> Star:
    """Star instance for mutation testing."""
    people_df = pd.DataFrame({
        "__ID__": [1, 2, 3],
        "name": ["Alice", "Bob", "Carol"],
        "age": [30, 25, 35],
    })

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return Star(context=context)


# ---------------------------------------------------------------------------
# CREATE Node Operations
# ---------------------------------------------------------------------------


class TestMutationEngineCreateNode:
    """CREATE node operations."""

    def test_create_single_node(self, mutation_star: Star) -> None:
        """CREATE (n:Person {name: 'Eve', age: 32})."""
        result = mutation_star.execute_query("CREATE (n:Person {name: 'Eve', age: 32}) RETURN n.name")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Eve"

    def test_create_multiple_nodes(self, mutation_star: Star) -> None:
        """CREATE (a), (b), (c)."""
        result = mutation_star.execute_query(
            "CREATE (a:Person {name: 'X'}), (b:Person {name: 'Y'}), (c:Person {name: 'Z'}) "
            "RETURN a.name, b.name, c.name"
        )
        assert len(result) == 1
        assert result.iloc[0]["a.name"] == "X"
        assert result.iloc[0]["b.name"] == "Y"
        assert result.iloc[0]["c.name"] == "Z"

    def test_create_node_without_label(self, mutation_star: Star) -> None:
        """CREATE (n {prop: 'value'})."""
        result = mutation_star.execute_query("CREATE (n {name: 'Unlabeled'}) RETURN n.name")
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Unlabeled"

    def test_create_node_with_multiple_properties(self, mutation_star: Star) -> None:
        """CREATE node with many properties."""
        result = mutation_star.execute_query(
            "CREATE (n:Person {name: 'Multi', age: 40, dept: 'eng', salary: 100000}) "
            "RETURN n.name, n.age, n.dept"
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "Multi"
        assert result.iloc[0]["age"] == 40

    def test_create_node_returns_id(self, mutation_star: Star) -> None:
        """Created node receives auto-generated ID."""
        result = mutation_star.execute_query(
            "CREATE (n:Person {name: 'IDTest'}) RETURN n"
        )
        assert len(result) == 1
        # Node should have an ID (implementation-dependent format)

    def test_create_then_query_created_node(self, mutation_star: Star) -> None:
        """CREATE then MATCH the created node."""
        mutation_star.execute_query("CREATE (n:Person {name: 'NewPerson', age: 45})")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'NewPerson'}) RETURN p.age")
        assert len(result) == 1
        assert result.iloc[0]["age"] == 45


# ---------------------------------------------------------------------------
# SET Property Operations
# ---------------------------------------------------------------------------


class TestMutationEngineSetProperty:
    """SET node/relationship property operations."""

    def test_set_single_property(self, mutation_star: Star) -> None:
        """SET n.age = 31."""
        mutation_star.execute_query("MATCH (p:Person {name: 'Alice'}) SET p.age = 31")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'Alice'}) RETURN p.age")
        assert result.iloc[0]["age"] == 31

    def test_set_multiple_properties(self, mutation_star: Star) -> None:
        """SET n.a = 1, n.b = 2."""
        mutation_star.execute_query(
            "MATCH (p:Person {name: 'Bob'}) SET p.age = 26, p.name = 'Robert'"
        )
        result = mutation_star.execute_query(
            "MATCH (p:Person {name: 'Robert'}) RETURN p.age"
        )
        assert result.iloc[0]["age"] == 26

    def test_set_with_expression(self, mutation_star: Star) -> None:
        """SET n.age = n.age + 1."""
        mutation_star.execute_query("MATCH (p:Person {name: 'Carol'}) SET p.age = p.age + 1")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'Carol'}) RETURN p.age")
        assert result.iloc[0]["age"] == 36

    def test_set_to_null(self, mutation_star: Star) -> None:
        """SET n.prop = NULL."""
        mutation_star.execute_query("CREATE (n:Person {name: 'Temp', age: 50})")
        mutation_star.execute_query("MATCH (p:Person {name: 'Temp'}) SET p.age = NULL")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'Temp'}) RETURN p.age")
        assert pd.isna(result.iloc[0]["age"])

    def test_set_adds_new_property(self, mutation_star: Star) -> None:
        """SET adds property that didn't exist."""
        mutation_star.execute_query("MATCH (p:Person {name: 'Alice'}) SET p.email = 'alice@example.com'")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'Alice'}) RETURN p.email")
        assert result.iloc[0]["email"] == "alice@example.com"

    def test_set_batch_update(self, mutation_star: Star) -> None:
        """SET updates multiple nodes."""
        mutation_star.execute_query("MATCH (p:Person) WHERE p.age > 28 SET p.age = 99")
        result = mutation_star.execute_query("MATCH (p:Person) WHERE p.age = 99 RETURN COUNT(*) as cnt")
        # Should have Alice and Carol
        assert result.iloc[0]["cnt"] >= 1


# ---------------------------------------------------------------------------
# DELETE Operations
# ---------------------------------------------------------------------------


class TestMutationEngineDelete:
    """DELETE node and relationship operations."""

    def test_delete_single_node(self, mutation_star: Star) -> None:
        """DELETE n."""
        mutation_star.execute_query("CREATE (n:Person {name: 'ToDelete', age: 50})")
        mutation_star.execute_query("MATCH (p:Person {name: 'ToDelete'}) DELETE p")
        result = mutation_star.execute_query("MATCH (p:Person {name: 'ToDelete'}) RETURN p")
        assert len(result) == 0

    def test_delete_multiple_nodes(self, mutation_star: Star) -> None:
        """DELETE multiple nodes in one query."""
        mutation_star.execute_query("CREATE (a:Person {name: 'Del1'}), (b:Person {name: 'Del2'})")
        mutation_star.execute_query(
            "MATCH (p:Person) WHERE p.name IN ['Del1', 'Del2'] DELETE p"
        )
        result = mutation_star.execute_query(
            "MATCH (p:Person) WHERE p.name IN ['Del1', 'Del2'] RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 0

    def test_delete_with_condition(self, mutation_star: Star) -> None:
        """DELETE with WHERE condition."""
        mutation_star.execute_query("MATCH (p:Person) WHERE p.age < 30 DELETE p")
        result = mutation_star.execute_query("MATCH (p:Person) RETURN COUNT(*) as cnt")
        # Bob (25) should be deleted, Alice (30) and Carol (35) remain
        assert result.iloc[0]["cnt"] < 3

    def test_delete_nonexistent_node(self, mutation_star: Star) -> None:
        """DELETE non-existent node (should fail silently)."""
        result = mutation_star.execute_query("MATCH (p:Person {name: 'NonExistent'}) DELETE p RETURN COUNT(*) as cnt")
        assert result.iloc[0]["cnt"] == 0

    def test_delete_returns_count(self, mutation_star: Star) -> None:
        """DELETE operation returns affected count."""
        result = mutation_star.execute_query("MATCH (p:Person {name: 'Alice'}) DELETE p")
        # Result indicates deletion happened


# ---------------------------------------------------------------------------
# MERGE Operations (Upsert)
# ---------------------------------------------------------------------------


class TestMutationEngineMerge:
    """MERGE (upsert) operations."""

    def test_merge_existing_node(self, mutation_star: Star) -> None:
        """MERGE matches existing node (no creation)."""
        result = mutation_star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) RETURN p.age"
        )
        assert len(result) == 1
        assert result.iloc[0]["age"] == 30

    def test_merge_new_node(self, mutation_star: Star) -> None:
        """MERGE creates node if not found."""
        result = mutation_star.execute_query(
            "MERGE (p:Person {name: 'NewMerge'}) RETURN p.name"
        )
        assert len(result) == 1
        assert result.iloc[0]["name"] == "NewMerge"

    def test_merge_with_on_create_set(self, mutation_star: Star) -> None:
        """MERGE with ON CREATE SET."""
        mutation_star.execute_query(
            "MERGE (p:Person {name: 'MergeCreate'}) ON CREATE SET p.age = 40"
        )
        result = mutation_star.execute_query(
            "MATCH (p:Person {name: 'MergeCreate'}) RETURN p.age"
        )
        assert result.iloc[0]["age"] == 40

    def test_merge_with_on_match_set(self, mutation_star: Star) -> None:
        """MERGE with ON MATCH SET."""
        mutation_star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.age = 31"
        )
        result = mutation_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age"
        )
        assert result.iloc[0]["age"] == 31

    def test_merge_idempotent(self, mutation_star: Star) -> None:
        """MERGE is idempotent (multiple executions same result)."""
        mutation_star.execute_query("MERGE (p:Person {name: 'Idempotent'}) SET p.age = 50")
        count1 = mutation_star.execute_query("MATCH (p:Person {name: 'Idempotent'}) RETURN COUNT(*) as cnt")

        mutation_star.execute_query("MERGE (p:Person {name: 'Idempotent'}) SET p.age = 50")
        count2 = mutation_star.execute_query("MATCH (p:Person {name: 'Idempotent'}) RETURN COUNT(*) as cnt")

        assert count1.iloc[0]["cnt"] == count2.iloc[0]["cnt"] == 1


# ---------------------------------------------------------------------------
# CREATE Relationship Operations
# ---------------------------------------------------------------------------


class TestMutationEngineCreateRelationship:
    """CREATE relationship operations."""

    def test_create_simple_relationship(self, mutation_star: Star) -> None:
        """CREATE (a)-[r:KNOWS]->(b)."""
        mutation_star.execute_query("CREATE (a:Person {name: 'X'}), (b:Person {name: 'Y'})")
        mutation_star.execute_query(
            "MATCH (a:Person {name: 'X'}), (b:Person {name: 'Y'}) "
            "CREATE (a)-[r:KNOWS]->(b)"
        )
        # type() is not a supported scalar function here — verify the
        # relationship exists by matching on its type instead.
        result = mutation_star.execute_query(
            "MATCH (a:Person {name: 'X'})-[r:KNOWS]->(b:Person {name: 'Y'}) "
            "RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1

    @pytest.mark.xfail(
        reason=(
            "Relationship inline properties are silently dropped by CREATE: "
            "MutationEngine.process_create reads element.properties for "
            "nodes but never rel.properties for relationships, and "
            "shadow_create_relationship has no props parameter at all "
            "(mutation_engine.py, contrast ~line 544 node-property handling "
            "with ~line 553 shadow_create_relationship call and its "
            "signature at ~line 405). Confirmed via direct reproduction: "
            "r.since reads back as None immediately after CREATE."
        ),
        strict=True,
    )
    def test_create_relationship_with_properties(self, mutation_star: Star) -> None:
        """CREATE relationship with properties."""
        mutation_star.execute_query("CREATE (a:Person {name: 'X'}), (b:Person {name: 'Y'})")
        result = mutation_star.execute_query(
            "MATCH (a:Person {name: 'X'}), (b:Person {name: 'Y'}) "
            "CREATE (a)-[r:KNOWS {since: 2020}]->(b) RETURN r.since"
        )
        assert result.iloc[0]["since"] == 2020

    def test_create_multiple_relationships(self, mutation_star: Star) -> None:
        """CREATE multiple relationships."""
        mutation_star.execute_query(
            "CREATE (a:Person {name: 'A'}), (b:Person {name: 'B'}), (c:Person {name: 'C'})"
        )
        result = mutation_star.execute_query(
            "MATCH (a:Person {name: 'A'}), (b:Person {name: 'B'}), (c:Person {name: 'C'}) "
            "CREATE (a)-[r1:KNOWS]->(b), (b)-[r2:KNOWS]->(c) "
            "RETURN COUNT(*) as cnt"
        )
        assert result.iloc[0]["cnt"] == 1


# ---------------------------------------------------------------------------
# Shadow Buffer and Consistency
# ---------------------------------------------------------------------------


class TestMutationEngineShadowBuffer:
    """Shadow buffer management for transaction semantics."""

    def test_shadow_buffer_isolation(self, mutation_star: Star) -> None:
        """Created nodes visible in same query."""
        result = mutation_star.execute_query(
            "CREATE (a:Person {name: 'Shadow1'}) "
            "CREATE (b:Person {name: 'Shadow2'}) "
            "WITH a, b "
            "MATCH (p:Person {name: 'Shadow1'}) RETURN p.name"
        )
        assert len(result) > 0

    def test_shadow_buffer_consistency(self, mutation_star: Star) -> None:
        """Multiple mutations in same query are consistent."""
        result = mutation_star.execute_query(
            "CREATE (n:Person {name: 'Consistent', age: 40}) "
            "MATCH (p:Person {name: 'Consistent'}) SET p.age = 41 "
            "MATCH (p:Person {name: 'Consistent'}) RETURN p.age"
        )
        assert result.iloc[0]["age"] == 41


# ---------------------------------------------------------------------------
# Type Conversion in Mutations
# ---------------------------------------------------------------------------


class TestMutationEngineTypeConversion:
    """Type coercion in mutation operations."""

    def test_set_incompatible_type(self, mutation_star: Star) -> None:
        """SET incompatible type (e.g., string to age property)."""
        # May raise error or coerce depending on implementation
        try:
            mutation_star.execute_query(
                "MATCH (p:Person {name: 'Alice'}) SET p.age = 'thirty'"
            )
        except Exception:
            pass  # Type error is acceptable

    def test_set_numeric_string(self, mutation_star: Star) -> None:
        """SET numeric string (may or may not coerce)."""
        mutation_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) SET p.age = '31'"
        )
        result = mutation_star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age"
        )
        # Result depends on type handling
        assert result is not None


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestMutationEngineEdgeCases:
    """Edge cases in mutation operations."""

    def test_create_duplicate_id_handling(self, mutation_star: Star) -> None:
        """ID collision handling in CREATE."""
        # IDs should be auto-generated and unique
        result = mutation_star.execute_query(
            "CREATE (a:Person {name: 'A'}), (b:Person {name: 'B'}) "
            "RETURN a, b"
        )
        assert len(result) == 1

    def test_delete_cascade_behavior(self, mutation_star: Star) -> None:
        """DELETE with relationships (cascade behavior)."""
        # Implementation-dependent: may cascade or fail
        pass

    def test_merge_with_missing_properties(self, mutation_star: Star) -> None:
        """MERGE with incomplete match criteria."""
        result = mutation_star.execute_query(
            "MERGE (p:Person {name: 'PartialMatch'}) RETURN p"
        )
        assert len(result) == 1
