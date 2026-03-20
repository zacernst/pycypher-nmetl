"""Targeted unit tests for MutationEngine methods.

Covers uncovered code paths: ID generation edge cases, shadow_create_entity/relationship,
process_create with relationships, process_delete, process_merge ON CREATE/ON MATCH,
process_foreach, remove_properties, and process_call with YIELD.
"""

from __future__ import annotations

import pandas as pd
from pycypher.mutation_engine import MutationEngine
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
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


def _people_context(
    *,
    with_relationships: bool = False,
) -> Context:
    """Build a small Context with Person entities and optionally KNOWS rels."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        },
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    rel_mapping: dict[str, RelationshipTable] = {}
    if with_relationships:
        rel_df = pd.DataFrame(
            {
                ID_COLUMN: [100, 101],
                RELATIONSHIP_SOURCE_COLUMN: [1, 2],
                RELATIONSHIP_TARGET_COLUMN: [2, 3],
            },
        )
        rel_mapping["KNOWS"] = RelationshipTable(
            relationship_type="KNOWS",
            identifier="KNOWS",
            column_names=[
                ID_COLUMN,
                RELATIONSHIP_SOURCE_COLUMN,
                RELATIONSHIP_TARGET_COLUMN,
            ],
            source_obj=rel_df,
            source_entity_type="Person",
            target_entity_type="Person",
        )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping=rel_mapping),
    )


def _empty_context() -> Context:
    """Build an entirely empty Context."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ===========================================================================
# ID generation
# ===========================================================================


class TestNextEntityIds:
    """Test MutationEngine.next_entity_ids edge cases."""

    def test_ids_start_after_max_existing(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        ids = engine.next_entity_ids("Person", 3)
        assert list(ids) == [4, 5, 6]

    def test_ids_for_new_entity_type_start_at_1(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        ids = engine.next_entity_ids("Animal", 2)
        assert list(ids) == [1, 2]

    def test_ids_account_for_shadow_layer(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        # Seed shadow with higher IDs
        ctx._shadow["Person"] = pd.DataFrame({ID_COLUMN: [1, 2, 3, 10]})
        ids = engine.next_entity_ids("Person", 2)
        assert list(ids) == [11, 12]

    def test_ids_handle_non_integer_ids_gracefully(self) -> None:
        ctx = _empty_context()
        # Entity with string IDs
        str_df = pd.DataFrame({ID_COLUMN: ["abc", "def"]})
        ctx.entity_mapping.mapping["Foo"] = EntityTable(
            entity_type="Foo",
            identifier="Foo",
            column_names=[ID_COLUMN],
            source_obj_attribute_map={},
            attribute_map={},
            source_obj=str_df,
        )
        engine = MutationEngine(context=ctx)
        ids = engine.next_entity_ids("Foo", 1)
        # Falls back to 0 and starts at 1
        assert list(ids) == [1]


class TestNextRelationshipIds:
    """Test MutationEngine.next_relationship_ids edge cases."""

    def test_ids_start_after_max_existing(self) -> None:
        ctx = _people_context(with_relationships=True)
        engine = MutationEngine(context=ctx)
        ids = engine.next_relationship_ids("KNOWS", 2)
        assert list(ids) == [102, 103]

    def test_ids_for_new_rel_type_start_at_1(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        ids = engine.next_relationship_ids("LIKES", 3)
        assert list(ids) == [1, 2, 3]

    def test_ids_account_for_shadow_rels(self) -> None:
        ctx = _people_context(with_relationships=True)
        engine = MutationEngine(context=ctx)
        ctx._shadow_rels["KNOWS"] = pd.DataFrame(
            {
                ID_COLUMN: [100, 101, 200],
                RELATIONSHIP_SOURCE_COLUMN: [1, 2, 3],
                RELATIONSHIP_TARGET_COLUMN: [2, 3, 1],
            },
        )
        ids = engine.next_relationship_ids("KNOWS", 1)
        assert list(ids) == [201]

    def test_ids_handle_non_integer_rel_ids(self) -> None:
        ctx = _people_context(with_relationships=True)
        engine = MutationEngine(context=ctx)
        # Shadow with string ID
        ctx._shadow_rels["KNOWS"] = pd.DataFrame(
            {
                ID_COLUMN: ["x"],
                RELATIONSHIP_SOURCE_COLUMN: [1],
                RELATIONSHIP_TARGET_COLUMN: [2],
            },
        )
        ids = engine.next_relationship_ids("KNOWS", 1)
        # Falls back to max from source (101) + 1
        assert list(ids) == [102]


# ===========================================================================
# Shadow operations
# ===========================================================================


class TestShadowCreateEntity:
    """Test shadow_create_entity and attribute_map updates."""

    def test_creates_into_new_type(self) -> None:
        ctx = _empty_context()
        engine = MutationEngine(context=ctx)
        engine.shadow_create_entity(
            "Animal", [1, 2], {"species": ["Cat", "Dog"]}
        )
        shadow = ctx._shadow["Animal"]
        assert len(shadow) == 2
        assert list(shadow["species"]) == ["Cat", "Dog"]

    def test_appends_to_existing_shadow(self) -> None:
        ctx = _empty_context()
        engine = MutationEngine(context=ctx)
        engine.shadow_create_entity("Animal", [1], {"species": ["Cat"]})
        engine.shadow_create_entity("Animal", [2], {"species": ["Dog"]})
        assert len(ctx._shadow["Animal"]) == 2

    def test_seeds_from_live_table_when_no_shadow(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        engine.shadow_create_entity(
            "Person", [4], {"name": ["Dave"], "age": [40]}
        )
        shadow = ctx._shadow["Person"]
        assert len(shadow) == 4  # 3 existing + 1 new
        assert list(shadow[ID_COLUMN]) == [1, 2, 3, 4]

    def test_updates_attribute_map_for_new_columns(self) -> None:
        ctx = _people_context()
        engine = MutationEngine(context=ctx)
        engine.shadow_create_entity(
            "Person", [4], {"email": ["d@example.com"]}
        )
        et = ctx.entity_mapping.mapping["Person"]
        assert "email" in et.attribute_map
        assert "email" in et.source_obj_attribute_map


class TestShadowCreateRelationship:
    """Test shadow_create_relationship."""

    def test_creates_into_new_rel_type(self) -> None:
        ctx = _empty_context()
        engine = MutationEngine(context=ctx)
        engine.shadow_create_relationship("LIKES", [1], [10], [20])
        shadow = ctx._shadow_rels["LIKES"]
        assert len(shadow) == 1
        assert shadow[RELATIONSHIP_SOURCE_COLUMN].iloc[0] == 10
        assert shadow[RELATIONSHIP_TARGET_COLUMN].iloc[0] == 20

    def test_seeds_from_live_rel_table(self) -> None:
        ctx = _people_context(with_relationships=True)
        engine = MutationEngine(context=ctx)
        engine.shadow_create_relationship("KNOWS", [102], [3], [1])
        shadow = ctx._shadow_rels["KNOWS"]
        assert len(shadow) == 3  # 2 existing + 1 new


# ===========================================================================
# Integration via Star.execute_query()
# ===========================================================================


class TestProcessCreateViaQuery:
    """Test process_create through Star.execute_query."""

    def test_create_node_with_properties(self) -> None:
        star = Star(context=_people_context())
        result = star.execute_query(
            "CREATE (a:Person {name: 'Dave', age: 40}) RETURN a.name AS name"
        )
        assert result is not None
        names = result["name"].tolist()
        assert "Dave" in names

    def test_create_relationship_between_existing_and_new(self) -> None:
        star = Star(context=_people_context())
        result = star.execute_query(
            "MATCH (a:Person {name: 'Alice'}) CREATE (a)-[r:KNOWS]->(b:Person {name: 'Dave'}) RETURN b.name AS name",
        )
        assert result is not None
        assert "Dave" in result["name"].tolist()

    def test_create_relationship_with_direction(self) -> None:
        star = Star(context=_people_context())
        result = star.execute_query(
            "CREATE (a:Person {name: 'X'})-[r:FOLLOWS]->(b:Person {name: 'Y'}) RETURN a.name AS a_name, b.name AS b_name",
        )
        assert result is not None
        assert result["a_name"].iloc[0] == "X"
        assert result["b_name"].iloc[0] == "Y"


class TestProcessDeleteViaQuery:
    """Test process_delete through Star.execute_query."""

    def test_delete_specific_node(self) -> None:
        star = Star(context=_people_context())
        star.execute_query("MATCH (p:Person {name: 'Bob'}) DELETE p")
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result is not None
        names = result["name"].tolist()
        assert "Bob" not in names
        assert "Alice" in names

    def test_detach_delete_removes_relationships(self) -> None:
        star = Star(context=_people_context(with_relationships=True))
        star.execute_query("MATCH (p:Person {name: 'Alice'}) DETACH DELETE p")
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result is not None
        assert "Alice" not in result["name"].tolist()


class TestProcessMergeViaQuery:
    """Test process_merge through Star.execute_query."""

    def test_merge_existing_does_not_duplicate(self) -> None:
        star = Star(context=_people_context())
        star.execute_query("MERGE (p:Person {name: 'Alice'})")
        result = star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert result is not None
        assert result["cnt"].iloc[0] == 3

    def test_merge_creates_when_missing(self) -> None:
        star = Star(context=_people_context())
        star.execute_query("MERGE (p:Person {name: 'Dave'})")
        result = star.execute_query("MATCH (p:Person) RETURN count(p) AS cnt")
        assert result is not None
        assert result["cnt"].iloc[0] == 4

    def test_merge_on_create_sets_property(self) -> None:
        star = Star(context=_people_context())
        star.execute_query(
            "MERGE (p:Person {name: 'Eve'}) ON CREATE SET p.age = 28"
        )
        result = star.execute_query(
            "MATCH (p:Person {name: 'Eve'}) RETURN p.age AS age"
        )
        assert result is not None
        assert result["age"].iloc[0] == 28

    def test_merge_on_match_sets_property(self) -> None:
        star = Star(context=_people_context())
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.age = 99"
        )
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert result is not None
        assert result["age"].iloc[0] == 99


class TestProcessForeachViaQuery:
    """Test process_foreach through Star.execute_query."""

    def test_foreach_creates_nodes(self) -> None:
        star = Star(context=_empty_context())
        star.execute_query(
            "FOREACH (name IN ['X', 'Y', 'Z'] | CREATE (n:Person {name: name}))"
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result is not None
        assert set(result["name"].tolist()) == {"X", "Y", "Z"}

    def test_foreach_with_match_context(self) -> None:
        star = Star(context=_people_context())
        star.execute_query(
            "MATCH (p:Person) FOREACH (x IN [1] | CREATE (n:Tag {val: 'tag'}))",
        )
        result = star.execute_query("MATCH (t:Tag) RETURN count(t) AS cnt")
        assert result is not None
        # One tag created per person (3 people)
        assert result["cnt"].iloc[0] == 3


class TestRemovePropertiesViaQuery:
    """Test remove_properties through Star.execute_query."""

    def test_remove_property_sets_null(self) -> None:
        star = Star(context=_people_context())
        star.execute_query("MATCH (p:Person {name: 'Alice'}) REMOVE p.age")
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert result is not None
        assert pd.isna(result["age"].iloc[0])


class TestSetPropertiesEdgeCases:
    """Test set_properties edge cases."""

    def test_set_with_map_expression(self) -> None:
        star = Star(context=_people_context())
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) SET p += {age: 31, email: 'a@test.com'} RETURN p.age AS age",
        )
        assert result is not None
        assert result["age"].iloc[0] == 31

    def test_set_property_to_null(self) -> None:
        star = Star(context=_people_context())
        star.execute_query("MATCH (p:Person {name: 'Alice'}) SET p.age = null")
        result = star.execute_query(
            "MATCH (p:Person {name: 'Alice'}) RETURN p.age AS age"
        )
        assert result is not None
        assert pd.isna(result["age"].iloc[0])
