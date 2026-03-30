"""TDD tests for WHERE n:Label predicate support.

Root cause: the Cypher grammar lacks a label predicate expression, so
``WHERE n:Person`` raises ``UnexpectedCharacters``. In standard openCypher /
Neo4j, ``n:Label`` is a valid boolean expression that returns true when the
node bound to ``n`` has the label ``Label``.

Patterns this unlocks:
- ``MATCH (n) WHERE n:Person RETURN n.name``     (unlabeled scan + filter)
- ``MATCH (n:Person) WHERE n:Person RETURN n.name`` (redundant but valid)
- ``WHERE n:Person AND n.age > 28``              (compound predicate)
- ``WHERE NOT n:Person``                         (negation)
- ``WHERE n:OtherLabel``                         (false for all rows)

TDD: all tests written before the fix (red phase).
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def label_star() -> Star:
    """Star with Person and Animal tables for multi-type label tests."""
    persons = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    p_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": p_table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


@pytest.fixture
def multi_type_star() -> Star:
    """Star with both Person and Animal entity types."""
    persons = pd.DataFrame(
        {ID_COLUMN: [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]},
    )
    animals = pd.DataFrame(
        {
            ID_COLUMN: [10, 11],
            "name": ["Rex", "Mimi"],
            "species": ["dog", "cat"],
        },
    )
    p_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons,
    )
    a_table = EntityTable(
        entity_type="Animal",
        identifier="Animal",
        column_names=[ID_COLUMN, "name", "species"],
        source_obj_attribute_map={"name": "name", "species": "species"},
        attribute_map={"name": "name", "species": "species"},
        source_obj=animals,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(
                mapping={"Person": p_table, "Animal": a_table},
            ),
            relationship_mapping=RelationshipMapping(mapping={}),
        ),
    )


# ---------------------------------------------------------------------------
# Class 1: Basic parsing — grammar must accept WHERE n:Label
# ---------------------------------------------------------------------------


class TestLabelPredicateParsing:
    """Grammar must parse WHERE n:Label without raising UnexpectedCharacters."""

    def test_where_label_parses(self) -> None:
        """MATCH (n) WHERE n:Person RETURN n.name must parse without error."""
        from pycypher.grammar_parser import GrammarParser

        p = GrammarParser()
        p.parse("MATCH (n) WHERE n:Person RETURN n.name")

    def test_where_label_with_labeled_match(self) -> None:
        """MATCH (n:Person) WHERE n:Person RETURN n.name must parse."""
        from pycypher.grammar_parser import GrammarParser

        p = GrammarParser()
        p.parse("MATCH (n:Person) WHERE n:Person RETURN n.name")

    def test_where_label_with_compound_and(self) -> None:
        """WHERE n:Person AND n.age > 28 must parse."""
        from pycypher.grammar_parser import GrammarParser

        p = GrammarParser()
        p.parse("MATCH (n) WHERE n:Person AND n.age > 28 RETURN n.name")

    def test_where_not_label(self) -> None:
        """WHERE NOT n:Person must parse."""
        from pycypher.grammar_parser import GrammarParser

        p = GrammarParser()
        p.parse("MATCH (n) WHERE NOT n:Person RETURN n.name")

    def test_where_label_in_or(self) -> None:
        """WHERE n:Person OR n:Animal must parse."""
        from pycypher.grammar_parser import GrammarParser

        p = GrammarParser()
        p.parse("MATCH (n) WHERE n:Person OR n:Animal RETURN n.name")


# ---------------------------------------------------------------------------
# Class 2: Execution — WHERE n:Label filters correctly
# ---------------------------------------------------------------------------


class TestLabelPredicateExecution:
    """WHERE n:Label must filter rows based on the entity's type."""

    def test_unlabeled_match_filtered_by_label(self, label_star: Star) -> None:
        """MATCH (n) WHERE n:Person returns all persons (same as labeled match)."""
        result = label_star.execute_query(
            "MATCH (n) WHERE n:Person RETURN n.name ORDER BY n.name",
        )
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_labeled_match_redundant_where(self, label_star: Star) -> None:
        """MATCH (n:Person) WHERE n:Person is redundant but must return all persons."""
        result = label_star.execute_query(
            "MATCH (n:Person) WHERE n:Person RETURN n.name ORDER BY n.name",
        )
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]

    def test_label_with_property_filter(self, label_star: Star) -> None:
        """WHERE n:Person AND n.age > 28 filters by label AND property."""
        result = label_star.execute_query(
            "MATCH (n) WHERE n:Person AND n.age > 28 RETURN n.name ORDER BY n.name",
        )
        assert list(result["name"]) == ["Alice", "Carol"]

    def test_wrong_label_returns_empty(self, label_star: Star) -> None:
        """WHERE n:Animal returns empty when context has only Person entities."""
        result = label_star.execute_query(
            "MATCH (n) WHERE n:Animal RETURN n.name",
        )
        assert len(result) == 0

    def test_not_label_inverts_result(self, label_star: Star) -> None:
        """WHERE NOT n:Person returns empty (all nodes ARE persons)."""
        result = label_star.execute_query(
            "MATCH (n) WHERE NOT n:Person RETURN n.name",
        )
        assert len(result) == 0

    def test_label_in_return_clause(self, label_star: Star) -> None:
        """RETURN n:Person AS is_person must return True for all rows."""
        result = label_star.execute_query(
            "MATCH (n:Person) RETURN n:Person AS is_person ORDER BY n.name",
        )
        values = list(result["is_person"])
        assert all(v is True for v in values), f"Expected all True, got {values}"

    def test_label_false_in_return_clause(self, label_star: Star) -> None:
        """RETURN n:Animal AS is_animal must return False for Person nodes."""
        result = label_star.execute_query(
            "MATCH (n:Person) RETURN n:Animal AS is_animal ORDER BY n.name",
        )
        values = list(result["is_animal"])
        assert all(v is False for v in values), f"Expected all False, got {values}"


# ---------------------------------------------------------------------------
# Class 3: Multi-type context — label predicate discriminates entity types
# ---------------------------------------------------------------------------


class TestLabelPredicateMultiType:
    """With multiple entity types, WHERE n:Label discriminates between them."""

    def test_filter_persons_from_mixed_scan(
        self,
        multi_type_star: Star,
    ) -> None:
        """Unlabeled scan filtered by n:Person returns only persons."""
        result = multi_type_star.execute_query(
            "MATCH (n) WHERE n:Person RETURN n.name ORDER BY n.name",
        )
        assert set(result["name"]) == {"Alice", "Bob"}

    def test_filter_animals_from_mixed_scan(
        self,
        multi_type_star: Star,
    ) -> None:
        """Unlabeled scan filtered by n:Animal returns only animals."""
        result = multi_type_star.execute_query(
            "MATCH (n) WHERE n:Animal RETURN n.name ORDER BY n.name",
        )
        assert set(result["name"]) == {"Rex", "Mimi"}

    def test_or_label_returns_all(self, multi_type_star: Star) -> None:
        """WHERE n:Person OR n:Animal returns all entities."""
        result = multi_type_star.execute_query(
            "MATCH (n) WHERE n:Person OR n:Animal RETURN n.name ORDER BY n.name",
        )
        assert set(result["name"]) == {"Alice", "Bob", "Rex", "Mimi"}

    def test_and_label_impossible_returns_empty(
        self,
        multi_type_star: Star,
    ) -> None:
        """WHERE n:Person AND n:Animal returns empty (entities have one type)."""
        result = multi_type_star.execute_query(
            "MATCH (n) WHERE n:Person AND n:Animal RETURN n.name",
        )
        assert len(result) == 0

    def test_label_with_not_in_multitype(self, multi_type_star: Star) -> None:
        """WHERE NOT n:Person in mixed context returns only animals."""
        result = multi_type_star.execute_query(
            "MATCH (n) WHERE NOT n:Person RETURN n.name ORDER BY n.name",
        )
        assert set(result["name"]) == {"Rex", "Mimi"}


# ---------------------------------------------------------------------------
# Class 4: ContextBuilder path
# ---------------------------------------------------------------------------


class TestLabelPredicateContextBuilder:
    """Label predicate must work via the ContextBuilder (PyArrow-backed) path."""

    def test_contextbuilder_label_filter(self) -> None:
        """ContextBuilder + WHERE n:Person must return all persons."""
        ctx = ContextBuilder.from_dict(
            {
                "Person": pd.DataFrame(
                    {
                        ID_COLUMN: [1, 2, 3],
                        "name": ["Alice", "Bob", "Carol"],
                        "age": [30, 25, 35],
                    },
                ),
            },
        )
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (n) WHERE n:Person RETURN n.name ORDER BY n.name",
        )
        assert list(result["name"]) == ["Alice", "Bob", "Carol"]
