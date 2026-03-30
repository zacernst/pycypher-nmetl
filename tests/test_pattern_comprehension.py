"""Tests for PatternComprehension evaluation.

Pattern comprehensions collect values from graph patterns into lists:
  [(p)-[:KNOWS]->(f) | f.name]  — list of names of all friends of p
  [(p)-[:KNOWS]->(f) WHERE f.age > 25 | f.name]  — filtered
  [(p)-[:KNOWS]->(f)]  — no map: returns list of target IDs

Only single-hop directed patterns are supported in this implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture
def knows_context() -> Context:
    """Three people: Alice knows Bob and Carol; Bob knows Carol."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
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
    # Alice(1)→Bob(2), Alice(1)→Carol(3), Bob(2)→Carol(3)
    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [101, 102, 103],
            "__SOURCE__": [1, 1, 2],
            "__TARGET__": [2, 3, 3],
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
            mapping={"KNOWS": knows_table},
        ),
    )


class TestPatternComprehension:
    """PatternComprehension evaluation: [(src)-[:REL]->(tgt) | expr]."""

    def test_basic_pattern_comprehension_returns_list(
        self,
        knows_context: Context,
    ) -> None:
        """Pattern comprehension produces a list column, one list per row."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends",
        )
        assert len(result) == 1
        friends = result["friends"].iloc[0]
        assert isinstance(friends, list)
        assert set(friends) == {"Bob", "Carol"}

    def test_pattern_comprehension_counts_match_result(
        self,
        knows_context: Context,
    ) -> None:
        """Bob knows one person — list has one element."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends",
        )
        friends = result["friends"].iloc[0]
        assert friends == ["Carol"]

    def test_pattern_comprehension_with_where_filter(
        self,
        knows_context: Context,
    ) -> None:
        """WHERE inside comprehension filters targets by predicate."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [(p)-[:KNOWS]->(f:Person) WHERE f.age > 25 | f.name] AS friends",
        )
        friends = result["friends"].iloc[0]
        # Bob (age=25) excluded; Carol (age=35) included
        assert friends == ["Carol"]

    def test_pattern_comprehension_no_matches_returns_empty_list(
        self,
        knows_context: Context,
    ) -> None:
        """Carol has no outgoing KNOWS — result is empty list."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Carol' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends",
        )
        friends = result["friends"].iloc[0]
        assert friends == []

    def test_pattern_comprehension_multiple_rows(
        self,
        knows_context: Context,
    ) -> None:
        """One list per source row — each list has different contents."""
        star = Star(context=knows_context)
        result = star.execute_query(
            "MATCH (p:Person) "
            "RETURN p.name AS person, [(p)-[:KNOWS]->(f:Person) | f.name] AS friends "
            "ORDER BY p.name",
        )
        assert len(result) == 3
        alice_row = result[result["person"] == "Alice"].iloc[0]
        bob_row = result[result["person"] == "Bob"].iloc[0]
        carol_row = result[result["person"] == "Carol"].iloc[0]
        assert set(alice_row["friends"]) == {"Bob", "Carol"}
        assert bob_row["friends"] == ["Carol"]
        assert carol_row["friends"] == []

    def test_pattern_comprehension_does_not_raise_not_implemented(
        self,
        knows_context: Context,
    ) -> None:
        """Pattern comprehension must not raise NotImplementedError."""
        star = Star(context=knows_context)
        # Previously raised: "Expression type 'PatternComprehension' not yet supported"
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "RETURN [(p)-[:KNOWS]->(f:Person) | f.name] AS friends",
        )
        assert result is not None
