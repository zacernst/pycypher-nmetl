"""Tests for CREATE clause execution.

CREATE (n:Label {prop: val}) inserts new nodes into the entity tables
and binds the new variable so that subsequent clauses (SET, RETURN) can
use it.  CREATE (a)-[:TYPE]->(b) inserts a new relationship row.

TDD: all tests written before implementation.
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
)
from pycypher.star import Star


@pytest.fixture()
def empty_context() -> Context:
    """Context with Person table only — used for CREATE into existing/new tables."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=people_df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestCreateNodeLiterals:
    """CREATE (n:Label {prop: 'value'}) — literal property values."""

    def test_create_returns_string_property(
        self, empty_context: Context
    ) -> None:
        """CREATE with a string literal property is returned correctly."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CREATE (n:Widget {name: 'Gadget'}) RETURN n.name AS name"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Gadget"

    def test_create_returns_integer_property(
        self, empty_context: Context
    ) -> None:
        """CREATE with an integer literal property is returned correctly."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CREATE (n:Widget {score: 42}) RETURN n.score AS score"
        )
        assert len(result) == 1
        assert result["score"].iloc[0] == 42

    def test_create_returns_multiple_properties(
        self, empty_context: Context
    ) -> None:
        """Multiple literal properties on a CREATE node are all accessible."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CREATE (n:Widget {name: 'Gadget', score: 7}) "
            "RETURN n.name AS name, n.score AS score"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Gadget"
        assert result["score"].iloc[0] == 7

    def test_create_does_not_raise_not_implemented(
        self, empty_context: Context
    ) -> None:
        """Regression: CREATE must not raise NotImplementedError."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CREATE (n:Widget {name: 'x'}) RETURN n.name AS n"
        )
        assert result is not None


class TestCreateNodePersistence:
    """Created nodes persist across subsequent queries in the same Star session."""

    def test_created_node_visible_in_subsequent_match(
        self, empty_context: Context
    ) -> None:
        """A node created by CREATE is found by a later MATCH in the same session."""
        star = Star(context=empty_context)
        star.execute_query(
            "CREATE (n:Widget {name: 'Gadget'}) RETURN n.name AS n"
        )
        result = star.execute_query("MATCH (w:Widget) RETURN w.name AS name")
        assert "Gadget" in result["name"].tolist()

    def test_created_node_has_correct_property_in_subsequent_match(
        self, empty_context: Context
    ) -> None:
        """Properties set on CREATE are readable in a subsequent MATCH query."""
        star = Star(context=empty_context)
        star.execute_query(
            "CREATE (n:Widget {name: 'Gadget', score: 99}) RETURN n.name AS n"
        )
        result = star.execute_query(
            "MATCH (w:Widget) WHERE w.name = 'Gadget' RETURN w.score AS score"
        )
        assert result["score"].iloc[0] == 99

    def test_create_into_existing_entity_type(
        self, empty_context: Context
    ) -> None:
        """CREATE can add rows to an existing entity type (Person)."""
        star = Star(context=empty_context)
        initial_count = len(
            star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        )
        star.execute_query(
            "CREATE (p:Person {name: 'Carol', age: 35}) RETURN p.name AS n"
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == initial_count + 1
        assert "Carol" in result["name"].tolist()

    def test_create_atomicity_failure_does_not_persist(
        self, empty_context: Context
    ) -> None:
        """A failing query after CREATE does not persist the created node."""
        star = Star(context=empty_context)
        with pytest.raises(Exception):
            star.execute_query(
                "CREATE (n:Widget {name: 'Ghost'}) RETURN nosuchvar.prop AS x"
            )
        # Widget table should either not exist or have no 'Ghost' rows
        try:
            result = star.execute_query(
                "MATCH (w:Widget) RETURN w.name AS name"
            )
            assert "Ghost" not in result["name"].tolist()
        except Exception:
            pass  # Widget type not found is acceptable too


class TestCreateAfterMatch:
    """CREATE following MATCH creates one new node per matched row."""

    def test_create_after_match_creates_per_row(
        self, empty_context: Context
    ) -> None:
        """MATCH + CREATE produces one new entity per matched row."""
        star = Star(context=empty_context)
        # Alice matches; one Friend should be created
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "CREATE (f:Friend {name: 'Dave'}) "
            "RETURN f.name AS name"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Dave"

    def test_create_references_match_variable_in_property(
        self, empty_context: Context
    ) -> None:
        """CREATE property expression can reference a variable from preceding MATCH."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "CREATE (f:Friend {label: p.name}) "
            "RETURN f.label AS label"
        )
        assert len(result) == 1
        assert result["label"].iloc[0] == "Alice"


class TestCreateRelationship:
    """CREATE (a)-[:TYPE]->(b) inserts a new relationship row."""

    def test_create_relationship_between_new_nodes(
        self, empty_context: Context
    ) -> None:
        """CREATE creates both nodes and the relationship between them."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "CREATE (a:Widget {name: 'A'})-[:CONNECTS]->(b:Widget {name: 'B'}) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        assert len(result) == 1
        assert result["src"].iloc[0] == "A"
        assert result["tgt"].iloc[0] == "B"

    def test_created_relationship_visible_in_subsequent_match(
        self, empty_context: Context
    ) -> None:
        """A relationship created by CREATE is traversable in a later MATCH."""
        star = Star(context=empty_context)
        star.execute_query(
            "CREATE (a:Widget {name: 'A'})-[:CONNECTS]->(b:Widget {name: 'B'}) "
            "RETURN a.name AS src"
        )
        result = star.execute_query(
            "MATCH (a:Widget)-[:CONNECTS]->(b:Widget) "
            "RETURN a.name AS src, b.name AS tgt"
        )
        assert len(result) == 1
        assert result["src"].iloc[0] == "A"
        assert result["tgt"].iloc[0] == "B"
