"""Tests for MERGE clause execution.

MERGE (n:Label {prop: val}) either:
  - Finds an existing node matching the pattern → binds it
  - Creates a new node matching the pattern → binds the new node

This is the graph "upsert" primitive — essential for idempotent data loads.

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


@pytest.fixture
def person_context() -> Context:
    """Alice and Bob in the Person table; no relationships."""
    people_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
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
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": people_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def empty_context() -> Context:
    """Completely empty context — no entities or relationships."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestMergeMatchesExisting:
    """MERGE finds existing nodes that match the pattern."""

    def test_merge_returns_existing_node(
        self,
        person_context: Context,
    ) -> None:
        """MERGE on an existing node returns it without creating a duplicate."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Alice"

    def test_merge_does_not_duplicate_existing_node(
        self,
        person_context: Context,
    ) -> None:
        """MERGE on an existing node leaves the count unchanged."""
        star = Star(context=person_context)
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) RETURN p.name AS name",
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result["name"].tolist().count("Alice") == 1

    def test_merge_returns_correct_property_of_existing_node(
        self,
        person_context: Context,
    ) -> None:
        """MERGE on an existing node can return other properties."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MERGE (p:Person {name: 'Bob'}) RETURN p.age AS age",
        )
        assert result["age"].iloc[0] == 25


class TestMergeCreatesNew:
    """MERGE creates a new node when no match is found."""

    def test_merge_creates_node_when_missing(
        self,
        person_context: Context,
    ) -> None:
        """MERGE with no match creates the node and returns it."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MERGE (p:Person {name: 'Carol'}) RETURN p.name AS name",
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Carol"

    def test_merge_created_node_persists(
        self,
        person_context: Context,
    ) -> None:
        """A node created via MERGE is visible in subsequent MATCH queries."""
        star = Star(context=person_context)
        star.execute_query(
            "MERGE (p:Person {name: 'Carol'}) RETURN p.name AS name",
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert "Carol" in result["name"].tolist()

    def test_merge_into_empty_context(self, empty_context: Context) -> None:
        """MERGE into an empty context creates the entity type and the node."""
        star = Star(context=empty_context)
        result = star.execute_query(
            "MERGE (w:Widget {code: 'W1'}) RETURN w.code AS code",
        )
        assert len(result) == 1
        assert result["code"].iloc[0] == "W1"

    def test_merge_created_node_in_empty_context_persists(
        self,
        empty_context: Context,
    ) -> None:
        """MERGE-created entity type is available in subsequent MATCH."""
        star = Star(context=empty_context)
        star.execute_query(
            "MERGE (w:Widget {code: 'W1'}) RETURN w.code AS code",
        )
        result = star.execute_query("MATCH (w:Widget) RETURN w.code AS code")
        assert "W1" in result["code"].tolist()

    def test_merge_does_not_raise_not_implemented(
        self,
        person_context: Context,
    ) -> None:
        """Regression: MERGE must not raise NotImplementedError."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MERGE (p:Person {name: 'Dave'}) RETURN p.name AS name",
        )
        assert result is not None


class TestMergeIdempotency:
    """MERGE is idempotent — running it twice produces the same state."""

    def test_merge_twice_does_not_create_duplicate(
        self,
        person_context: Context,
    ) -> None:
        """Running MERGE for the same node twice leaves exactly one copy."""
        star = Star(context=person_context)
        star.execute_query(
            "MERGE (p:Person {name: 'Dave'}) RETURN p.name AS name",
        )
        star.execute_query(
            "MERGE (p:Person {name: 'Dave'}) RETURN p.name AS name",
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result["name"].tolist().count("Dave") == 1

    def test_merge_existing_then_missing_correct_counts(
        self,
        person_context: Context,
    ) -> None:
        """Merging an existing then a new node gives total count = 3."""
        star = Star(context=person_context)
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) RETURN p.name AS n",
        )
        star.execute_query(
            "MERGE (p:Person {name: 'Carol'}) RETURN p.name AS n",
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert len(result) == 3  # Alice (existing), Bob (existing), Carol (created)
