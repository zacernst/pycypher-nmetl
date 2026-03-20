"""Tests for MERGE ON CREATE SET / ON MATCH SET semantics.

``ON CREATE SET`` applies the SET items only when the pattern was not found
(a new node/relationship was created).  ``ON MATCH SET`` applies the SET
items only when the pattern already existed.

Previously these actions were parsed but silently ignored at execution time.

TDD: all tests written before the implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture()
def person_ctx() -> Context:
    """One existing Person row for merge tests."""
    df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"], "visits": [0]})
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "visits"],
        source_obj_attribute_map={"name": "name", "visits": "visits"},
        attribute_map={"name": "name", "visits": "visits"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# ON CREATE SET
# ---------------------------------------------------------------------------


class TestMergeOnCreateSet:
    """ON CREATE SET runs only when the MERGE creates a new node."""

    def test_on_create_set_applied_when_node_created(
        self, person_ctx: Context
    ) -> None:
        """New node gets the ON CREATE property value."""
        star = Star(context=person_ctx)
        # 'Bob' does not exist — MERGE will create it
        star.execute_query(
            "MERGE (p:Person {name: 'Bob'}) ON CREATE SET p.visits = 42"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.visits AS v"
        )
        assert result["v"].iloc[0] == 42

    def test_on_create_set_not_applied_when_node_matched(
        self, person_ctx: Context
    ) -> None:
        """Existing node does NOT get modified by ON CREATE SET."""
        star = Star(context=person_ctx)
        # 'Alice' already exists — MERGE matches, ON CREATE does not fire
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON CREATE SET p.visits = 999"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.visits AS v"
        )
        # visits should remain 0 (the original value), not 999
        assert result["v"].iloc[0] == 0

    def test_on_create_set_does_not_raise(self, person_ctx: Context) -> None:
        """Regression: ON CREATE SET must not raise any exception."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MERGE (p:Person {name: 'Carol'}) ON CREATE SET p.visits = 1"
        )


# ---------------------------------------------------------------------------
# ON MATCH SET
# ---------------------------------------------------------------------------


class TestMergeOnMatchSet:
    """ON MATCH SET runs only when the MERGE matches an existing node."""

    def test_on_match_set_applied_when_node_matched(
        self, person_ctx: Context
    ) -> None:
        """Existing node gets updated by ON MATCH SET."""
        star = Star(context=person_ctx)
        # 'Alice' exists — MERGE matches, ON MATCH fires
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.visits = 7"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.visits AS v"
        )
        assert result["v"].iloc[0] == 7

    def test_on_match_set_not_applied_when_node_created(
        self, person_ctx: Context
    ) -> None:
        """New node does NOT get the ON MATCH property value."""
        star = Star(context=person_ctx)
        # 'Dave' does not exist — MERGE creates, ON MATCH does not fire
        star.execute_query(
            "MERGE (p:Person {name: 'Dave'}) ON MATCH SET p.visits = 99"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Dave' RETURN p.visits AS v"
        )
        # visits should be None/NA for newly created 'Dave'
        val = result["v"].iloc[0]
        assert (
            val is None
            or (hasattr(val, "__class__") and "NA" in type(val).__name__)
            or (isinstance(val, float) and val != 99)
        )

    def test_on_match_set_does_not_raise(self, person_ctx: Context) -> None:
        """Regression: ON MATCH SET must not raise any exception."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) ON MATCH SET p.visits = 5"
        )


# ---------------------------------------------------------------------------
# Combined ON CREATE SET + ON MATCH SET
# ---------------------------------------------------------------------------


class TestMergeOnCreateAndOnMatchSet:
    """Both ON CREATE and ON MATCH actions can be present simultaneously."""

    def test_combined_create_fires_on_create(
        self, person_ctx: Context
    ) -> None:
        """When node created: ON CREATE fires, ON MATCH does not."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MERGE (p:Person {name: 'Eve'}) "
            "ON CREATE SET p.visits = 1 "
            "ON MATCH SET p.visits = 100"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Eve' RETURN p.visits AS v"
        )
        assert result["v"].iloc[0] == 1

    def test_combined_match_fires_on_match(self, person_ctx: Context) -> None:
        """When node matched: ON MATCH fires, ON CREATE does not."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MERGE (p:Person {name: 'Alice'}) "
            "ON CREATE SET p.visits = 1 "
            "ON MATCH SET p.visits = 100"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.visits AS v"
        )
        assert result["v"].iloc[0] == 100
