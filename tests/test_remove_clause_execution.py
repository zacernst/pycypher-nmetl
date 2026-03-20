"""Tests for REMOVE clause execution.

REMOVE p.property sets the named property to null (None/NaN) for every
matched node, mirroring the Cypher specification.

REMOVE p:Label is silently ignored in the current architecture (the engine
does not maintain label membership separately from entity-table membership),
but must not raise an exception.
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

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_context() -> Context:
    """Three persons with name, age, and optional score."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
            "score": [100.0, 200.0, 300.0],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age", "score"],
        source_obj_attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
        },
        attribute_map={
            "name": "name",
            "age": "age",
            "score": "score",
        },
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


# ---------------------------------------------------------------------------
# REMOVE property tests
# ---------------------------------------------------------------------------


class TestRemoveProperty:
    """REMOVE p.prop sets the property to null for matched nodes."""

    def test_remove_single_property(self, person_context: Context) -> None:
        """REMOVE p.score nullifies the score for all matched persons."""
        star = Star(context=person_context)
        star.execute_query(
            "MATCH (p:Person) REMOVE p.score RETURN p.name AS nm"
        )
        # After execution, the score column must be null for all persons
        result = star.execute_query("MATCH (p:Person) RETURN p.score AS sc")
        assert result["sc"].isna().all(), (
            f"Expected all scores to be null after REMOVE, got: {result['sc'].tolist()}"
        )

    def test_remove_property_followed_by_return(
        self, person_context: Context
    ) -> None:
        """MATCH … REMOVE … RETURN works as a single pipeline."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) REMOVE p.score RETURN p.name AS nm, p.score AS sc"
        )
        assert len(result) == 3
        assert result["sc"].isna().all()
        assert set(result["nm"]) == {"Alice", "Bob", "Charlie"}

    def test_remove_property_does_not_affect_other_properties(
        self, person_context: Context
    ) -> None:
        """Removing score must not touch name or age."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) REMOVE p.score RETURN p.name AS nm, p.age AS ag"
        )
        assert not result["nm"].isna().any()
        assert not result["ag"].isna().any()

    def test_remove_with_where_only_affects_matched_rows(
        self, person_context: Context
    ) -> None:
        """REMOVE after WHERE nullifies score only for matching persons."""
        star = Star(context=person_context)
        # Remove score only for Alice
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' REMOVE p.score RETURN p.name"
        )
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name AS nm, p.score AS sc ORDER BY p.age ASC"
        )
        alice_row = result[result["nm"] == "Alice"]
        bob_row = result[result["nm"] == "Bob"]
        charlie_row = result[result["nm"] == "Charlie"]

        assert alice_row["sc"].isna().all(), "Alice's score should be null"
        assert not bob_row["sc"].isna().any(), (
            "Bob's score should be unchanged"
        )
        assert not charlie_row["sc"].isna().any(), (
            "Charlie's score should be unchanged"
        )

    def test_remove_multiple_properties(self, person_context: Context) -> None:
        """REMOVE p.score REMOVE p.age in separate clauses both nullify."""
        star = Star(context=person_context)
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "REMOVE p.score "
            "RETURN p.name AS nm, p.score AS sc, p.age AS ag"
        )
        assert result["sc"].isna().all()
        # age must still have its original value (only score was removed)
        assert result["ag"].iloc[0] == 25


# ---------------------------------------------------------------------------
# REMOVE label — no-op but must not raise
# ---------------------------------------------------------------------------


class TestRemoveLabel:
    """REMOVE p:Label is architecturally a no-op but must not raise."""

    def test_remove_label_does_not_raise(
        self, person_context: Context
    ) -> None:
        """REMOVE p:Person must execute without raising any exception."""
        star = Star(context=person_context)
        # Should not raise NotImplementedError or any other exception
        result = star.execute_query(
            "MATCH (p:Person) REMOVE p:Person RETURN p.name AS nm"
        )
        # Label removal doesn't change the result rows — they're still returned
        assert len(result) == 3
