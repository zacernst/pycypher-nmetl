"""UX tests for "Did you mean?" hints on unknown entity/relationship type errors.

When ``MATCH (p:Persn)`` is executed against a context that has ``Person``
registered, the error should include a close-match hint:

    ValueError: Entity type 'Persn' is not registered ...
                Did you mean 'Person'?

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
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


@pytest.fixture()
def person_ctx() -> Context:
    """Single Person entity table."""
    df = pd.DataFrame({ID_COLUMN: [1], "name": ["Alice"]})
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


class TestEntityTypeDidYouMean:
    """Close-match hints for misspelled entity type labels in MATCH."""

    def test_unknown_entity_type_raises_value_error(
        self, person_ctx: Context
    ) -> None:
        """MATCH on unregistered entity type raises ValueError."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError):
            star.execute_query("MATCH (p:Persn) RETURN p.name AS name")

    def test_unknown_entity_type_error_names_the_type(
        self, person_ctx: Context
    ) -> None:
        """Error message includes the misspelled type name."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError, match="Persn"):
            star.execute_query("MATCH (p:Persn) RETURN p.name AS name")

    def test_close_match_triggers_did_you_mean(
        self, person_ctx: Context
    ) -> None:
        """'Persn' is close to 'Person' — error includes 'Did you mean'."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError, match="Did you mean"):
            star.execute_query("MATCH (p:Persn) RETURN p.name AS name")

    def test_close_match_names_the_suggestion(
        self, person_ctx: Context
    ) -> None:
        """The 'Did you mean' hint names 'Person'."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError, match="'Person'"):
            star.execute_query("MATCH (p:Persn) RETURN p.name AS name")

    def test_completely_wrong_type_no_suggestion(
        self, person_ctx: Context
    ) -> None:
        """'Xyzzy' is not close to 'Person' — no 'Did you mean'."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query("MATCH (p:Xyzzy) RETURN p.name AS name")
        assert "Did you mean" not in str(exc_info.value)

    def test_correct_entity_type_executes_normally(
        self, person_ctx: Context
    ) -> None:
        """Correctly spelled entity type runs without error."""
        star = Star(context=person_ctx)
        result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
        assert result["name"].iloc[0] == "Alice"


@pytest.fixture()
def knows_ctx() -> Context:
    """Person nodes + KNOWS relationships for relationship-type hint tests."""
    pdf = pd.DataFrame({ID_COLUMN: [1, 2], "name": ["Alice", "Bob"]})
    ptable = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=pdf,
    )
    from pycypher.relational_models import (
        RELATIONSHIP_SOURCE_COLUMN,
        RELATIONSHIP_TARGET_COLUMN,
    )

    rdf = pd.DataFrame(
        {
            ID_COLUMN: [10],
            RELATIONSHIP_SOURCE_COLUMN: [1],
            RELATIONSHIP_TARGET_COLUMN: [2],
        }
    )
    rtable = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=rdf,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": ptable}),
        relationship_mapping=RelationshipMapping(mapping={"KNOWS": rtable}),
    )


class TestRelationshipTypeDidYouMean:
    """Close-match hints for misspelled relationship type labels in MATCH."""

    def test_unknown_rel_type_raises_value_error(
        self, knows_ctx: Context
    ) -> None:
        """MATCH on unregistered relationship type raises ValueError."""
        star = Star(context=knows_ctx)
        with pytest.raises(ValueError):
            star.execute_query(
                "MATCH (p:Person)-[r:KNOS]->(q:Person) RETURN p.name AS n"
            )

    def test_unknown_rel_type_error_names_the_type(
        self, knows_ctx: Context
    ) -> None:
        """Error message includes the misspelled relationship type."""
        star = Star(context=knows_ctx)
        with pytest.raises(ValueError, match="KNOS"):
            star.execute_query(
                "MATCH (p:Person)-[r:KNOS]->(q:Person) RETURN p.name AS n"
            )

    def test_close_rel_type_triggers_did_you_mean(
        self, knows_ctx: Context
    ) -> None:
        """'KNOS' is close to 'KNOWS' — error includes 'Did you mean'."""
        star = Star(context=knows_ctx)
        with pytest.raises(ValueError, match="Did you mean"):
            star.execute_query(
                "MATCH (p:Person)-[r:KNOS]->(q:Person) RETURN p.name AS n"
            )

    def test_completely_wrong_rel_type_no_suggestion(
        self, knows_ctx: Context
    ) -> None:
        """'XYZZY' has no match — no 'Did you mean'."""
        star = Star(context=knows_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (p:Person)-[r:XYZZY]->(q:Person) RETURN p.name AS n"
            )
        assert "Did you mean" not in str(exc_info.value)
