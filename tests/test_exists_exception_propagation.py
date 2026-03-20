"""TDD tests verifying that _eval_exists propagates real errors instead of
silently returning False.

Before the fix: *any* exception during per-row EXISTS evaluation is caught and
converted to False.  This masks real bugs:
- A mis-configured context (relationship in entity_mapping) → silently False
- A referenced but non-existent function in the subquery → silently False
- A type error in the subquery expression → silently False

After the fix: only empty-result cases (pattern truly not matched) produce
False; real exceptions propagate so the caller gets an actionable error message
instead of an incorrect result.

All tests written before the fix (TDD step 1).
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
def social_star() -> Star:
    """Alice -KNOWS-> Bob, -KNOWS-> Carol."""
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 1], "__TARGET__": [2, 3]}
    )
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": people_table}),
            relationship_mapping=RelationshipMapping(
                mapping={"KNOWS": knows_table}
            ),
        )
    )


@pytest.fixture()
def broken_star() -> Star:
    """A Star where KNOWS is accidentally placed in entity_mapping.

    Before the fix this causes EXISTS to silently return False for all rows
    instead of raising a ValueError about the missing relationship type.
    """
    people_df = pd.DataFrame(
        {ID_COLUMN: [1, 2, 3], "name": ["Alice", "Bob", "Carol"]}
    )
    people_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name"],
        source_obj_attribute_map={"name": "name"},
        attribute_map={"name": "name"},
        source_obj=people_df,
    )
    knows_df = pd.DataFrame(
        {ID_COLUMN: [10, 11], "__SOURCE__": [1, 1], "__TARGET__": [2, 3]}
    )
    # Intentionally wrong: RelationshipTable in entity_mapping
    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[ID_COLUMN, "__SOURCE__", "__TARGET__"],
        source_obj_attribute_map={},
        attribute_map={},
        source_obj=knows_df,
    )
    return Star(
        context=Context(
            entity_mapping=EntityMapping(
                mapping={"Person": people_table, "KNOWS": knows_table}
            )
            # relationship_mapping intentionally empty
        )
    )


class TestExistsExceptionPropagation:
    """EXISTS subquery errors should propagate, not silently become False."""

    def test_correct_context_returns_true(self, social_star: Star) -> None:
        """Sanity check: correct setup → EXISTS returns True for Alice."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) } "
            "RETURN p.name"
        )
        assert "Alice" in list(r["name"])

    def test_correct_context_returns_false_for_no_match(
        self, social_star: Star
    ) -> None:
        """Correct setup with non-existent name → EXISTS returns False, not error."""
        r = social_star.execute_query(
            "MATCH (p:Person) "
            "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) WHERE f.name = 'Zara' } "
            "RETURN p.name"
        )
        # Should return 0 rows, not raise
        assert len(r) == 0

    def test_misconfigured_context_raises_not_false(
        self, broken_star: Star
    ) -> None:
        """EXISTS with KNOWS in entity_mapping (wrong) must raise, not silently False.

        Before the fix: exists returns [] (all False), Alice is absent.
        After the fix: a ValueError about 'KNOWS not in relationship_mapping' propagates.
        """
        with pytest.raises((ValueError, KeyError)):
            broken_star.execute_query(
                "MATCH (p:Person) "
                "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) } "
                "RETURN p.name"
            )

    def test_unknown_function_in_exists_raises_not_false(
        self, social_star: Star
    ) -> None:
        """EXISTS { MATCH ... WHERE unknownFn(f.name) } must raise, not silently False.

        An unknown scalar function raises ValueError. Before the fix that
        ValueError was swallowed and all rows returned False, silently making
        every person look like they have no matches even though Alice does.
        """
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            social_star.execute_query(
                "MATCH (p:Person) "
                "WHERE EXISTS { MATCH (p)-[:KNOWS]->(f:Person) "
                "WHERE _nonExistentFn_(f.name) = 'Bob' } "
                "RETURN p.name"
            )
