"""Tests for FOREACH loop variable type inference.

When the list expression is a list of entity variables, the loop variable
should inherit the entity type, enabling SET on the loop variable.

Previously, `FOREACH (x IN [p] | SET x.prop = val)` raised KeyError because
'x' was not in the type_registry of the loop BindingFrame.

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
)
from pycypher.star import Star

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_ctx() -> Context:
    """Two Person rows for loop variable type inference tests."""
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2],
            "name": ["Alice", "Bob"],
            "processed": [False, False],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "processed"],
        source_obj_attribute_map={"name": "name", "processed": "processed"},
        attribute_map={"name": "name", "processed": "processed"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# Type inference tests
# ---------------------------------------------------------------------------


class TestForeachLoopVariableTypeInference:
    """Loop variable in FOREACH inherits entity type from list element."""

    def test_set_on_loop_variable_from_single_entity_list(
        self, person_ctx: Context
    ) -> None:
        """FOREACH (x IN [p] | SET x.processed = true) works without KeyError."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MATCH (p:Person) FOREACH (x IN [p] | SET x.processed = true)"
        )
        result = star.execute_query("MATCH (p:Person) RETURN p.processed AS v")
        assert all(result["v"].tolist())

    def test_set_on_loop_variable_does_not_raise(
        self, person_ctx: Context
    ) -> None:
        """Regression: FOREACH (x IN [p] | SET x.prop) must not raise KeyError."""
        star = Star(context=person_ctx)
        # Should not raise
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "FOREACH (x IN [p] | SET x.name = 'AliceUpdated')"
        )

    def test_set_on_loop_variable_single_person(
        self, person_ctx: Context
    ) -> None:
        """Loop variable SET only affects matched person, not all persons."""
        star = Star(context=person_ctx)
        star.execute_query(
            "MATCH (p:Person) WHERE p.name = 'Alice' "
            "FOREACH (x IN [p] | SET x.processed = true)"
        )
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.processed = true RETURN p.name AS name"
        )
        # Only Alice was in the MATCH; only Alice gets processed=true
        assert set(result["name"].tolist()) == {"Alice"}

    def test_foreach_with_variable_list_does_not_break_outer_frame(
        self, person_ctx: Context
    ) -> None:
        """FOREACH (x IN [p] | ...) leaves the outer frame unchanged for RETURN."""
        star = Star(context=person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) "
            "FOREACH (x IN [p] | SET x.processed = true) "
            "RETURN p.name AS name"
        )
        assert set(result["name"].tolist()) == {"Alice", "Bob"}
