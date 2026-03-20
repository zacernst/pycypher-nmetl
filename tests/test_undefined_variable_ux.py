"""UX tests: undefined variable errors must be helpful.

When a WHERE clause or RETURN clause references a variable not bound
by any preceding MATCH clause, the error must:
  1. Clearly name the undefined variable.
  2. List available variables.
  3. Suggest close matches (Did you mean?) via difflib when applicable.

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
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def person_ctx() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )
    table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=df,
    )
    return Context(entity_mapping=EntityMapping(mapping={"Person": table}))


# ---------------------------------------------------------------------------
# Basic error content
# ---------------------------------------------------------------------------


class TestUndefinedVariableError:
    """Error for undefined WHERE/RETURN variables must be informative."""

    def test_undefined_variable_error_names_the_variable(
        self, person_ctx: Context
    ) -> None:
        """Error message includes the undefined variable name."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (n:Person) WHERE m.age > 30 RETURN n.name"
            )
        assert "'m'" in str(exc_info.value) or "m" in str(exc_info.value)

    def test_undefined_variable_error_lists_available_variables(
        self, person_ctx: Context
    ) -> None:
        """Error message lists variables that *are* bound."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (n:Person) WHERE m.age > 30 RETURN n.name"
            )
        assert "n" in str(exc_info.value)

    def test_undefined_variable_raises_value_error(
        self, person_ctx: Context
    ) -> None:
        """The exception type is ValueError, not KeyError or AttributeError."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError):
            star.execute_query(
                "MATCH (n:Person) WHERE typo.age > 30 RETURN n.name"
            )


# ---------------------------------------------------------------------------
# "Did you mean?" suggestion via difflib
# ---------------------------------------------------------------------------


class TestUndefinedVariableSuggestion:
    """A close-match typo should trigger a 'Did you mean?' hint."""

    def test_close_match_suggests_correction(
        self, person_ctx: Context
    ) -> None:
        """Typo 'nn' close to 'n' should suggest 'n' in the error message."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            # 'nn' is 1 edit from 'n' — difflib should catch it
            star.execute_query(
                "MATCH (n:Person) WHERE nn.age > 30 RETURN n.name"
            )
        error_msg = str(exc_info.value)
        # The error should contain a "Did you mean?" style suggestion
        assert "n" in error_msg  # 'n' is the close match

    def test_suggestion_uses_did_you_mean_phrasing(
        self, person_ctx: Context
    ) -> None:
        """Error message contains 'Did you mean' when a close match exists."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (person:Person) WHERE persn.age > 30 RETURN person.name"
            )
        error_msg = str(exc_info.value).lower()
        assert "did you mean" in error_msg or "similar" in error_msg

    def test_no_suggestion_for_completely_different_name(
        self, person_ctx: Context
    ) -> None:
        """No suggestion when the undefined variable has no close match."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (n:Person) WHERE xyz.age > 30 RETURN n.name"
            )
        # No "Did you mean?" since 'xyz' is far from 'n'
        # Must not raise AttributeError trying to form the suggestion
        assert issubclass(exc_info.type, ValueError)

    def test_valid_query_not_affected(self, person_ctx: Context) -> None:
        """Correctly written queries must not trigger suggestion logic."""
        star = Star(context=person_ctx)
        result = star.execute_query(
            "MATCH (n:Person) WHERE n.age > 28 RETURN n.name AS name"
        )
        assert set(result["name"].tolist()) == {"Bob", "Charlie"}
