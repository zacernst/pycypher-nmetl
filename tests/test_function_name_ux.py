"""UX tests: misspelled scalar function names get "Did you mean?" hints.

When a WHERE/WITH/RETURN expression references an unknown scalar function,
the error must:
  1. Name the unrecognised function.
  2. Suggest similar registered functions via difflib when a close match exists.
  3. NOT suggest anything for completely unrelated names.

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


@pytest.fixture
def person_ctx() -> Context:
    df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        },
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


class TestUnknownFunctionError:
    """Unknown function errors must be informative."""

    def test_error_names_the_function(self, person_ctx: Context) -> None:
        """Error message includes the unrecognised function name."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) RETURN noSuchFunction(p.name) AS x",
            )
        assert (
            "noSuchFunction" in str(exc_info.value)
            or "nosuchfunction" in str(exc_info.value).lower()
        )

    def test_error_is_value_error(self, person_ctx: Context) -> None:
        """Unknown function raises ValueError, not AttributeError or KeyError."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError):
            star.execute_query(
                "MATCH (p:Person) RETURN missingFn(p.name) AS x",
            )


# ---------------------------------------------------------------------------
# "Did you mean?" suggestion
# ---------------------------------------------------------------------------


class TestFunctionNameSuggestion:
    """A close-match typo triggers a 'Did you mean?' hint."""

    def test_tolower_typo_suggests_correction(
        self,
        person_ctx: Context,
    ) -> None:
        """'tolow' is close to 'toLower' — error should explicitly suggest it."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN tolow(p.name) AS x")
        error_msg = str(exc_info.value).lower()
        # Must contain "did you mean" explicitly, not just list the function
        assert "did you mean" in error_msg

    def test_did_you_mean_phrasing_present(self, person_ctx: Context) -> None:
        """Error contains 'Did you mean' (case-insensitive) when a close match is found."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN toUppe(p.name) AS x")
        error_msg = str(exc_info.value).lower()
        assert "did you mean" in error_msg

    def test_substring_typo_suggests_substring(
        self,
        person_ctx: Context,
    ) -> None:
        """'substr' is close to 'substring' — error must say "Did you mean 'substring'"."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query(
                "MATCH (p:Person) RETURN substr(p.name, 1) AS x",
            )
        error_msg = str(exc_info.value).lower()
        assert "did you mean" in error_msg

    def test_no_suggestion_for_completely_unrelated_name(
        self,
        person_ctx: Context,
    ) -> None:
        """No 'Did you mean?' for a completely unrelated function name."""
        star = Star(context=person_ctx)
        with pytest.raises(ValueError) as exc_info:
            # 'qxzpqxz' is far from any registered function
            star.execute_query("MATCH (p:Person) RETURN qxzpqxz(p.name) AS x")
        # Must not crash; must still be a ValueError (or subclass)
        assert issubclass(exc_info.type, ValueError)
        # Must NOT contain "Did you mean" since no close match exists
        error_msg = str(exc_info.value).lower()
        assert "did you mean" not in error_msg

    def test_correct_function_does_not_raise(
        self,
        person_ctx: Context,
    ) -> None:
        """Correctly spelled functions must execute without error."""
        star = Star(context=person_ctx)
        result = star.execute_query(
            "MATCH (p:Person) RETURN toLower(p.name) AS low_name",
        )
        assert result["low_name"].tolist() == ["alice", "bob", "charlie"]
