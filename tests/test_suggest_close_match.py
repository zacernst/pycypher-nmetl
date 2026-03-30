"""Tests for the suggest_close_match utility extracted from three duplicate sites.

The helper centralises the difflib.get_close_matches pattern that previously
appeared verbatim in:
  - binding_evaluator._eval_variable
  - binding_frame.get_property
  - scalar_functions.ScalarFunctionRegistry.execute

TDD: all tests written before implementation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from shared.helpers import suggest_close_match

# ---------------------------------------------------------------------------
# Unit tests — shared.helpers.suggest_close_match
# ---------------------------------------------------------------------------


class TestSuggestCloseMatchUnit:
    """Direct tests for the shared utility function."""

    def test_close_match_returns_hint_string(self) -> None:
        """A typo close to one candidate returns a non-empty hint."""
        hint = suggest_close_match("persn", ["person", "company", "product"])
        assert hint != ""
        assert "person" in hint

    def test_close_match_formats_as_did_you_mean(self) -> None:
        """The hint has the canonical 'Did you mean' phrasing."""
        hint = suggest_close_match("tolow", ["tolower", "toupper", "trim"])
        assert "did you mean" in hint.lower()
        assert "tolower" in hint

    def test_no_close_match_returns_empty_string(self) -> None:
        """Completely unrelated target produces no hint."""
        hint = suggest_close_match("xyzqxzp", ["person", "company"])
        assert hint == ""

    def test_empty_candidates_returns_empty_string(self) -> None:
        """Empty candidate list always produces no hint."""
        hint = suggest_close_match("anything", [])
        assert hint == ""

    def test_exact_match_returns_empty_string(self) -> None:
        """Exact match is not 'close' — no 'Did you mean' for correct input."""
        # When the user types the correct name, no hint is needed.
        # difflib.get_close_matches includes the exact match, but we
        # only emit a hint when the target is NOT already in candidates.
        hint = suggest_close_match("person", ["person", "company"])
        assert hint == ""

    def test_returns_string_type(self) -> None:
        """Return type is always str regardless of match outcome."""
        hint_match = suggest_close_match("persn", ["person"])
        hint_no_match = suggest_close_match("xyz", ["person"])
        assert isinstance(hint_match, str)
        assert isinstance(hint_no_match, str)

    def test_custom_cutoff_respected(self) -> None:
        """A high cutoff silences suggestions for weak matches."""
        # 'xyz' shares nothing with 'person' — both cutoffs produce no hint
        hint_strict = suggest_close_match("xyz", ["person"], cutoff=0.9)
        hint_low = suggest_close_match("xyz", ["person"], cutoff=0.1)
        assert hint_strict == ""
        # At a very low cutoff even a poor match might surface, but 'xyz'
        # vs 'person' should still produce nothing at 0.1
        assert hint_low == ""

    def test_list_input_accepted(self) -> None:
        """Candidates may be a plain list."""
        hint = suggest_close_match("tolwr", ["tolower", "trim"])
        assert isinstance(hint, str)

    def test_hint_ends_with_question_mark(self) -> None:
        """Convention: hint ends with '?'."""
        hint = suggest_close_match("persn", ["person"])
        if hint:
            assert hint.rstrip().endswith("?")


# ---------------------------------------------------------------------------
# Integration tests — call sites use the helper
# ---------------------------------------------------------------------------


from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
)
from pycypher.star import Star


@pytest.fixture
def one_person_ctx() -> Context:
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


class TestSuggestCloseMatchIntegration:
    """Confirm the three call sites now use suggest_close_match."""

    def test_binding_evaluator_variable_error_has_suggestion(
        self,
        one_person_ctx: Context,
    ) -> None:
        """_eval_variable 'Did you mean' hint still appears after refactor."""
        star = Star(context=one_person_ctx)
        with pytest.raises(ValueError) as exc_info:
            # Bind as 'person', then misspell as 'persn' in WHERE clause
            star.execute_query(
                "MATCH (person:Person) WHERE persn.name = 'Alice' RETURN person.name AS name",
            )
        assert "did you mean" in str(exc_info.value).lower()

    def test_scalar_registry_execute_has_suggestion(
        self,
        one_person_ctx: Context,
    ) -> None:
        """ScalarFunctionRegistry 'Did you mean' hint still appears after refactor."""
        star = Star(context=one_person_ctx)
        with pytest.raises(ValueError) as exc_info:
            star.execute_query("MATCH (p:Person) RETURN toUppe(p.name) AS n")
        assert "did you mean" in str(exc_info.value).lower()
