"""Coverage-focused tests for string_predicate_evaluator.py.

Targets uncovered code paths identified via coverage analysis:

1. _validate_regex_pattern — ReDoS, length, and invalid-regex guards
   (existing tests in test_redos_protection.py import from binding_evaluator,
   not from string_predicate_evaluator — this module covers the canonical copy).
2. _validate_string_operand object-dtype iteration — NaN, pd.NA, and non-string
   values in an object-dtype Series.
3. _eval_in_predicate three-valued logic edge cases — null LHS + empty list,
   null items in RHS list, scalar RHS, NOT IN negation with nulls.
4. UnsupportedOperatorError branch — exercised via direct evaluator call.
5. Null mask application on string predicate results — mixed null/string Series
   for STARTS WITH, ENDS WITH, CONTAINS, and =~ operators.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher import Star
from pycypher.exceptions import UnsupportedOperatorError
from pycypher.ingestion import ContextBuilder
from pycypher.string_predicate_evaluator import (
    StringPredicateEvaluator,
    _validate_regex_pattern,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def people_star() -> Star:
    """Star with Person entities including names for string predicate tests."""
    df = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4"],
            "name": ["Alice", "Bob", "Charlie", "Dave"],
            "age": [30, 25, 40, 28],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


@pytest.fixture
def nullable_star() -> Star:
    """Star with Person entities where some names are null."""
    df = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4"],
            "name": ["Alice", None, "Charlie", None],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


@pytest.fixture
def tags_star() -> Star:
    """Star with Person entities having list-typed tags (some with nulls)."""
    df = pd.DataFrame(
        {
            "__ID__": ["p1", "p2", "p3", "p4"],
            "name": ["Alice", "Bob", "Carol", "Dave"],
            "tags": [
                ["python", "ml"],
                ["java", None],
                [],
                None,
            ],
        },
    )
    return Star(context=ContextBuilder.from_dict({"Person": df}))


# ===========================================================================
# 1. _validate_regex_pattern — canonical copy in string_predicate_evaluator
# ===========================================================================


class TestValidateRegexPatternCanonical:
    """Exercise the _validate_regex_pattern in string_predicate_evaluator."""

    def test_rejects_overly_long_pattern(self) -> None:
        """Pattern exceeding 1000 chars is rejected."""
        with pytest.raises(ValueError, match="maximum length"):
            _validate_regex_pattern("a" * 1001)

    def test_rejects_invalid_regex_syntax(self) -> None:
        """Syntactically broken regex is rejected."""
        with pytest.raises(ValueError, match="Invalid regex"):
            _validate_regex_pattern("[unclosed")

    def test_rejects_nested_quantifier_redos(self) -> None:
        """Nested quantifiers like (a+)+ are rejected."""
        with pytest.raises(ValueError, match="catastrophic backtracking"):
            _validate_regex_pattern(r"(a+)+b")

    def test_rejects_star_nested_quantifier(self) -> None:
        """(a*)*b pattern is rejected."""
        with pytest.raises(ValueError, match="catastrophic backtracking"):
            _validate_regex_pattern(r"(a*)*b")

    def test_allows_safe_pattern(self) -> None:
        """A simple, safe pattern passes validation."""
        _validate_regex_pattern(r"^[A-Z][a-z]+$")  # Should not raise

    def test_allows_exact_max_length(self) -> None:
        """Pattern at exactly 1000 chars is accepted."""
        _validate_regex_pattern("a" * 1000)  # Should not raise


# ===========================================================================
# 2. ReDoS/invalid regex via =~ in Cypher queries
# ===========================================================================


class TestRegexViaQuery:
    """Exercise _validate_regex_pattern through actual =~ Cypher queries."""

    def test_regex_match_basic(self, people_star: Star) -> None:
        """=~ with a valid regex returns matching rows."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_regex_fullmatch_semantics(self, people_star: Star) -> None:
        """=~ requires a full match — partial pattern should not match."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name =~ 'Ali' RETURN p.name AS name",
        )
        # 'Ali' does not fully match 'Alice'
        assert len(result) == 0

    def test_regex_with_null_values(self, nullable_star: Star) -> None:
        """=~ on a column with nulls: null rows produce null (filtered out)."""
        result = nullable_star.execute_query(
            "MATCH (p:Person) WHERE p.name =~ 'A.*' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]


# ===========================================================================
# 3. Null mask on string predicate results (lines 164-165)
# ===========================================================================


class TestNullMaskOnStringPredicates:
    """Mixed null/string Series for STARTS WITH, ENDS WITH, CONTAINS."""

    def test_starts_with_null_rows_filtered(self, nullable_star: Star) -> None:
        """STARTS WITH on column with nulls: null rows are excluded."""
        result = nullable_star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_ends_with_null_rows_filtered(self, nullable_star: Star) -> None:
        """ENDS WITH on column with nulls: null rows are excluded."""
        result = nullable_star.execute_query(
            "MATCH (p:Person) WHERE p.name ENDS WITH 'e' RETURN p.name AS name",
        )
        names = sorted(result["name"].tolist())
        assert "Alice" in names
        assert "Charlie" in names

    def test_contains_null_rows_filtered(self, nullable_star: Star) -> None:
        """CONTAINS on column with nulls: null rows are excluded."""
        result = nullable_star.execute_query(
            "MATCH (p:Person) WHERE p.name CONTAINS 'li' RETURN p.name AS name",
        )
        names = sorted(result["name"].tolist())
        assert "Alice" in names
        assert "Charlie" in names

    def test_regex_null_rows_filtered(self, nullable_star: Star) -> None:
        """=~ on column with nulls: null rows are excluded."""
        result = nullable_star.execute_query(
            "MATCH (p:Person) WHERE p.name =~ '.*lie' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Charlie"]


# ===========================================================================
# 4. _validate_string_operand — object-dtype iteration paths
# ===========================================================================


class TestValidateStringOperandObjectDtype:
    """Cover the object-dtype iteration in _validate_string_operand.

    When the Series has dtype 'O' (object), the code iterates values to find
    the first non-null and checks if it is a string. This covers:
    - NaN values (float NaN, v != v)
    - pd.NA values (raises TypeError in boolean context)
    - Non-string values in object-dtype Series
    """

    def test_object_dtype_with_leading_nans(self) -> None:
        """Object Series with NaN before first string should not raise."""
        df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2", "p3"],
                "label": [float("nan"), None, "hello"],
            },
        )
        star = Star(context=ContextBuilder.from_dict({"Thing": df}))
        result = star.execute_query(
            "MATCH (t:Thing) WHERE t.label STARTS WITH 'h' RETURN t.label AS label",
        )
        assert list(result["label"]) == ["hello"]

    def test_object_dtype_with_pd_na_values(self) -> None:
        """Object Series containing pd.NA should not raise during validation.

        pd.NA raises TypeError in boolean context (v != v), which the code
        catches on line 204.
        """
        df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2"],
                "label": pd.array(["hello", pd.NA], dtype=object),
            },
        )
        star = Star(context=ContextBuilder.from_dict({"Thing": df}))
        result = star.execute_query(
            "MATCH (t:Thing) WHERE t.label STARTS WITH 'h' RETURN t.label AS label",
        )
        assert list(result["label"]) == ["hello"]

    def test_object_dtype_non_string_raises(self) -> None:
        """Object Series with non-string first non-null value raises TypeError."""
        df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2"],
                "value": pd.array([None, 42], dtype=object),
            },
        )
        star = Star(context=ContextBuilder.from_dict({"Thing": df}))
        with pytest.raises(TypeError, match="STARTS WITH"):
            star.execute_query(
                "MATCH (t:Thing) WHERE t.value STARTS WITH 'x' RETURN t.value",
            )

    def test_object_dtype_all_none_no_raise(self) -> None:
        """Object Series that is entirely None should not trigger type guard."""
        df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2"],
                "label": pd.array([None, None], dtype=object),
            },
        )
        star = Star(context=ContextBuilder.from_dict({"Thing": df}))
        result = star.execute_query(
            "MATCH (t:Thing) WHERE t.label STARTS WITH 'x' RETURN t.label AS label",
        )
        # All-null: no rows match
        assert len(result) == 0


# ===========================================================================
# 5. IN / NOT IN three-valued logic edge cases
# ===========================================================================


class TestInPredicateThreeValuedLogic:
    """Cover _eval_in_predicate three-valued NULL handling."""

    def test_in_with_null_in_rhs_list_no_match(self, tags_star: Star) -> None:
        """'java' IN ['java', null] → True (found before null).
        'go' IN ['java', null] → null (not found, null present).
        """
        # Bob has tags = ['java', None]
        result = tags_star.execute_query(
            "MATCH (p:Person) WHERE 'java' IN p.tags RETURN p.name AS name",
        )
        assert "Bob" in result["name"].tolist()

    def test_in_with_empty_list_returns_false(self, tags_star: Star) -> None:
        """'x' IN [] → False for Carol (tags = [])."""
        result = tags_star.execute_query(
            "MATCH (p:Person) WHERE 'python' IN p.tags RETURN p.name AS name",
        )
        # Carol has empty list — should not match
        assert "Carol" not in result["name"].tolist()

    def test_not_in_with_null_in_rhs(self, tags_star: Star) -> None:
        """NOT IN with null items in the list uses three-valued negation."""
        result = tags_star.execute_query(
            "MATCH (p:Person) WHERE 'python' NOT IN p.tags RETURN p.name AS name ORDER BY p.name",
        )
        # Alice has python → False, Bob has [java, null] → null (filtered),
        # Carol has [] → True, Dave has null tags → filtered
        assert "Carol" in result["name"].tolist()
        assert "Alice" not in result["name"].tolist()

    def test_not_in_empty_list_returns_all(self) -> None:
        """Value NOT IN [] → True for all non-null values."""
        df = pd.DataFrame(
            {
                "__ID__": ["p1", "p2"],
                "name": ["Alice", "Bob"],
            },
        )
        star = Star(context=ContextBuilder.from_dict({"Person": df}))
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.name NOT IN [] RETURN p.name AS name ORDER BY p.name",
        )
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_in_with_literal_list(self, people_star: Star) -> None:
        """Standard IN with literal list — basic coverage."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name IN ['Alice', 'Dave'] "
            "RETURN p.name AS name ORDER BY p.name",
        )
        assert list(result["name"]) == ["Alice", "Dave"]


# ===========================================================================
# 6. UnsupportedOperatorError branch (direct evaluator call)
# ===========================================================================


class TestUnsupportedOperator:
    """Exercise the UnsupportedOperatorError raise on unknown op string."""

    def test_unknown_op_raises(self) -> None:
        """Calling evaluate_string_predicate with an unknown op raises."""
        evaluator = StringPredicateEvaluator()

        # Build a minimal mock for the evaluator protocol
        class _FakeExpr:
            pass

        class _FakeEvaluator:
            def evaluate(self, expr: object) -> pd.Series:
                return pd.Series(["hello"])

        with pytest.raises(UnsupportedOperatorError):
            evaluator.evaluate_string_predicate(
                "BANANA",
                _FakeExpr(),  # type: ignore[arg-type]
                _FakeExpr(),  # type: ignore[arg-type]
                _FakeEvaluator(),  # type: ignore[arg-type]
            )


# ===========================================================================
# 7. CONTAINS, ENDS WITH, STARTS WITH — additional edge cases
# ===========================================================================


class TestStringPredicateEdgeCases:
    """Edge cases for string predicate operators via Cypher queries."""

    def test_starts_with_empty_string_matches_all(self, people_star: Star) -> None:
        """Every string starts with the empty string."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH '' RETURN p.name AS name",
        )
        assert len(result) == 4

    def test_ends_with_empty_string_matches_all(self, people_star: Star) -> None:
        """Every string ends with the empty string."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name ENDS WITH '' RETURN p.name AS name",
        )
        assert len(result) == 4

    def test_contains_empty_string_matches_all(self, people_star: Star) -> None:
        """Every string contains the empty string."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name CONTAINS '' RETURN p.name AS name",
        )
        assert len(result) == 4

    def test_starts_with_full_string(self, people_star: Star) -> None:
        """A string starts with itself."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name STARTS WITH 'Alice' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_ends_with_full_string(self, people_star: Star) -> None:
        """A string ends with itself."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name ENDS WITH 'Alice' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]

    def test_contains_full_string(self, people_star: Star) -> None:
        """A string contains itself."""
        result = people_star.execute_query(
            "MATCH (p:Person) WHERE p.name CONTAINS 'Alice' RETURN p.name AS name",
        )
        assert list(result["name"]) == ["Alice"]
