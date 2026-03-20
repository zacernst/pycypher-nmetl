"""TDD tests for GrammarParser.validate() exception narrowing — Error handling loop.

Currently validate() uses `except Exception: return False`, which silently
swallows internal transformer bugs (VisitError, AttributeError, etc.) and
makes them indistinguishable from genuine parse errors.

The fix: catch only `lark.exceptions.UnexpectedInput`, the base class for all
real Lark parse failures.  Non-parse exceptions must propagate.

Tests 1–2 are the *red phase*: they fail before the fix (validate() returns
False instead of raising) and pass after the fix (exceptions propagate).
Tests 3–6 are regression guards that should pass both before and after.

Run with:
    uv run pytest tests/test_grammar_parser_validate.py -v
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from lark.exceptions import VisitError
from pycypher.grammar_parser import GrammarParser

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def parser() -> GrammarParser:
    return GrammarParser()


# ---------------------------------------------------------------------------
# Red-phase tests — fail before the fix
# ---------------------------------------------------------------------------


class TestNonParseExceptionsPropagateAfterFix:
    def test_non_lark_exception_propagates(
        self, parser: GrammarParser
    ) -> None:
        """An AttributeError inside parse() must NOT be swallowed by validate().

        Before fix: validate() catches Exception and returns False.
        After fix: AttributeError propagates to the caller.
        """
        with patch.object(
            parser,
            "parse",
            side_effect=AttributeError("simulated internal bug"),
        ):
            with pytest.raises(AttributeError, match="simulated internal bug"):
                parser.validate("MATCH (n) RETURN n")

    def test_visit_error_propagates(self, parser: GrammarParser) -> None:
        """A VisitError (transformer bug) must NOT be swallowed by validate().

        VisitError is a LarkError but NOT an UnexpectedInput — it wraps bugs
        in the Lark grammar transformer.

        Before fix: validate() catches Exception and returns False.
        After fix: VisitError propagates to the caller.
        """
        visit_err = VisitError("some_rule", None, ValueError("inner error"))
        with patch.object(parser, "parse", side_effect=visit_err):
            with pytest.raises(VisitError):
                parser.validate("MATCH (n) RETURN n")


# ---------------------------------------------------------------------------
# Regression guards — must pass both before and after the fix
# ---------------------------------------------------------------------------


class TestValidateRegressionGuards:
    def test_valid_query_returns_true(self, parser: GrammarParser) -> None:
        """A syntactically valid Cypher query always returns True."""
        assert parser.validate("MATCH (n:Person) RETURN n.name") is True

    def test_valid_query_with_where_returns_true(
        self, parser: GrammarParser
    ) -> None:
        """A valid query with WHERE clause returns True."""
        assert (
            parser.validate("MATCH (n:Person) WHERE n.age > 30 RETURN n")
            is True
        )

    def test_invalid_cypher_missing_paren_returns_false(
        self, parser: GrammarParser
    ) -> None:
        """Missing closing parenthesis is a genuine parse error → False."""
        assert parser.validate("MATCH (n RETURN n") is False

    def test_invalid_cypher_garbage_returns_false(
        self, parser: GrammarParser
    ) -> None:
        """Garbage input is a genuine parse error → False."""
        assert parser.validate("!@#$%^&*") is False

    def test_invalid_cypher_unexpected_eof_returns_false(
        self, parser: GrammarParser
    ) -> None:
        """Truncated query (unexpected EOF) is a genuine parse error → False."""
        assert parser.validate("MATCH () RETURN") is False

    def test_empty_string_returns_false(self, parser: GrammarParser) -> None:
        """Empty string is not valid Cypher → False."""
        assert parser.validate("") is False
