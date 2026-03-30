"""Tests for enhanced error messages: keyword typo detection and contextual guidance.

These tests verify that CypherSyntaxError produces helpful suggestions when
the user makes common mistakes like misspelling Cypher keywords or forgetting
required clauses.
"""

import pytest
from pycypher.exceptions import CypherSyntaxError
from pycypher.grammar_parser import GrammarParser

_PARSE_ERRORS = (SyntaxError, CypherSyntaxError)


@pytest.fixture
def parser():
    """Create a GrammarParser instance for testing."""
    return GrammarParser()


# ---------------------------------------------------------------------------
# Keyword typo detection
# ---------------------------------------------------------------------------


class TestKeywordTypoDetection:
    """Test that misspelled keywords produce 'Did you mean ...?' suggestions."""

    def test_mach_suggests_match(self, parser):
        """MACH should suggest MATCH."""
        with pytest.raises(_PARSE_ERRORS, match="MATCH") as exc_info:
            parser.parse("MACH (n) RETURN n")
        assert "Did you mean" in str(exc_info.value)

    def test_retrun_suggests_return(self, parser):
        """RETRUN should suggest RETURN."""
        with pytest.raises(_PARSE_ERRORS, match="RETURN") as exc_info:
            parser.parse("MATCH (n) RETRUN n")
        assert "Did you mean" in str(exc_info.value)

    def test_wehre_suggests_where(self, parser):
        """WEHRE should suggest WHERE."""
        with pytest.raises(_PARSE_ERRORS, match="WHERE") as exc_info:
            parser.parse("MATCH (n) WEHRE n.age > 30 RETURN n")
        assert "Did you mean" in str(exc_info.value)

    def test_creat_suggests_create(self, parser):
        """CREAT should suggest CREATE."""
        with pytest.raises(_PARSE_ERRORS, match="CREATE") as exc_info:
            parser.parse("CREAT (n:Person {name: 'Alice'})")
        assert "Did you mean" in str(exc_info.value)

    def test_delet_suggests_delete(self, parser):
        """DELET should suggest DELETE."""
        with pytest.raises(_PARSE_ERRORS, match="DELETE") as exc_info:
            parser.parse("MATCH (n) DELET n")
        assert "Did you mean" in str(exc_info.value)

    def test_mereg_suggests_merge(self, parser):
        """MEREG should suggest MERGE."""
        with pytest.raises(_PARSE_ERRORS, match="MERGE") as exc_info:
            parser.parse("MEREG (n:Person {name: 'Alice'})")
        assert "Did you mean" in str(exc_info.value)

    def test_completely_invalid_no_suggestion(self, parser):
        """A completely invalid word should not produce a keyword suggestion."""
        with pytest.raises(_PARSE_ERRORS) as exc_info:
            parser.parse("XYZZYPLUGH (n) RETURN n")
        assert "Did you mean" not in str(exc_info.value)

    def test_case_insensitive_typo(self, parser):
        """Typo detection should work regardless of case."""
        with pytest.raises(_PARSE_ERRORS, match="MATCH") as exc_info:
            parser.parse("mach (n) RETURN n")
        assert "Did you mean" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Contextual guidance
# ---------------------------------------------------------------------------


class TestContextualGuidance:
    """Test that contextual hints are produced for common structural errors."""

    def test_unclosed_parenthesis_hint(self, parser):
        """Unclosed parenthesis should produce a hint."""
        with pytest.raises(_PARSE_ERRORS) as exc_info:
            parser.parse("MATCH (n RETURN n")
        msg = str(exc_info.value)
        assert "Hint" in msg or ")" in msg

    def test_unclosed_string_single_quote_hint(self, parser):
        """Unclosed single-quote string should produce a hint."""
        with pytest.raises(_PARSE_ERRORS) as exc_info:
            parser.parse("RETURN 'unclosed")
        msg = str(exc_info.value)
        assert "Hint" in msg or "quote" in msg.lower() or "string" in msg.lower()

    def test_where_without_match_hint(self, parser):
        """WHERE without preceding MATCH should produce guidance."""
        with pytest.raises(_PARSE_ERRORS) as exc_info:
            parser.parse("WHERE n.age > 30 RETURN n")
        msg = str(exc_info.value)
        assert "Hint" in msg or "MATCH" in msg


# ---------------------------------------------------------------------------
# CypherSyntaxError attributes
# ---------------------------------------------------------------------------


class TestCypherSyntaxErrorAttributes:
    """Test that CypherSyntaxError exposes useful structured attributes."""

    def test_line_and_column_set(self, parser):
        """Line and column should be extracted from the Lark exception."""
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse("MATCH (n) WEHRE n.age > 30 RETURN n")
        err = exc_info.value
        assert err.line is not None
        assert err.column is not None

    def test_query_preserved(self, parser):
        """The original query should be preserved on the exception."""
        query = "MACH (n) RETURN n"
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse(query)
        assert exc_info.value.query == query

    def test_keyword_suggestion_attribute(self, parser):
        """keyword_suggestion attribute should be set for typos."""
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse("MACH (n) RETURN n")
        assert exc_info.value.keyword_suggestion != ""
        assert "MATCH" in exc_info.value.keyword_suggestion

    def test_repr_format(self, parser):
        """Repr should show line and column."""
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse("MACH (n) RETURN n")
        repr_str = repr(exc_info.value)
        assert "CypherSyntaxError" in repr_str
        assert "line=" in repr_str
        assert "column=" in repr_str


# ---------------------------------------------------------------------------
# Error message quality
# ---------------------------------------------------------------------------


class TestErrorMessageQuality:
    """Test that error messages are clear, actionable, and well-formatted."""

    def test_message_shows_offending_line(self, parser):
        """Error message should include the offending query line."""
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse("MATCH (n) WEHRE n.age > 30 RETURN n")
        assert "WEHRE" in str(exc_info.value)

    def test_message_shows_caret_pointer(self, parser):
        """Error message should include a caret pointing to the error."""
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse("MATCH (n) WEHRE n.age > 30 RETURN n")
        assert "^" in str(exc_info.value)

    def test_multiline_query_error(self, parser):
        """Multi-line query errors should show the correct line."""
        query = "MATCH (n:Person)\nWEHRE n.age > 30\nRETURN n"
        with pytest.raises(CypherSyntaxError) as exc_info:
            parser.parse(query)
        assert "WEHRE" in str(exc_info.value)
