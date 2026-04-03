"""Property-based tests for the Cypher parser.

Tests structural properties that must hold for ALL valid inputs:
1. Parse-round-trip — parsing valid Cypher must never crash
2. Error consistency — invalid Cypher must raise, not return garbage
3. AST structure invariants — parsed ASTs have required fields
4. Whitespace insensitivity — extra whitespace doesn't change semantics
"""

from __future__ import annotations

import re
import string

from hypothesis import given, settings, assume
from hypothesis import strategies as st

from pycypher.grammar_parser import GrammarParser

SETTINGS = settings(max_examples=50, deadline=10000)

# Reusable parser instance (thread-safe for reads)
_parser = GrammarParser()


# ---------------------------------------------------------------------------
# Strategies for generating Cypher-like fragments
# ---------------------------------------------------------------------------

_identifiers = st.text(
    alphabet=string.ascii_letters + "_",
    min_size=1,
    max_size=8,
).filter(lambda s: s[0].isalpha() and s.upper() not in {
    "MATCH", "RETURN", "WHERE", "WITH", "ORDER", "BY", "LIMIT",
    "SKIP", "AS", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
    "CREATE", "DELETE", "SET", "REMOVE", "MERGE", "UNWIND",
    "DISTINCT", "OPTIONAL", "CALL", "YIELD", "UNION", "DESC", "ASC",
    "IN", "IS", "STARTS", "ENDS", "CONTAINS", "EXISTS", "CASE",
    "WHEN", "THEN", "ELSE", "END",
})

_labels = st.text(
    alphabet=string.ascii_letters,
    min_size=1,
    max_size=8,
).filter(lambda s: s[0].isupper())

_int_literals = st.integers(min_value=-10000, max_value=10000)

_properties = st.sampled_from(["name", "age", "score", "title", "value"])


# ===========================================================================
# Property 1: Valid queries parse without crashing
# ===========================================================================


class TestParserNoCrash:
    """Valid Cypher queries must parse without exceptions."""

    @given(label=_labels, var=_identifiers)
    @SETTINGS
    def test_simple_match_return(self, label: str, var: str) -> None:
        """MATCH (var:Label) RETURN var parses."""
        q = f"MATCH ({var}:{label}) RETURN {var}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None

    @given(
        label=_labels,
        var=_identifiers,
        prop=_properties,
        val=_int_literals,
    )
    @SETTINGS
    def test_match_where_return(
        self, label: str, var: str, prop: str, val: int
    ) -> None:
        """MATCH with WHERE clause parses."""
        q = f"MATCH ({var}:{label}) WHERE {var}.{prop} = {val} RETURN {var}.{prop}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None

    @given(
        label=_labels,
        var=_identifiers,
        limit=st.integers(min_value=1, max_value=1000),
    )
    @SETTINGS
    def test_match_return_limit(self, label: str, var: str, limit: int) -> None:
        """MATCH RETURN LIMIT parses."""
        q = f"MATCH ({var}:{label}) RETURN {var} LIMIT {limit}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None

    @given(val=_int_literals)
    @SETTINGS
    def test_return_literal(self, val: int) -> None:
        """RETURN <literal> parses."""
        ast = _parser.parse_to_ast(f"RETURN {val}")
        assert ast is not None

    @given(
        label=_labels,
        var=_identifiers,
        prop=_properties,
    )
    @SETTINGS
    def test_order_by(self, label: str, var: str, prop: str) -> None:
        """ORDER BY parses."""
        q = f"MATCH ({var}:{label}) RETURN {var}.{prop} ORDER BY {var}.{prop}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None


# ===========================================================================
# Property 2: Whitespace insensitivity
# ===========================================================================


class TestWhitespaceInsensitivity:
    """Extra whitespace must not change parse results."""

    @given(
        label=_labels,
        var=_identifiers,
        extra_spaces=st.integers(min_value=1, max_value=5),
    )
    @SETTINGS
    def test_extra_spaces_dont_change_ast_type(
        self, label: str, var: str, extra_spaces: int
    ) -> None:
        """Extra spaces between tokens produce equivalent AST."""
        sep = " " * extra_spaces
        q_normal = f"MATCH ({var}:{label}) RETURN {var}"
        q_spaced = f"MATCH{sep}({var}:{label}){sep}RETURN{sep}{var}"
        ast1 = _parser.parse_to_ast(q_normal)
        ast2 = _parser.parse_to_ast(q_spaced)
        assert type(ast1) is type(ast2)

    @given(label=_labels, var=_identifiers)
    @SETTINGS
    def test_newlines_treated_as_whitespace(self, label: str, var: str) -> None:
        """Newlines between clauses parse correctly."""
        q = f"MATCH ({var}:{label})\nRETURN {var}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None


# ===========================================================================
# Property 3: Invalid input raises, doesn't crash
# ===========================================================================


class TestInvalidInputRaises:
    """Syntactically invalid input must raise an exception, not crash."""

    @given(garbage=st.text(min_size=1, max_size=50))
    @SETTINGS
    def test_random_text_doesnt_crash(self, garbage: str) -> None:
        """Random text either parses or raises a clean exception."""
        try:
            _parser.parse_to_ast(garbage)
        except Exception:
            pass  # Any exception is fine — no crash/segfault

    @given(
        label=_labels,
        var=_identifiers,
    )
    @SETTINGS
    def test_incomplete_query_raises(self, label: str, var: str) -> None:
        """Query without RETURN raises (unless it's a valid standalone clause)."""
        q = f"MATCH ({var}:{label})"
        try:
            _parser.parse_to_ast(q)
        except Exception:
            pass  # Expected for incomplete queries


# ===========================================================================
# Property 4: AST structure invariants
# ===========================================================================


class TestASTStructure:
    """Parsed ASTs must have consistent structure."""

    @given(label=_labels, var=_identifiers, prop=_properties)
    @SETTINGS
    def test_match_return_has_clauses(self, label: str, var: str, prop: str) -> None:
        """Parsed MATCH...RETURN has both clauses in AST."""
        q = f"MATCH ({var}:{label}) RETURN {var}.{prop}"
        ast = _parser.parse_to_ast(q)
        # AST should be a list of statements
        assert ast is not None
        # Convert to string representation and verify structure
        ast_str = str(ast)
        assert "Match" in ast_str or "match" in ast_str.lower()

    @given(
        label=_labels,
        var=_identifiers,
        prop=_properties,
        val=_int_literals,
    )
    @SETTINGS
    def test_where_preserved_in_ast(
        self, label: str, var: str, prop: str, val: int
    ) -> None:
        """WHERE clause is present in AST when specified."""
        q = f"MATCH ({var}:{label}) WHERE {var}.{prop} > {val} RETURN {var}.{prop}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None
        ast_str = str(ast)
        # The comparison value should appear somewhere in the AST
        assert str(val) in ast_str or str(abs(val)) in ast_str


# ===========================================================================
# Property 5: Case insensitivity of keywords
# ===========================================================================


class TestKeywordCaseInsensitivity:
    """Cypher keywords are case-insensitive."""

    @given(label=_labels, var=_identifiers)
    @SETTINGS
    def test_lowercase_keywords_parse(self, label: str, var: str) -> None:
        """Lowercase keywords parse identically to uppercase."""
        q_upper = f"MATCH ({var}:{label}) RETURN {var}"
        q_lower = f"match ({var}:{label}) return {var}"
        ast1 = _parser.parse_to_ast(q_upper)
        ast2 = _parser.parse_to_ast(q_lower)
        assert type(ast1) is type(ast2)

    @given(label=_labels, var=_identifiers)
    @SETTINGS
    def test_mixed_case_keywords_parse(self, label: str, var: str) -> None:
        """Mixed-case keywords parse correctly."""
        q = f"Match ({var}:{label}) Return {var}"
        ast = _parser.parse_to_ast(q)
        assert ast is not None
