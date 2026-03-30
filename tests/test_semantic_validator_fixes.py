"""TDD tests for two semantic validator false-positive bugs.

Bug 1 — node variable extraction with property predicates:
  ``MATCH (n:Person {id: id})`` reported ``n`` as undefined because
  ``find_data("variable_name")`` (Lark post-order) returned the property
  value reference ``id`` before the node variable ``n``, so
  ``_extract_variable_from_node_pattern`` defined the wrong name.

Bug 2 — ORDER BY aliases undefined:
  ``RETURN a.name AS since ORDER BY since DESC`` reported ``since`` as
  undefined because RETURN aliases were never added to the current scope,
  so the ORDER BY reference was "used but not defined".

TDD: all tests written before the fix.
"""

from __future__ import annotations

import pytest
from pycypher.grammar_parser import GrammarParser
from pycypher.semantic_validator import ErrorSeverity, SemanticValidator


@pytest.fixture
def parser() -> GrammarParser:
    return GrammarParser()


@pytest.fixture
def validator() -> SemanticValidator:
    return SemanticValidator()


# ---------------------------------------------------------------------------
# Bug 1: node variable shadowed by property predicate variable reference
# ---------------------------------------------------------------------------


class TestNodePatternVariableExtraction:
    """MATCH (n:Label {prop: var}) — 'n' must be defined, not 'var'."""

    def test_match_with_property_predicate_no_false_positive(
        self,
        parser,
        validator,
    ) -> None:
        """MATCH (n:Person {id: id}) RETURN n — no false-positive for n."""
        q = "MATCH (n:Person {id: id}) RETURN n.name AS name"
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        # 'id' (the *value* in the predicate) is genuinely unbound here, but
        # 'n' (the node variable) is defined by MATCH and must NOT be flagged.
        undefined = {e.variable_name for e in errors}
        assert "n" not in undefined, (
            f"'n' falsely flagged as undefined. Errors: {errors}"
        )

    def test_match_defines_node_variable(self, parser, validator) -> None:
        """MATCH (n:Person) RETURN n — n must be defined with no errors."""
        q = "MATCH (n:Person) RETURN n.name AS name"
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert errors == [], errors

    def test_unwind_with_match_no_errors(self, parser, validator) -> None:
        """UNWIND + WITH + MATCH — n defined by MATCH must not be undefined."""
        q = (
            "UNWIND [1, 2, 3] AS id "
            "WITH id "
            "MATCH (n:Person {id: id}) "
            "RETURN n.name AS name"
        )
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        undefined = {e.variable_name for e in errors}
        assert "n" not in undefined, (
            f"'n' falsely flagged as undefined. Errors: {errors}"
        )

    def test_match_with_multiple_property_predicates(
        self,
        parser,
        validator,
    ) -> None:
        """MATCH (n:Person {age: a, name: b}) — n still correctly defined."""
        q = (
            "WITH 25 AS a, 'Alice' AS b "
            "MATCH (n:Person {age: a, name: b}) "
            "RETURN n.name AS name"
        )
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        undefined = {e.variable_name for e in errors}
        assert "n" not in undefined, (
            f"'n' falsely flagged as undefined. Errors: {errors}"
        )


# ---------------------------------------------------------------------------
# Bug 2: RETURN alias not in scope → ORDER BY false-positive
# ---------------------------------------------------------------------------


class TestReturnAliasInScope:
    """RETURN expr AS alias ORDER BY alias — alias must be in scope."""

    def test_order_by_return_alias_no_error(self, parser, validator) -> None:
        """ORDER BY a RETURN alias must not be flagged as undefined."""
        q = "MATCH (n:Person) RETURN n.name AS alias ORDER BY alias"
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert errors == [], f"False positives: {errors}"

    def test_order_by_multiple_aliases(self, parser, validator) -> None:
        """ORDER BY with two aliases from RETURN — no undefined."""
        q = (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "RETURN a.name AS person1, b.name AS person2 "
            "ORDER BY person1, person2"
        )
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert errors == [], f"False positives: {errors}"

    def test_order_by_direction_alias(self, parser, validator) -> None:
        """ORDER BY alias DESC — alias still valid."""
        q = "MATCH (n:Person) RETURN n.age AS years ORDER BY years DESC"
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert errors == [], f"False positives: {errors}"

    def test_complex_query_with_with_order_by(self, parser, validator) -> None:
        """Full complex query with two WITH + ORDER BY — zero errors."""
        q = (
            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
            "WHERE a.age > 25 "
            "WITH a, b, r "
            "WITH a, b, r WHERE b.active = true "
            "RETURN a.name AS person1, b.name AS person2, r.since AS since "
            "ORDER BY since DESC "
            "LIMIT 10"
        )
        tree = parser.parse(q)
        errors = validator.validate(tree)
        errors = [e for e in errors if e.severity == ErrorSeverity.ERROR]
        assert errors == [], f"False positives: {errors}"
