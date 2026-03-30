"""TDD tests for the query_validator module.

Sprint 5, Phase 3.2: Combined Query Validation — validates combined
Cypher query ASTs for structural integrity, variable binding consistency,
and clause ordering rules.

RED phase: interface contracts, syntax validation, binding checks,
clause ordering, and error diagnostics.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Interface contract tests
# ---------------------------------------------------------------------------


class TestQueryValidatorInterfaceContract:
    """CombinedQueryValidator must expose a well-defined public API."""

    def test_validator_has_validate_method(self) -> None:
        """CombinedQueryValidator exposes a validate() method."""
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        assert callable(validator.validate)

    def test_validate_returns_validation_result(self) -> None:
        """validate() returns a ValidationResult."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import (
            CombinedQueryValidator,
            ValidationResult,
        )

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        assert isinstance(result, ValidationResult)

    def test_validation_result_has_is_valid(self) -> None:
        """ValidationResult has an is_valid boolean property."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        assert isinstance(result.is_valid, bool)

    def test_validation_result_has_errors_list(self) -> None:
        """ValidationResult has an errors list."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        assert isinstance(result.errors, list)


# ---------------------------------------------------------------------------
# Valid query acceptance tests
# ---------------------------------------------------------------------------


class TestValidQueryAcceptance:
    """Validator must accept well-formed combined queries."""

    def test_accept_simple_match_return(self) -> None:
        """Simple MATCH...RETURN is valid."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        assert result.is_valid

    def test_accept_create_with_star_match_return(self) -> None:
        """CREATE...WITH *...MATCH...RETURN is valid combined pattern."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'}) WITH * MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        assert result.is_valid

    def test_accept_multiple_with_star_separators(self) -> None:
        """Multiple WITH * separators in a chain are valid."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "CREATE (p:Person {name: 'Alice'}) "
            "WITH * "
            "MATCH (p:Person) CREATE (e:Employee {n: p.name}) "
            "WITH * "
            "MATCH (e:Employee) RETURN e.n",
        )
        result = validator.validate(ast)
        assert result.is_valid

    def test_accept_standalone_create(self) -> None:
        """Standalone CREATE (no RETURN) is valid."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "CREATE (n:Person {name: 'Alice'})",
        )
        result = validator.validate(ast)
        assert result.is_valid


# ---------------------------------------------------------------------------
# Clause ordering validation tests
# ---------------------------------------------------------------------------


class TestClauseOrderingValidation:
    """Validator must detect invalid clause orderings."""

    def test_reject_return_before_match(self) -> None:
        """RETURN before MATCH is invalid ordering."""
        from pycypher.ast_models import (
            Match,
            NodePattern,
            Pattern,
            PatternPath,
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        # Manually construct invalid AST: RETURN then MATCH
        bad_query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ],
                ),
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="n"),
                                        labels=["Person"],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
            ],
        )
        result = validator.validate(bad_query)
        assert not result.is_valid
        assert len(result.errors) > 0

    def test_reject_multiple_returns(self) -> None:
        """Multiple RETURN clauses are invalid."""
        from pycypher.ast_models import (
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        bad_query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ],
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="m")),
                    ],
                ),
            ],
        )
        result = validator.validate(bad_query)
        assert not result.is_valid

    def test_return_must_be_last_clause(self) -> None:
        """RETURN must be the final clause if present."""
        from pycypher.ast_models import (
            Create,
            Match,
            NodePattern,
            Pattern,
            PatternPath,
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        # MATCH, RETURN, CREATE — RETURN is not last
        bad_query = Query(
            clauses=[
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="n"),
                                        labels=["Person"],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ],
                ),
                Create(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="m"),
                                        labels=["Company"],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
            ],
        )
        result = validator.validate(bad_query)
        assert not result.is_valid


# ---------------------------------------------------------------------------
# Empty/degenerate query handling
# ---------------------------------------------------------------------------


class TestDegenerateQueries:
    """Validator must handle edge cases gracefully."""

    def test_empty_query_is_valid(self) -> None:
        """Query with no clauses is valid (no-op)."""
        from pycypher.ast_models import Query
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        result = validator.validate(Query(clauses=[]))
        assert result.is_valid

    def test_validation_result_str_representation(self) -> None:
        """ValidationResult has a useful string representation."""
        from pycypher.ast_models import ASTConverter
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        ast = ASTConverter.from_cypher(
            "MATCH (n:Person) RETURN n.name",
        )
        result = validator.validate(ast)
        text = str(result)
        assert isinstance(text, str)
        assert len(text) > 0


# ---------------------------------------------------------------------------
# Error diagnostics tests
# ---------------------------------------------------------------------------


class TestErrorDiagnostics:
    """Validation errors must provide actionable diagnostics."""

    def test_error_has_message(self) -> None:
        """Validation errors include a human-readable message."""
        from pycypher.ast_models import (
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        bad_query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ],
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="m")),
                    ],
                ),
            ],
        )
        result = validator.validate(bad_query)
        assert not result.is_valid
        assert any("RETURN" in str(e) for e in result.errors)

    def test_multiple_errors_collected(self) -> None:
        """Validator collects all errors, not just the first."""
        from pycypher.ast_models import (
            Match,
            NodePattern,
            Pattern,
            PatternPath,
            Query,
            Return,
            ReturnItem,
            Variable,
        )
        from pycypher.query_validator import CombinedQueryValidator

        validator = CombinedQueryValidator()
        # Two RETURN clauses (error), RETURN not last (error)
        bad_query = Query(
            clauses=[
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="n")),
                    ],
                ),
                Return(
                    items=[
                        ReturnItem(expression=Variable(name="m")),
                    ],
                ),
                Match(
                    pattern=Pattern(
                        paths=[
                            PatternPath(
                                elements=[
                                    NodePattern(
                                        variable=Variable(name="x"),
                                        labels=["Person"],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
            ],
        )
        result = validator.validate(bad_query)
        assert not result.is_valid
        assert len(result.errors) >= 2
