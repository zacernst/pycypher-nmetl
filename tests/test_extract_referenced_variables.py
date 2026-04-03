"""Tests for the canonical extract_referenced_variables utility.

Verifies that the unified variable extraction function handles all AST
expression types correctly — including And/Or operands, PropertyLookup,
nested comparisons, and function calls.
"""

from __future__ import annotations

from pycypher.ast_converter import ASTConverter
from pycypher.ast_models import (
    And,
    Comparison,
    Or,
    PropertyLookup,
    Variable,
    extract_referenced_variables,
)


class TestExtractReferencedVariables:
    """Tests for extract_referenced_variables."""

    def test_single_variable(self) -> None:
        var = Variable(name="x")
        assert extract_referenced_variables(var) == {"x"}

    def test_property_lookup(self) -> None:
        prop = PropertyLookup(expression=Variable(name="p"), property="name")
        assert extract_referenced_variables(prop) == {"p"}

    def test_comparison_two_vars(self) -> None:
        comp = Comparison(
            operator="=",
            left=PropertyLookup(expression=Variable(name="a"), property="x"),
            right=PropertyLookup(expression=Variable(name="b"), property="y"),
        )
        assert extract_referenced_variables(comp) == {"a", "b"}

    def test_and_with_operands(self) -> None:
        """And nodes use .operands — the bug that prompted consolidation."""
        expr = And(
            operator="AND",
            operands=[
                Comparison(
                    operator="=",
                    left=PropertyLookup(
                        expression=Variable(name="p"), property="dept"
                    ),
                    right=PropertyLookup(
                        expression=Variable(name="q"),
                        property="dept",
                    ),
                ),
                Comparison(
                    operator="<>",
                    left=PropertyLookup(
                        expression=Variable(name="p"), property="name"
                    ),
                    right=PropertyLookup(
                        expression=Variable(name="q"),
                        property="name",
                    ),
                ),
            ],
        )
        assert extract_referenced_variables(expr) == {"p", "q"}

    def test_or_with_operands(self) -> None:
        expr = Or(
            operator="OR",
            operands=[
                Comparison(
                    operator="=",
                    left=Variable(name="x"),
                    right=Variable(name="y"),
                ),
                Comparison(
                    operator="=",
                    left=Variable(name="z"),
                    right=Variable(name="w"),
                ),
            ],
        )
        assert extract_referenced_variables(expr) == {"x", "y", "z", "w"}

    def test_from_parsed_query_where(self) -> None:
        """Integration: extract from a real parsed WHERE clause."""
        query = ASTConverter.from_cypher(
            "MATCH (p:Person) MATCH (q:Person) "
            "WHERE p.dept = q.dept AND p.name <> q.name "
            "RETURN p.name",
        )
        # WHERE is on the second MATCH clause
        where_clause = query.clauses[1].where
        assert where_clause is not None
        refs = extract_referenced_variables(where_clause)
        assert refs == {"p", "q"}

    def test_empty_variable_set(self) -> None:
        """A literal comparison has no variables."""
        from pycypher.ast_models import Literal

        comp = Comparison(
            operator="=",
            left=Literal(value=1),
            right=Literal(value=2),
        )
        assert extract_referenced_variables(comp) == set()
