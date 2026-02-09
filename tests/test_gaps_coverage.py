"""Tests to cover gaps in grammar_parser and ast_models functionality.

Focuses on complex expressions: CASE, List Comprehension, and Quantifiers.
"""

import pytest
from pycypher.ast_models import (
    ASTConverter,
    CaseExpression,
    ListComprehension,
    Quantifier,
)
from pycypher.grammar_parser import GrammarParser


@pytest.fixture
def parser():
    return GrammarParser()


@pytest.fixture
def converter():
    return ASTConverter()


class TestComplexStructures:
    def test_case_expression_structure(self, parser, converter):
        """Test parsing and conversion of CASE WHEN expression."""
        query = "RETURN CASE WHEN n.age >= 18 THEN 'Adult' ELSE 'Minor' END AS status"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)

        # 1. Check Dictionary Structure
        # Traverse statements -> query_statement -> return -> body -> items -> item -> expression
        # Note: Exact path depends on wrapper classes, so we search/inspect carefully or use 'find' logic if implemented,
        # but here we'll assume basic structure based on transformer code read.

        # Accessing the first statement's return clause item
        stmt = ast_dict["statements"][0][0]  # QueryStatement
        ret_item = stmt["return"]["body"]["items"][0]
        expr_dict = ret_item["expression"]

        assert expr_dict["type"] == "SearchedCase"
        assert len(expr_dict["when"]) == 1
        assert expr_dict["when"][0]["type"] == "SearchedWhen"
        assert expr_dict["else"] is not None

        # 2. Check Pydantic Model Conversion
        typed_ast = converter.convert(ast_dict)

        # Navigation to the expression in Typed AST
        # Query -> clauses -> (Match/Return)
        # return clause is last
        return_clause = typed_ast.clauses[-1]
        typed_expr = return_clause.items[0].expression

        assert isinstance(typed_expr, CaseExpression)
        assert len(typed_expr.when_clauses) == 1
        assert typed_expr.else_expr is not None
        # Check values
        assert isinstance(typed_expr.when_clauses[0].result.name, str)
        assert typed_expr.when_clauses[0].result.name == "Adult"

    def test_list_comprehension_structure(self, parser, converter):
        """Test parsing and conversion of List Comprehension."""
        query = "RETURN [x IN [1,2,3] WHERE x > 1 | x*2] AS doubted"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)

        stmt = ast_dict["statements"][0][0]
        ret_item = stmt["return"]["body"]["items"][0]
        expr_dict = ret_item["expression"]

        # 1. Structural Check
        assert expr_dict["type"] == "ListComprehension"
        assert expr_dict["variable"] == "x"
        assert isinstance(expr_dict["in"], list)
        assert expr_dict["where"] is not None
        assert expr_dict["projection"] is not None

        # 2. Conversion Check
        typed_ast = converter.convert(ast_dict)
        return_clause = typed_ast.clauses[-1]
        typed_expr = return_clause.items[0].expression

        assert isinstance(typed_expr, ListComprehension)
        assert typed_expr.variable.name == "x"
        assert typed_expr.where is not None
        assert typed_expr.map_expr is not None

    def test_quantifier_any_structure(self, parser, converter):
        """Test parsing and conversion of ALL quantifier."""
        query = "MATCH (n) WHERE ALL(x IN [1,2,3] WHERE x > 2) RETURN n"
        tree = parser.parse(query)
        ast_dict = parser.transformer.transform(tree)

        stmt = ast_dict["statements"][0][0]
        # MATCH clause is first
        match_clause = stmt["clauses"][0]
        # WHERE clause inside match
        where_cond = match_clause["where"]["condition"]

        assert where_cond["type"] == "Quantifier"
        assert where_cond["quantifier"] == "ALL"
        assert where_cond["variable"] == "x"

        # Conversion
        typed_ast = converter.convert(ast_dict)
        # Use flattened clauses
        typed_match = typed_ast.clauses[0]
        # Match.where might be the expression directly or a Where object?
        # Let's assume it's the expression if the previous error suggests typed_match.where IS the quantifier.
        # But wait, usually Where is a node enveloping the condition.
        # Let's try inspecting type at runtime or safer navigation.
        # If Match model has field `where: Optional[Expression]`, then it's direct.
        # If `where: Optional[Where]`, then it wraps.
        # Traceback said `Quantifier object has no attribute 'condition'`, called on `typed_match.where.condition`.
        # This implies `typed_match.where` was the Quantifier.
        typed_quantifier = typed_match.where

        # If it is wrapped in Comparison (e.g. implicitly = true), we might need to dig.
        # But let's assume direct for now based on error.

        assert isinstance(typed_quantifier, Quantifier)
        assert (
            typed_quantifier.quantifier == "ALL"
        )  # Field is 'quantifier' based on repr
        assert typed_quantifier.variable.name == "x"
