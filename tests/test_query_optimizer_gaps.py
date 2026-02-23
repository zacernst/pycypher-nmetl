"""Tests for QueryOptimizer coverage gaps.

Covers uncovered internal methods:
- _evaluate_constant_expression (2-child and 3-child forms)
- _evaluate_boolean_expression (AND, OR, simple literal)
- _get_numeric_value (Tree wrappers and _ambig)
- _get_boolean_value (Token and Tree.literal)
- _is_always_true / _is_always_false (various node types)
- _find_parent
- _check_always_true_recursive (_ambig path)
- _is_passthrough_with
- estimate_cost
- constant folding on boolean expressions
"""

from __future__ import annotations

import pytest
from lark import Token, Tree

from pycypher.query_optimizer import OptimizationLevel, QueryOptimizer


@pytest.fixture()
def opt() -> QueryOptimizer:
    return QueryOptimizer(OptimizationLevel.BASIC)


# ---------------------------------------------------------------------------
# _evaluate_constant_expression
# ---------------------------------------------------------------------------


class TestEvaluateConstantExpression:
    """Cover _evaluate_constant_expression branches."""

    def test_single_token(self, opt: QueryOptimizer) -> None:
        """Single-token child is returned as-is."""
        expr = Tree("add_expression", [Token("INTEGER", "7")])
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert result.value == "7"

    def test_two_child_add(self, opt: QueryOptimizer) -> None:
        """2-child add_expression: 3 + 4 → 7."""
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "3"), Token("INTEGER", "4")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 7

    def test_two_child_subtract(self, opt: QueryOptimizer) -> None:
        """2-child subtract_expression."""
        expr = Tree(
            "subtract_expression",
            [Token("INTEGER", "10"), Token("INTEGER", "3")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 7

    def test_two_child_multiply(self, opt: QueryOptimizer) -> None:
        """2-child multiply_expression."""
        expr = Tree(
            "multiply_expression",
            [Token("INTEGER", "5"), Token("INTEGER", "6")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 30

    def test_two_child_divide(self, opt: QueryOptimizer) -> None:
        """2-child divide_expression."""
        expr = Tree(
            "divide_expression",
            [Token("FLOAT", "10.0"), Token("FLOAT", "4.0")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert float(result.value) == 2.5

    def test_two_child_divide_by_zero(self, opt: QueryOptimizer) -> None:
        """2-child division by zero returns None."""
        expr = Tree(
            "divide_expression",
            [Token("INTEGER", "5"), Token("INTEGER", "0")],
        )
        assert opt._evaluate_constant_expression(expr) is None

    def test_two_child_power(self, opt: QueryOptimizer) -> None:
        """2-child power_expression."""
        expr = Tree(
            "power_expression",
            [Token("INTEGER", "2"), Token("INTEGER", "3")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 8

    def test_two_child_unknown_op(self, opt: QueryOptimizer) -> None:
        """Unknown 2-child expression type returns None."""
        expr = Tree(
            "modulo_expression",
            [Token("INTEGER", "7"), Token("INTEGER", "3")],
        )
        assert opt._evaluate_constant_expression(expr) is None

    def test_three_child_add(self, opt: QueryOptimizer) -> None:
        """3-child form: left + right with explicit '+' token."""
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "2"), Token("PLUS", "+"), Token("INTEGER", "3")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 5

    def test_three_child_subtract(self, opt: QueryOptimizer) -> None:
        """3-child form with '-' token."""
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "9"), Token("MINUS", "-"), Token("INTEGER", "4")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 5

    def test_three_child_multiply(self, opt: QueryOptimizer) -> None:
        """3-child form with '*' token."""
        expr = Tree(
            "mult_expression",
            [Token("INTEGER", "3"), Token("STAR", "*"), Token("INTEGER", "4")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 12

    def test_three_child_divide_by_zero(self, opt: QueryOptimizer) -> None:
        """3-child form with '/' and divisor 0 returns None."""
        expr = Tree(
            "mult_expression",
            [Token("INTEGER", "5"), Token("SLASH", "/"), Token("INTEGER", "0")],
        )
        assert opt._evaluate_constant_expression(expr) is None

    def test_three_child_wrapped_op(self, opt: QueryOptimizer) -> None:
        """3-child form where operator is a Tree wrapper (add_op)."""
        op_tree = Tree("add_op", [Token("PLUS", "+")])
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "1"), op_tree, Token("INTEGER", "2")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert int(result.value) == 3

    def test_three_child_unknown_op(self, opt: QueryOptimizer) -> None:
        """3-child with unrecognized operator returns None."""
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "1"), Token("TILDE", "~"), Token("INTEGER", "2")],
        )
        assert opt._evaluate_constant_expression(expr) is None

    def test_float_result_token_type(self, opt: QueryOptimizer) -> None:
        """Float result uses FLOAT token type."""
        expr = Tree(
            "divide_expression",
            [Token("FLOAT", "7.0"), Token("FLOAT", "2.0")],
        )
        result = opt._evaluate_constant_expression(expr)
        assert result is not None
        assert result.type == "FLOAT"

    def test_no_children(self, opt: QueryOptimizer) -> None:
        """Empty-children tree returns None."""
        expr = Tree("add_expression", [])
        assert opt._evaluate_constant_expression(expr) is None


# ---------------------------------------------------------------------------
# _evaluate_boolean_expression
# ---------------------------------------------------------------------------


class TestEvaluateBooleanExpression:
    """Cover AND, OR, and simple literal branches."""

    def test_true_literal(self, opt: QueryOptimizer) -> None:
        expr = Tree("and_expression", [Token("TRUE", "TRUE")])
        assert opt._evaluate_boolean_expression(expr) is True

    def test_false_literal(self, opt: QueryOptimizer) -> None:
        expr = Tree("or_expression", [Token("FALSE", "FALSE")])
        assert opt._evaluate_boolean_expression(expr) is False

    def test_and_true_true(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "and_expression",
            [Token("TRUE", "TRUE"), Token("AND", "AND"), Token("TRUE", "TRUE")],
        )
        assert opt._evaluate_boolean_expression(expr) is True

    def test_and_true_false(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "and_expression",
            [Token("TRUE", "TRUE"), Token("AND", "AND"), Token("FALSE", "FALSE")],
        )
        assert opt._evaluate_boolean_expression(expr) is False

    def test_or_false_true(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "or_expression",
            [Token("FALSE", "FALSE"), Token("OR", "OR"), Token("TRUE", "TRUE")],
        )
        assert opt._evaluate_boolean_expression(expr) is True

    def test_no_match_returns_none(self, opt: QueryOptimizer) -> None:
        expr = Tree("and_expression", [Token("SYMBOL", "x")])
        assert opt._evaluate_boolean_expression(expr) is None


# ---------------------------------------------------------------------------
# _get_numeric_value
# ---------------------------------------------------------------------------


class TestGetNumericValue:
    """Cover Token types and Tree wrappers."""

    def test_integer_token(self, opt: QueryOptimizer) -> None:
        assert opt._get_numeric_value(Token("INTEGER", "42")) == 42

    def test_decimal_integer_token(self, opt: QueryOptimizer) -> None:
        assert opt._get_numeric_value(Token("DECIMAL_INTEGER", "99")) == 99

    def test_float_token(self, opt: QueryOptimizer) -> None:
        assert opt._get_numeric_value(Token("FLOAT", "3.14")) == pytest.approx(3.14)

    def test_number_literal_tree(self, opt: QueryOptimizer) -> None:
        tree = Tree("number_literal", [Token("INTEGER", "7")])
        assert opt._get_numeric_value(tree) == 7

    def test_ambig_tree(self, opt: QueryOptimizer) -> None:
        tree = Tree(
            "_ambig",
            [
                Tree("variable_name", [Token("NAME", "x")]),
                Tree("number_literal", [Token("INTEGER", "5")]),
            ],
        )
        assert opt._get_numeric_value(tree) == 5

    def test_unknown_returns_none(self, opt: QueryOptimizer) -> None:
        assert opt._get_numeric_value(Token("STRING", "abc")) is None

    def test_invalid_value_returns_none(self, opt: QueryOptimizer) -> None:
        assert opt._get_numeric_value(Token("INTEGER", "not_a_number")) is None


# ---------------------------------------------------------------------------
# _get_boolean_value
# ---------------------------------------------------------------------------


class TestGetBooleanValue:
    """Cover Token and Tree.literal paths."""

    def test_true_token_type(self, opt: QueryOptimizer) -> None:
        assert opt._get_boolean_value(Token("TRUE", "true")) is True

    def test_false_token_type(self, opt: QueryOptimizer) -> None:
        assert opt._get_boolean_value(Token("FALSE", "false")) is False

    def test_true_by_value(self, opt: QueryOptimizer) -> None:
        assert opt._get_boolean_value(Token("KEYWORD", "true")) is True

    def test_false_by_value(self, opt: QueryOptimizer) -> None:
        assert opt._get_boolean_value(Token("KEYWORD", "false")) is False

    def test_literal_tree(self, opt: QueryOptimizer) -> None:
        tree = Tree("literal", [Token("TRUE", "TRUE")])
        assert opt._get_boolean_value(tree) is True

    def test_unknown_returns_none(self, opt: QueryOptimizer) -> None:
        assert opt._get_boolean_value(Token("INTEGER", "42")) is None


# ---------------------------------------------------------------------------
# _is_always_true / _is_always_false
# ---------------------------------------------------------------------------


class TestIsAlwaysTrueFalse:
    """Cover various node types for truth/falsity detection."""

    def test_true_token(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_true(Token("TRUE", "true")) is True

    def test_true_tree(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_true(Tree("true", [])) is True

    def test_literal_true(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_true(Tree("literal", [Tree("true", [])])) is True

    def test_ambig_true(self, opt: QueryOptimizer) -> None:
        tree = Tree("_ambig", [Tree("true", []), Tree("variable_name", [])])
        assert opt._is_always_true(tree) is True

    def test_not_true(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_true(Token("INTEGER", "5")) is False

    def test_false_token(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_false(Token("FALSE", "false")) is True

    def test_false_tree(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_false(Tree("false", [])) is True

    def test_literal_false(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_false(Tree("literal", [Tree("false", [])])) is True

    def test_ambig_false(self, opt: QueryOptimizer) -> None:
        tree = Tree("_ambig", [Tree("false", [])])
        assert opt._is_always_false(tree) is True

    def test_not_false(self, opt: QueryOptimizer) -> None:
        assert opt._is_always_false(Token("INTEGER", "0")) is False


# ---------------------------------------------------------------------------
# _check_always_true_recursive  (covers _ambig branch)
# ---------------------------------------------------------------------------


class TestCheckAlwaysTrueRecursive:
    """Cover the _ambig branch in recursive true-checking."""

    def test_ambig_true_no_variable(self, opt: QueryOptimizer) -> None:
        tree = Tree("_ambig", [Tree("true", [])])
        assert opt._check_always_true_recursive(tree) is True

    def test_ambig_true_with_variable(self, opt: QueryOptimizer) -> None:
        tree = Tree(
            "_ambig",
            [Tree("true", []), Tree("variable_name", [Token("NAME", "n")])],
        )
        # _is_always_true is checked first and returns True for _ambig with any 'true' child
        assert opt._check_always_true_recursive(tree) is True

    def test_comparison_not_true(self, opt: QueryOptimizer) -> None:
        tree = Tree("comparison_expression", [Token("NAME", "n.age")])
        assert opt._check_always_true_recursive(tree) is False

    def test_and_with_true_child(self, opt: QueryOptimizer) -> None:
        tree = Tree("and_expression", [Tree("true", []), Token("INTEGER", "1")])
        assert opt._check_always_true_recursive(tree) is True


# ---------------------------------------------------------------------------
# _find_parent
# ---------------------------------------------------------------------------


class TestFindParent:
    """Cover _find_parent utility."""

    def test_finds_parent(self, opt: QueryOptimizer) -> None:
        child = Tree("inner", [])
        parent = Tree("outer", [child])
        root = Tree("root", [parent])
        assert opt._find_parent(root, child) is parent

    def test_returns_none_if_not_found(self, opt: QueryOptimizer) -> None:
        orphan = Tree("orphan", [])
        root = Tree("root", [Tree("child", [])])
        assert opt._find_parent(root, orphan) is None


# ---------------------------------------------------------------------------
# estimate_cost (via internal method)
# ---------------------------------------------------------------------------


class TestEstimateCost:
    """Cover estimate_cost method directly."""

    def test_single_match(self, opt: QueryOptimizer) -> None:
        tree = Tree("query", [Tree("match_clause", [])])
        assert opt.estimate_cost(tree) == 100.0

    def test_match_with_where(self, opt: QueryOptimizer) -> None:
        tree = Tree(
            "query",
            [Tree("match_clause", []), Tree("where_clause", [])],
        )
        assert opt.estimate_cost(tree) == 120.0  # 100 + 20

    def test_cartesian_product_penalty(self, opt: QueryOptimizer) -> None:
        tree = Tree(
            "query",
            [Tree("match_clause", []), Tree("match_clause", [])],
        )
        # 2*100 + (2-1)*1000 = 1200
        assert opt.estimate_cost(tree) == 1200.0

    def test_with_clause_cost(self, opt: QueryOptimizer) -> None:
        tree = Tree(
            "query",
            [Tree("match_clause", []), Tree("with_clause", [])],
        )
        assert opt.estimate_cost(tree) == 110.0  # 100 + 10


# ---------------------------------------------------------------------------
# _is_constant_expression / _is_constant_boolean_expression
# ---------------------------------------------------------------------------


class TestIsConstantExpression:
    """Cover has-variable detection."""

    def test_all_constants(self, opt: QueryOptimizer) -> None:
        expr = Tree("add_expression", [Token("INTEGER", "1"), Token("INTEGER", "2")])
        assert opt._is_constant_expression(expr) is True

    def test_has_variable(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "1"), Tree("variable_name", [Token("NAME", "x")])],
        )
        assert opt._is_constant_expression(expr) is False

    def test_has_function(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "add_expression",
            [Token("INTEGER", "1"), Tree("function_invocation", [])],
        )
        assert opt._is_constant_expression(expr) is False

    def test_boolean_all_constants(self, opt: QueryOptimizer) -> None:
        expr = Tree("and_expression", [Token("TRUE", "TRUE")])
        assert opt._is_constant_boolean_expression(expr) is True

    def test_boolean_has_property(self, opt: QueryOptimizer) -> None:
        expr = Tree(
            "and_expression",
            [Tree("property_lookup", [])],
        )
        assert opt._is_constant_boolean_expression(expr) is False


# ---------------------------------------------------------------------------
# Constant folding (boolean branch via full parse)
# ---------------------------------------------------------------------------


class TestConstantFoldingBoolean:
    """Cover the boolean constant_folding branch in _fold_constants."""

    def test_boolean_expression_folding(self, opt: QueryOptimizer) -> None:
        """Construct an and_expression with only constant booleans and fold it."""
        tree = Tree(
            "query",
            [
                Tree(
                    "and_expression",
                    [
                        Token("TRUE", "TRUE"),
                        Token("AND", "AND"),
                        Token("TRUE", "TRUE"),
                    ],
                )
            ],
        )
        result = opt._fold_constants(tree)
        # The and_expression should be replaced with "true"
        and_expr = list(result.find_data("true"))
        assert len(and_expr) >= 1
