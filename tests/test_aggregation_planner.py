"""Unit tests for :mod:`pycypher.aggregation_planner`.

Tests the two public methods of ``AggregationPlanner``:

- ``contains_aggregation`` — recursive aggregation detection in AST trees.
- ``aggregate_items`` — three-mode projection (no-agg, full-table, grouped).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
from pycypher.aggregation_planner import AggregationPlanner
from pycypher.ast_models import (
    Comparison,
    CountStar,
    FunctionInvocation,
    IntegerLiteral,
    ListLiteral,
    Not,
    NullCheck,
    PropertyLookup,
    ReturnItem,
    Unary,
    Variable,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _func(name: str, args: list[Any] | None = None) -> FunctionInvocation:
    """Build a FunctionInvocation with the given name and arguments."""
    return FunctionInvocation(name=name, arguments=args or [])


def _prop(var_name: str, prop_name: str) -> PropertyLookup:
    """Build a PropertyLookup on a Variable."""
    return PropertyLookup(
        expression=Variable(name=var_name), property=prop_name
    )


def _return_item(expr: Any, alias: str) -> ReturnItem:
    """Build a ReturnItem with a preset alias."""
    return ReturnItem(expression=expr, alias=alias)


# ---------------------------------------------------------------------------
# contains_aggregation — base cases
# ---------------------------------------------------------------------------


class TestContainsAggregationBaseCases:
    """Test contains_aggregation for atomic / leaf expressions."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_none_expression(self) -> None:
        assert self.planner.contains_aggregation(None) is False

    def test_count_star(self) -> None:
        assert self.planner.contains_aggregation(CountStar()) is True

    def test_plain_variable(self) -> None:
        assert self.planner.contains_aggregation(Variable(name="n")) is False

    def test_integer_literal(self) -> None:
        assert (
            self.planner.contains_aggregation(IntegerLiteral(value=42))
            is False
        )

    def test_property_lookup(self) -> None:
        expr = _prop("p", "name")
        assert self.planner.contains_aggregation(expr) is False


# ---------------------------------------------------------------------------
# contains_aggregation — FunctionInvocation paths
# ---------------------------------------------------------------------------


class TestContainsAggregationFunctions:
    """Test contains_aggregation for FunctionInvocation nodes."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_known_aggregation_count(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("count", [Variable(name="n")])
            )
            is True
        )

    def test_known_aggregation_sum(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("sum", [_prop("p", "salary")])
            )
            is True
        )

    def test_known_aggregation_avg(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("avg", [_prop("p", "age")])
            )
            is True
        )

    def test_known_aggregation_collect(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("collect", [Variable(name="n")])
            )
            is True
        )

    def test_scalar_function_toupper(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("toUpper", [_prop("p", "name")])
            )
            is False
        )

    def test_scalar_function_tolower(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("toLower", [_prop("p", "name")])
            )
            is False
        )

    def test_unknown_function_treated_as_scalar(self) -> None:
        """Unknown functions are treated as scalar — contains_aggregation is False."""
        assert (
            self.planner.contains_aggregation(
                _func("myCustomFunc", [Variable(name="x")]),
            )
            is False
        )

    def test_graph_function_labels(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("labels", [Variable(name="n")])
            )
            is False
        )

    def test_graph_function_type(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("type", [Variable(name="r")])
            )
            is False
        )

    def test_graph_function_elementid(self) -> None:
        assert (
            self.planner.contains_aggregation(
                _func("elementid", [Variable(name="n")])
            )
            is False
        )


# ---------------------------------------------------------------------------
# contains_aggregation — dual-purpose min/max
# ---------------------------------------------------------------------------


class TestContainsAggregationDualPurpose:
    """Test min/max disambiguation: list-literal → scalar, else agg."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_min_with_property_is_aggregation(self) -> None:
        expr = _func("min", [_prop("p", "age")])
        assert self.planner.contains_aggregation(expr) is True

    def test_max_with_property_is_aggregation(self) -> None:
        expr = _func("max", [_prop("p", "salary")])
        assert self.planner.contains_aggregation(expr) is True

    def test_min_with_list_literal_is_scalar(self) -> None:
        list_lit = ListLiteral(
            value=[1, 2, 3],
            elements=[IntegerLiteral(value=i) for i in (1, 2, 3)],
        )
        expr = _func("min", [list_lit])
        assert self.planner.contains_aggregation(expr) is False

    def test_max_with_list_literal_is_scalar(self) -> None:
        list_lit = ListLiteral(
            value=[5, 10],
            elements=[IntegerLiteral(value=i) for i in (5, 10)],
        )
        expr = _func("max", [list_lit])
        assert self.planner.contains_aggregation(expr) is False

    def test_min_list_literal_containing_aggregation(self) -> None:
        """min([count(*)]) — list-literal path but element is an aggregation.

        The list-literal path recurses into the FunctionInvocation.arguments
        list.  However, CountStar is inside the ListLiteral.elements, not
        directly in the arguments list.  The recursion via
        _contains_aggregation_in_func_args iterates over _normalize_func_args
        which yields the ListLiteral itself — a non-matching AST type — so the
        result is False.
        """
        list_lit = ListLiteral(value=[], elements=[CountStar()])
        expr = _func("min", [list_lit])
        assert self.planner.contains_aggregation(expr) is False


# ---------------------------------------------------------------------------
# contains_aggregation — composite expressions
# ---------------------------------------------------------------------------


class TestContainsAggregationComposite:
    """Test aggregation detection through compound AST structures."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_binary_expression_left_agg(self) -> None:
        expr = Comparison(
            operator=">",
            left=_func("count", [Variable(name="n")]),
            right=IntegerLiteral(value=1),
        )
        assert self.planner.contains_aggregation(expr) is True

    def test_binary_expression_right_agg(self) -> None:
        expr = Comparison(
            operator="=",
            left=IntegerLiteral(value=0),
            right=_func("sum", [_prop("p", "x")]),
        )
        assert self.planner.contains_aggregation(expr) is True

    def test_binary_expression_no_agg(self) -> None:
        expr = Comparison(
            operator="<",
            left=_prop("p", "age"),
            right=IntegerLiteral(value=30),
        )
        assert self.planner.contains_aggregation(expr) is False

    def test_not_wrapping_aggregation(self) -> None:
        inner = _func("count", [Variable(name="n")])
        expr = Not(operand=inner)
        assert self.planner.contains_aggregation(expr) is True

    def test_not_wrapping_non_aggregation(self) -> None:
        expr = Not(operand=Variable(name="x"))
        assert self.planner.contains_aggregation(expr) is False

    def test_null_check_with_aggregation(self) -> None:
        inner = _func("sum", [_prop("p", "val")])
        expr = NullCheck(operator="IS NULL", operand=inner)
        assert self.planner.contains_aggregation(expr) is True

    def test_null_check_without_aggregation(self) -> None:
        expr = NullCheck(operator="IS NOT NULL", operand=Variable(name="x"))
        assert self.planner.contains_aggregation(expr) is False

    def test_unary_with_aggregation(self) -> None:
        inner = _func("avg", [_prop("p", "score")])
        expr = Unary(operator="-", operand=inner)
        assert self.planner.contains_aggregation(expr) is True

    def test_unary_without_aggregation(self) -> None:
        expr = Unary(operator="-", operand=_prop("p", "balance"))
        assert self.planner.contains_aggregation(expr) is False

    def test_nested_scalar_wrapping_aggregation(self) -> None:
        """toUpper(count(n)) — scalar function whose argument is an aggregation."""
        inner_agg = _func("count", [Variable(name="n")])
        expr = _func("toUpper", [inner_agg])
        assert self.planner.contains_aggregation(expr) is True


# ---------------------------------------------------------------------------
# aggregate_items — no aggregation (simple projection)
# ---------------------------------------------------------------------------


class TestAggregateItemsSimpleProjection:
    """When no items contain aggregation, delegate to _simple_projection."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_simple_projection_variables(self) -> None:
        """Simple projection of variables returns DataFrame with correct aliases."""
        frame = MagicMock()
        frame.type_registry = {}
        frame.bindings = pd.DataFrame({"x": [1, 2, 3]})

        evaluator_instance = MagicMock()

        def eval_side_effect(expr: Any) -> pd.Series:
            if isinstance(expr, Variable) and expr.name == "x":
                return pd.Series([1, 2, 3])
            return pd.Series([0, 0, 0])

        evaluator_instance.evaluate = eval_side_effect

        items = [_return_item(Variable(name="x"), "x")]

        with patch(
            "pycypher.binding_evaluator.BindingExpressionEvaluator",
            return_value=evaluator_instance,
        ):
            result = self.planner.aggregate_items(items, frame)

        assert list(result.columns) == ["x"]
        assert list(result["x"]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# aggregate_items — full-table aggregation
# ---------------------------------------------------------------------------


class TestAggregateItemsFullTable:
    """All items are aggregations → single aggregated row."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_full_table_count_star(self) -> None:
        frame = MagicMock()
        frame.type_registry = {}

        evaluator_instance = MagicMock()
        evaluator_instance.evaluate_aggregation = MagicMock(return_value=5)

        items = [_return_item(CountStar(), "cnt")]

        with patch(
            "pycypher.binding_evaluator.BindingExpressionEvaluator",
            return_value=evaluator_instance,
        ):
            result = self.planner.aggregate_items(items, frame)

        assert list(result.columns) == ["cnt"]
        assert len(result) == 1
        assert result["cnt"].iloc[0] == 5

    def test_full_table_multiple_aggregations(self) -> None:
        frame = MagicMock()
        frame.type_registry = {}

        call_count = 0

        def agg_side_effect(expr: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return 10  # count
            return 42.5  # avg

        evaluator_instance = MagicMock()
        evaluator_instance.evaluate_aggregation = agg_side_effect

        items = [
            _return_item(CountStar(), "cnt"),
            _return_item(_func("avg", [_prop("p", "age")]), "avg_age"),
        ]

        with patch(
            "pycypher.binding_evaluator.BindingExpressionEvaluator",
            return_value=evaluator_instance,
        ):
            result = self.planner.aggregate_items(items, frame)

        assert list(result.columns) == ["cnt", "avg_age"]
        assert len(result) == 1


# ---------------------------------------------------------------------------
# aggregate_items — grouped aggregation
# ---------------------------------------------------------------------------


class TestAggregateItemsGrouped:
    """Mixed agg + non-agg items → grouped aggregation."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_grouped_aggregation_basic(self) -> None:
        """Group by a single key with a count aggregation."""
        frame = MagicMock()
        frame.type_registry = {}
        frame.__len__ = MagicMock(return_value=4)
        frame.bindings = pd.DataFrame({"dept": ["eng", "eng", "hr", "hr"]})
        frame.filter = MagicMock()

        dept_series = pd.Series(["eng", "eng", "hr", "hr"])
        count_grouped_series = pd.Series([2, 2])

        evaluator_instance = MagicMock()
        evaluator_instance.evaluate = MagicMock(return_value=dept_series)
        evaluator_instance.evaluate_aggregation_grouped = MagicMock(
            return_value=count_grouped_series,
        )

        items = [
            _return_item(Variable(name="dept"), "dept"),
            _return_item(CountStar(), "cnt"),
        ]

        with patch(
            "pycypher.binding_evaluator.BindingExpressionEvaluator",
            return_value=evaluator_instance,
        ):
            result = self.planner.aggregate_items(items, frame)

        assert "dept" in result.columns
        assert "cnt" in result.columns
        assert len(result) == 2

    def test_grouped_aggregation_fallback(self) -> None:
        """When evaluate_aggregation_grouped returns None, fallback per-group evaluation is used."""
        frame = MagicMock()
        frame.type_registry = {}
        frame.__len__ = MagicMock(return_value=3)
        frame.bindings = pd.DataFrame({"grp": ["a", "a", "b"]})

        grp_series = pd.Series(["a", "a", "b"])
        sub_frame = MagicMock()

        evaluator_instance = MagicMock()
        evaluator_instance.evaluate = MagicMock(return_value=grp_series)
        # Return None to trigger fallback path
        evaluator_instance.evaluate_aggregation_grouped = MagicMock(
            return_value=None
        )

        frame.filter = MagicMock(return_value=sub_frame)

        sub_evaluator = MagicMock()
        sub_evaluator.evaluate_aggregation = MagicMock(return_value=99)

        items = [
            _return_item(Variable(name="grp"), "grp"),
            _return_item(CountStar(), "cnt"),
        ]

        # Patch BindingExpressionEvaluator to return different evaluators
        call_count = 0

        def evaluator_factory(f: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return evaluator_instance
            return sub_evaluator

        with patch(
            "pycypher.binding_evaluator.BindingExpressionEvaluator",
            side_effect=evaluator_factory,
        ):
            result = self.planner.aggregate_items(items, frame)

        assert "grp" in result.columns
        assert "cnt" in result.columns


# ---------------------------------------------------------------------------
# _contains_aggregation_in_func_args — argument normalisation
# ---------------------------------------------------------------------------


class TestContainsAggregationFuncArgs:
    """Test argument normalisation paths in _contains_aggregation_in_func_args."""

    def setup_method(self) -> None:
        self.planner = AggregationPlanner()

    def test_dict_arguments_with_aggregation(self) -> None:
        """FunctionInvocation.arguments as dict with 'arguments' key."""
        inner_agg = CountStar()
        expr = FunctionInvocation(
            name="tostring", arguments={"arguments": [inner_agg]}
        )
        assert self.planner.contains_aggregation(expr) is True

    def test_empty_arguments(self) -> None:
        expr = _func("rand")
        assert self.planner.contains_aggregation(expr) is False

    def test_none_arguments(self) -> None:
        expr = FunctionInvocation(name="rand", arguments=None)
        assert self.planner.contains_aggregation(expr) is False
