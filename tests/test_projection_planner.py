"""Unit tests for :mod:`pycypher.projection_planner`.

Tests the five public methods of ``ProjectionPlanner``:

- ``infer_alias`` — alias inference from AST expression nodes.
- ``qualify_alias`` — fully-qualified ``var.prop`` alias generation.
- ``apply_projection_modifiers`` — DISTINCT, ORDER BY, SKIP, LIMIT.
- ``return_from_frame`` — RETURN clause evaluation.
- ``with_to_binding_frame`` — WITH clause evaluation.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
from pycypher.ast_models import (
    CountStar,
    FunctionInvocation,
    IntegerLiteral,
    PropertyLookup,
    ReturnItem,
    Variable,
)
from pycypher.projection_planner import ProjectionPlanner

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_planner(
    *,
    renderer_render: Any = None,
    where_fn: Any = None,
) -> ProjectionPlanner:
    """Build a ProjectionPlanner with mocked dependencies."""
    agg_planner = MagicMock()
    renderer = MagicMock()
    if renderer_render is not None:
        renderer.render = renderer_render
    else:
        renderer.render = MagicMock(return_value=None)
    if where_fn is None:
        where_fn = MagicMock()
    return ProjectionPlanner(agg_planner, renderer, where_fn)


def _prop(var_name: str, prop_name: str) -> PropertyLookup:
    return PropertyLookup(expression=Variable(name=var_name), property=prop_name)


def _make_frame(
    df: pd.DataFrame, type_registry: dict[str, str] | None = None,
) -> MagicMock:
    """Create a mock BindingFrame."""
    frame = MagicMock()
    frame.bindings = df
    frame.type_registry = type_registry or {}
    frame.context = MagicMock()
    return frame


# ---------------------------------------------------------------------------
# infer_alias
# ---------------------------------------------------------------------------


class TestInferAlias:
    """Test alias inference from various expression types."""

    def test_variable_returns_name(self) -> None:
        planner = _make_planner()
        assert planner.infer_alias(Variable(name="person")) == "person"

    def test_property_lookup_returns_property(self) -> None:
        planner = _make_planner()
        expr = _prop("p", "name")
        assert planner.infer_alias(expr) == "name"

    def test_other_expression_delegates_to_renderer(self) -> None:
        planner = _make_planner(renderer_render=lambda e: "count(*)")
        expr = CountStar()
        assert planner.infer_alias(expr) == "count(*)"

    def test_renderer_returns_none(self) -> None:
        planner = _make_planner(renderer_render=lambda e: None)
        expr = IntegerLiteral(value=42)
        assert planner.infer_alias(expr) is None

    def test_function_invocation_rendered(self) -> None:
        planner = _make_planner(renderer_render=lambda e: "toUpper(name)")
        expr = FunctionInvocation(name="toUpper", arguments=[_prop("p", "name")])
        assert planner.infer_alias(expr) == "toUpper(name)"


# ---------------------------------------------------------------------------
# qualify_alias
# ---------------------------------------------------------------------------


class TestQualifyAlias:
    """Test fully-qualified alias generation."""

    def test_property_lookup_on_variable(self) -> None:
        planner = _make_planner()
        expr = _prop("p", "name")
        assert planner.qualify_alias(expr) == "p.name"

    def test_property_lookup_on_non_variable(self) -> None:
        """PropertyLookup on non-Variable → None."""
        planner = _make_planner()
        # PropertyLookup whose expression is another PropertyLookup (nested)
        inner = _prop("p", "address")
        outer = PropertyLookup(expression=inner, property="city")
        assert planner.qualify_alias(outer) is None

    def test_non_property_lookup(self) -> None:
        planner = _make_planner()
        assert planner.qualify_alias(Variable(name="x")) is None

    def test_count_star(self) -> None:
        planner = _make_planner()
        assert planner.qualify_alias(CountStar()) is None


# ---------------------------------------------------------------------------
# apply_projection_modifiers
# ---------------------------------------------------------------------------


class TestApplyProjectionModifiers:
    """Test DISTINCT, ORDER BY, SKIP, LIMIT modifiers."""

    def test_distinct(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [1, 1, 2, 2, 3]})
        clause = MagicMock()
        clause.distinct = True
        clause.order_by = None
        clause.skip = None
        clause.limit = None
        frame = _make_frame(df)

        result = planner.apply_projection_modifiers(df.copy(), clause, frame)
        assert list(result["x"]) == [1, 2, 3]

    def test_skip(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [10, 20, 30, 40, 50]})
        clause = MagicMock()
        clause.distinct = False
        clause.order_by = None
        clause.skip = 2
        clause.limit = None
        frame = _make_frame(df)

        result = planner.apply_projection_modifiers(df.copy(), clause, frame)
        assert list(result["x"]) == [30, 40, 50]

    def test_limit(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [10, 20, 30, 40, 50]})
        clause = MagicMock()
        clause.distinct = False
        clause.order_by = None
        clause.skip = None
        clause.limit = 3
        frame = _make_frame(df)

        result = planner.apply_projection_modifiers(df.copy(), clause, frame)
        assert list(result["x"]) == [10, 20, 30]

    def test_skip_and_limit_combined(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})
        clause = MagicMock()
        clause.distinct = False
        clause.order_by = None
        clause.skip = 1
        clause.limit = 2
        frame = _make_frame(df)

        result = planner.apply_projection_modifiers(df.copy(), clause, frame)
        assert list(result["x"]) == [2, 3]

    def test_order_by_ascending(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [3, 1, 2]})
        order_item = MagicMock()
        order_item.expression = Variable(name="x")
        order_item.ascending = True
        order_item.nulls_placement = None

        clause = MagicMock()
        clause.distinct = False
        clause.order_by = [order_item]
        clause.skip = None
        clause.limit = None

        frame = _make_frame(df)

        with patch("pycypher.binding_evaluator.BindingExpressionEvaluator") as MockEval:
            eval_inst = MagicMock()
            eval_inst.evaluate = MagicMock(return_value=pd.Series([3, 1, 2]))
            MockEval.return_value = eval_inst

            result = planner.apply_projection_modifiers(df.copy(), clause, frame)

        assert list(result["x"]) == [1, 2, 3]

    def test_order_by_descending(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [3, 1, 2]})
        order_item = MagicMock()
        order_item.expression = Variable(name="x")
        order_item.ascending = False
        order_item.nulls_placement = None

        clause = MagicMock()
        clause.distinct = False
        clause.order_by = [order_item]
        clause.skip = None
        clause.limit = None

        frame = _make_frame(df)

        with patch("pycypher.binding_evaluator.BindingExpressionEvaluator") as MockEval:
            eval_inst = MagicMock()
            eval_inst.evaluate = MagicMock(return_value=pd.Series([3, 1, 2]))
            MockEval.return_value = eval_inst

            result = planner.apply_projection_modifiers(df.copy(), clause, frame)

        assert list(result["x"]) == [3, 2, 1]

    def test_no_modifiers(self) -> None:
        planner = _make_planner()
        df = pd.DataFrame({"x": [1, 2, 3]})
        clause = MagicMock()
        clause.distinct = False
        clause.order_by = None
        clause.skip = None
        clause.limit = None
        frame = _make_frame(df)

        result = planner.apply_projection_modifiers(df.copy(), clause, frame)
        assert list(result["x"]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# return_from_frame — RETURN * (empty items)
# ---------------------------------------------------------------------------


class TestReturnFromFrame:
    """Test RETURN clause evaluation."""

    def test_return_star_no_items(self) -> None:
        """RETURN * returns all non-internal columns."""
        planner = _make_planner()
        df = pd.DataFrame(
            {"name": ["Alice", "Bob"], "_internal": [1, 2], "age": [30, 25]},
        )
        frame = _make_frame(df)

        return_clause = MagicMock()
        return_clause.items = []
        return_clause.distinct = False
        return_clause.order_by = None
        return_clause.skip = None
        return_clause.limit = None

        result = planner.return_from_frame(return_clause, frame)
        assert "name" in result.columns
        assert "age" in result.columns
        assert "_internal" not in result.columns

    def test_return_with_alias_inference(self) -> None:
        """Items without alias get one inferred via infer_alias."""
        planner = _make_planner()
        frame = _make_frame(pd.DataFrame({"p": [1, 2]}))

        var_expr = Variable(name="myVar")
        item = ReturnItem(expression=var_expr, alias=None)

        return_clause = MagicMock()
        return_clause.items = [item]
        return_clause.distinct = False
        return_clause.order_by = None
        return_clause.skip = None
        return_clause.limit = None

        agg_result = pd.DataFrame({"myVar": [1, 2]})
        planner._agg_planner.aggregate_items = MagicMock(return_value=agg_result)

        result = planner.return_from_frame(return_clause, frame)
        # Alias should have been inferred to "myVar"
        assert item.alias == "myVar"

    def test_return_disambiguates_colliding_aliases(self) -> None:
        """Two items inferring the same alias get qualified."""
        planner = _make_planner()
        frame = _make_frame(pd.DataFrame())

        item1 = ReturnItem(expression=_prop("p", "name"), alias=None)
        item2 = ReturnItem(expression=_prop("f", "name"), alias=None)

        return_clause = MagicMock()
        return_clause.items = [item1, item2]
        return_clause.distinct = False
        return_clause.order_by = None
        return_clause.skip = None
        return_clause.limit = None

        agg_result = pd.DataFrame({"p.name": ["Alice"], "f.name": ["Bob"]})
        planner._agg_planner.aggregate_items = MagicMock(return_value=agg_result)

        planner.return_from_frame(return_clause, frame)

        # After disambiguation, aliases should be fully qualified
        assert item1.alias == "p.name"
        assert item2.alias == "f.name"


# ---------------------------------------------------------------------------
# with_to_binding_frame
# ---------------------------------------------------------------------------


class TestWithToBindingFrame:
    """Test WITH clause evaluation."""

    def test_with_star_passes_through(self) -> None:
        """WITH * (empty items) passes bindings through."""
        planner = _make_planner()
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        frame = _make_frame(df, type_registry={"x": "Person"})

        with_clause = MagicMock()
        with_clause.items = []
        with_clause.where = None
        with_clause.distinct = False
        with_clause.order_by = None
        with_clause.skip = None
        with_clause.limit = None

        with patch("pycypher.binding_frame.BindingFrame") as MockBF:
            MockBF.return_value = MagicMock()
            result = planner.with_to_binding_frame(with_clause, frame)

        assert MockBF.called

    def test_with_preserves_type_registry_for_variables(self) -> None:
        """WITH p preserves p's type_registry entry."""
        planner = _make_planner()
        df = pd.DataFrame({"p": [1, 2]})
        frame = _make_frame(df, type_registry={"p": "Person"})

        var_p = Variable(name="p")
        item = ReturnItem(expression=var_p, alias=None)

        with_clause = MagicMock()
        with_clause.items = [item]
        with_clause.where = None
        with_clause.distinct = False
        with_clause.order_by = None
        with_clause.skip = None
        with_clause.limit = None

        agg_result = pd.DataFrame({"p": [1, 2]})
        planner._agg_planner.aggregate_items = MagicMock(return_value=agg_result)

        with patch("pycypher.binding_frame.BindingFrame") as MockBF:
            mock_bf_instance = MagicMock()
            mock_bf_instance.bindings = agg_result
            MockBF.return_value = mock_bf_instance

            planner.with_to_binding_frame(with_clause, frame)

        # Check that type_registry was passed with "p" -> "Person"
        first_call_kwargs = MockBF.call_args_list[0]
        passed_registry = first_call_kwargs.kwargs.get(
            "type_registry",
            first_call_kwargs[1].get("type_registry", {}),
        )
        assert passed_registry.get("p") == "Person"

    def test_with_where_applied(self) -> None:
        """WITH ... WHERE calls where_fn."""
        where_fn = MagicMock()
        planner = _make_planner(where_fn=where_fn)

        df = pd.DataFrame({"x": [1, 2]})
        frame = _make_frame(df)

        item = ReturnItem(expression=Variable(name="x"), alias="x")
        where_expr = MagicMock()

        with_clause = MagicMock()
        with_clause.items = [item]
        with_clause.where = where_expr
        with_clause.distinct = False
        with_clause.order_by = None
        with_clause.skip = None
        with_clause.limit = None

        agg_result = pd.DataFrame({"x": [1, 2]})
        planner._agg_planner.aggregate_items = MagicMock(return_value=agg_result)

        filtered_frame = MagicMock()
        filtered_frame.bindings = pd.DataFrame({"x": [1]})
        where_fn.return_value = filtered_frame

        with patch("pycypher.binding_frame.BindingFrame") as MockBF:
            mock_bf_instance = MagicMock()
            mock_bf_instance.bindings = agg_result
            MockBF.return_value = mock_bf_instance

            planner.with_to_binding_frame(with_clause, frame)

        where_fn.assert_called_once()

    def test_with_alias_inference(self) -> None:
        """WITH items without alias get one inferred."""
        planner = _make_planner()
        frame = _make_frame(pd.DataFrame({"x": [1]}))

        item = ReturnItem(expression=Variable(name="myAlias"), alias=None)

        with_clause = MagicMock()
        with_clause.items = [item]
        with_clause.where = None
        with_clause.distinct = False
        with_clause.order_by = None
        with_clause.skip = None
        with_clause.limit = None

        agg_result = pd.DataFrame({"myAlias": [1]})
        planner._agg_planner.aggregate_items = MagicMock(return_value=agg_result)

        with patch("pycypher.binding_frame.BindingFrame") as MockBF:
            mock_bf_instance = MagicMock()
            mock_bf_instance.bindings = agg_result
            MockBF.return_value = mock_bf_instance

            planner.with_to_binding_frame(with_clause, frame)

        assert item.alias == "myAlias"
