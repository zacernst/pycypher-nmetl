"""Exhaustive tests for every aggregation function in evaluate_aggregation().

These tests exercise every entry in the _AGG_OPS dispatch table so the
Table-Driven refactor can be verified correct before and after the change.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    Arithmetic,
    CountStar,
    FunctionInvocation,
    IntegerLiteral,
    PropertyLookup,
    Variable,
)
from pycypher.binding_evaluator import BindingExpressionEvaluator
from pycypher.binding_frame import BindingFrame
from pycypher.relational_models import (
    ID_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def num_context() -> Context:
    """Context with a Value entity having a numeric 'n' column (1,2,3,4,5)."""
    df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4, 5], "n": [1, 2, 3, 4, 5]})
    table = EntityTable(
        entity_type="V",
        identifier="V",
        column_names=[ID_COLUMN, "n"],
        source_obj_attribute_map={"n": "n"},
        attribute_map={"n": "n"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"V": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def frame(num_context: Context) -> BindingFrame:
    """BindingFrame with 5 rows — one per entity."""
    return BindingFrame(
        bindings=pd.DataFrame({"v": [1, 2, 3, 4, 5]}),
        type_registry={"v": "V"},
        context=num_context,
    )


@pytest.fixture
def dup_context() -> Context:
    """Context with duplicates: values 10, 20, 20, 30."""
    df = pd.DataFrame({ID_COLUMN: [1, 2, 3, 4], "n": [10, 20, 20, 30]})
    table = EntityTable(
        entity_type="V",
        identifier="V",
        column_names=[ID_COLUMN, "n"],
        source_obj_attribute_map={"n": "n"},
        attribute_map={"n": "n"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"V": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def dup_frame(dup_context: Context) -> BindingFrame:
    return BindingFrame(
        bindings=pd.DataFrame({"v": [1, 2, 3, 4]}),
        type_registry={"v": "V"},
        context=dup_context,
    )


def _prop() -> PropertyLookup:
    return PropertyLookup(expression=Variable(name="v"), property="n")


def _agg(name: str, distinct: bool = False) -> FunctionInvocation:
    args: dict = {"arguments": [_prop()]}
    if distinct:
        args["distinct"] = True
    return FunctionInvocation(name=name, arguments=args)


def _eval_agg(frame: BindingFrame, expr) -> object:
    return BindingExpressionEvaluator(frame).evaluate_aggregation(expr)


# ---------------------------------------------------------------------------
# collect
# ---------------------------------------------------------------------------


class TestCollect:
    def test_collect_returns_list(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("collect"))
        assert isinstance(result, list)
        assert sorted(result) == [1, 2, 3, 4, 5]

    def test_collect_distinct(self, dup_frame: BindingFrame) -> None:
        result = _eval_agg(dup_frame, _agg("collect", distinct=True))
        assert sorted(result) == [10, 20, 30]

    def test_collect_case_insensitive(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("COLLECT"))
        assert sorted(result) == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# count
# ---------------------------------------------------------------------------


class TestCount:
    def test_count_star(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, CountStar())
        assert result == 5

    def test_count_expr(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("count"))
        assert result == 5

    def test_count_distinct(self, dup_frame: BindingFrame) -> None:
        result = _eval_agg(dup_frame, _agg("count", distinct=True))
        assert result == 3  # 10, 20, 30

    def test_count_case_insensitive(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("COUNT"))
        assert result == 5


# ---------------------------------------------------------------------------
# sum
# ---------------------------------------------------------------------------


class TestSum:
    def test_sum_integers(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("sum"))
        assert float(result) == 15.0  # 1+2+3+4+5

    def test_sum_distinct(self, dup_frame: BindingFrame) -> None:
        result = _eval_agg(dup_frame, _agg("sum", distinct=True))
        assert float(result) == 60.0  # 10+20+30

    def test_sum_returns_float(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("sum"))
        assert isinstance(result, float)

    def test_sum_case_insensitive(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("SUM"))
        assert float(result) == 15.0


# ---------------------------------------------------------------------------
# avg
# ---------------------------------------------------------------------------


class TestAvg:
    def test_avg_integers(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("avg"))
        assert abs(float(result) - 3.0) < 0.001  # (1+2+3+4+5)/5

    def test_avg_distinct(self, dup_frame: BindingFrame) -> None:
        result = _eval_agg(dup_frame, _agg("avg", distinct=True))
        assert abs(float(result) - 20.0) < 0.001  # (10+20+30)/3

    def test_avg_returns_float(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("avg"))
        assert isinstance(result, float)

    def test_avg_case_insensitive(self, frame: BindingFrame) -> None:
        result = _eval_agg(frame, _agg("AVG"))
        assert abs(float(result) - 3.0) < 0.001


# ---------------------------------------------------------------------------
# min / max
# ---------------------------------------------------------------------------


class TestMinMax:
    def test_min(self, frame: BindingFrame) -> None:
        assert _eval_agg(frame, _agg("min")) == 1

    def test_max(self, frame: BindingFrame) -> None:
        assert _eval_agg(frame, _agg("max")) == 5

    def test_min_distinct(self, dup_frame: BindingFrame) -> None:
        assert _eval_agg(dup_frame, _agg("min", distinct=True)) == 10

    def test_max_distinct(self, dup_frame: BindingFrame) -> None:
        assert _eval_agg(dup_frame, _agg("max", distinct=True)) == 30

    def test_min_case_insensitive(self, frame: BindingFrame) -> None:
        assert _eval_agg(frame, _agg("MIN")) == 1

    def test_max_case_insensitive(self, frame: BindingFrame) -> None:
        assert _eval_agg(frame, _agg("MAX")) == 5


# ---------------------------------------------------------------------------
# Unsupported function raises ValueError
# ---------------------------------------------------------------------------


class TestUnsupported:
    def test_unsupported_raises_value_error(self, frame: BindingFrame) -> None:
        with pytest.raises(ValueError, match="median"):
            _eval_agg(frame, _agg("median"))

    def test_error_names_the_function(self, frame: BindingFrame) -> None:
        with pytest.raises(ValueError) as exc_info:
            _eval_agg(frame, _agg("median"))
        assert "median" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Arithmetic wrapper around aggregations
# ---------------------------------------------------------------------------


class TestAggArithmetic:
    def test_count_star_plus_one(self, frame: BindingFrame) -> None:
        expr = Arithmetic(
            operator="+",
            left=CountStar(),
            right=IntegerLiteral(value=1),
        )
        assert _eval_agg(frame, expr) == 6  # 5 + 1

    def test_sum_times_two(self, frame: BindingFrame) -> None:
        expr = Arithmetic(
            operator="*",
            left=_agg("sum"),
            right=IntegerLiteral(value=2),
        )
        assert float(_eval_agg(frame, expr)) == 30.0  # 15 * 2

    def test_count_minus_one(self, frame: BindingFrame) -> None:
        expr = Arithmetic(
            operator="-",
            left=CountStar(),
            right=IntegerLiteral(value=1),
        )
        assert _eval_agg(frame, expr) == 4  # 5 - 1
