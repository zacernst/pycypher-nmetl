"""Tests for BindingExpressionEvaluator coverage gaps.

Targets the 34% → 55%+ coverage increase for binding_evaluator.py by
exercising standalone functions, lazy property accessors, the evaluate()
dispatch branches, and helper methods.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.ast_models import (
    And,
    Arithmetic,
    BooleanLiteral,
    CaseExpression,
    Comparison,
    FloatLiteral,
    IntegerLiteral,
    Not,
    NullCheck,
    NullLiteral,
    Or,
    StringLiteral,
    Unary,
    Variable,
    WhenClause,
    Xor,
)
from pycypher.binding_evaluator import (
    _TEMPORAL_FIELD_ACCESSORS,
    BindingExpressionEvaluator,
    _extract_temporal_field,
)
from shared.helpers import is_null_raw_list as _is_null_raw_list
from pycypher.binding_frame import BindingFrame
from pycypher.comparison_evaluator import (
    _CMP_OPS,
    _NULL_CHECK_OPS,
    _UNARY_OPS,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
)

ID_COLUMN = "__ID__"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_frame(n: int = 4) -> BindingFrame:
    """Create a minimal BindingFrame with *n* rows and a Person entity table."""
    df = pd.DataFrame(
        {
            "p": range(1, n + 1),
            ID_COLUMN: range(1, n + 1),
        },
    )
    person_table = EntityTable.from_dataframe(
        "Person",
        pd.DataFrame(
            {
                ID_COLUMN: range(1, n + 1),
                "name": [f"Person{i}" for i in range(1, n + 1)],
                "age": [20 + i for i in range(1, n + 1)],
            },
        ),
    )
    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )
    return BindingFrame(
        bindings=df,
        context=context,
        type_registry={"p": "Person"},
    )


# ---------------------------------------------------------------------------
# Tests for _extract_temporal_field
# ---------------------------------------------------------------------------


class TestExtractTemporalField:
    """Tests for the standalone _extract_temporal_field function."""

    def test_extract_year_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:00", "year") == 2024

    def test_extract_month_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:00", "month") == 3

    def test_extract_day_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:00", "day") == 15

    def test_extract_hour_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:00", "hour") == 10

    def test_extract_minute_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:00", "minute") == 30

    def test_extract_second_from_datetime(self) -> None:
        assert _extract_temporal_field("2024-03-15T10:30:45", "second") == 45

    def test_extract_millisecond_from_datetime(self) -> None:
        assert (
            _extract_temporal_field(
                "2024-03-15T10:30:00.123456",
                "millisecond",
            )
            == 123
        )

    def test_extract_microsecond_from_datetime(self) -> None:
        assert (
            _extract_temporal_field(
                "2024-03-15T10:30:00.123456",
                "microsecond",
            )
            == 123456
        )

    def test_extract_week_from_date(self) -> None:
        # 2024-01-01 is Monday of week 1
        result = _extract_temporal_field("2024-01-01", "week")
        assert isinstance(result, int)
        assert result == 1

    def test_extract_dayOfWeek_from_date(self) -> None:
        # 2024-03-15 is a Friday = isoweekday() == 5
        assert _extract_temporal_field("2024-03-15", "dayOfWeek") == 5

    def test_extract_dayOfYear_from_date(self) -> None:
        # 2024-03-15: 31 (Jan) + 29 (Feb, leap) + 15 = 75
        assert _extract_temporal_field("2024-03-15", "dayOfYear") == 75

    def test_extract_quarter_from_date(self) -> None:
        assert _extract_temporal_field("2024-01-15", "quarter") == 1
        assert _extract_temporal_field("2024-04-15", "quarter") == 2
        assert _extract_temporal_field("2024-07-15", "quarter") == 3
        assert _extract_temporal_field("2024-10-15", "quarter") == 4

    def test_extract_year_from_date_only(self) -> None:
        assert _extract_temporal_field("2024-03-15", "year") == 2024

    def test_extract_hour_from_date_returns_none(self) -> None:
        # date objects don't have hour; falls back to getattr with None default
        # However the function tries datetime.fromisoformat first, which succeeds
        # for "2024-03-15" (parsed as midnight), so hour = 0
        result = _extract_temporal_field("2024-03-15", "hour")
        assert result == 0

    def test_non_string_returns_none(self) -> None:
        assert _extract_temporal_field(42, "year") is None
        assert _extract_temporal_field(None, "year") is None

    def test_unknown_field_returns_none(self) -> None:
        assert _extract_temporal_field("2024-03-15", "nonexistent") is None

    def test_invalid_string_returns_none(self) -> None:
        assert _extract_temporal_field("not-a-date", "year") is None

    def test_empty_string_returns_none(self) -> None:
        assert _extract_temporal_field("", "year") is None


# ---------------------------------------------------------------------------
# Tests for _is_null_raw_list
# ---------------------------------------------------------------------------


class TestIsNullRawList:
    """Tests for the standalone _is_null_raw_list function."""

    def test_none_is_null(self) -> None:
        assert _is_null_raw_list(None) is True

    def test_list_is_not_null(self) -> None:
        assert _is_null_raw_list([1, 2, 3]) is False

    def test_empty_list_is_null(self) -> None:
        # Empty lists are treated as missing/empty — nothing to iterate.
        assert _is_null_raw_list([]) is True

    def test_tuple_is_not_null(self) -> None:
        assert _is_null_raw_list((1, 2)) is False

    def test_empty_tuple_is_null(self) -> None:
        assert _is_null_raw_list(()) is True

    def test_int_is_not_null(self) -> None:
        # Scalars like int are not null (pd.isna(42) is False).
        assert _is_null_raw_list(42) is False

    def test_string_is_not_null(self) -> None:
        # pd.isna("hello") returns False
        assert _is_null_raw_list("hello") is False

    def test_nan_is_null(self) -> None:

        assert _is_null_raw_list(float("nan")) is True


# ---------------------------------------------------------------------------
# Tests for dispatch tables
# ---------------------------------------------------------------------------


class TestDispatchTables:
    """Tests for the operator dispatch tables."""

    def test_cmp_ops_contains_all_operators(self) -> None:
        assert set(_CMP_OPS.keys()) == {"=", "<>", "<", ">", "<=", ">="}

    def test_unary_ops_contains_plus_minus(self) -> None:
        assert set(_UNARY_OPS.keys()) == {"+", "-"}

    def test_null_check_ops_keys(self) -> None:
        assert set(_NULL_CHECK_OPS.keys()) == {"IS NULL", "IS NOT NULL"}

    def test_temporal_field_accessors_complete(self) -> None:
        expected = {
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "millisecond",
            "microsecond",
            "week",
            "dayOfWeek",
            "dayOfYear",
            "quarter",
        }
        assert set(_TEMPORAL_FIELD_ACCESSORS.keys()) == expected


# ---------------------------------------------------------------------------
# Tests for BindingExpressionEvaluator
# ---------------------------------------------------------------------------


class TestBindingExpressionEvaluatorInit:
    """Tests for evaluator initialization and lazy property accessors."""

    def test_init_stores_frame(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev.frame is frame

    def test_lazy_arithmetic_evaluator(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev._arithmetic_evaluator is None
        ae = ev.arithmetic_evaluator
        assert ae is not None
        # Second access returns the same instance (caching)
        assert ev.arithmetic_evaluator is ae

    def test_lazy_boolean_evaluator(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev._boolean_evaluator is None
        be = ev.boolean_evaluator
        assert be is not None
        assert ev.boolean_evaluator is be

    def test_lazy_aggregation_evaluator(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev._aggregation_evaluator is None
        ag = ev.aggregation_evaluator
        assert ag is not None
        assert ev.aggregation_evaluator is ag

    def test_lazy_collection_evaluator(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev._collection_evaluator is None
        ce = ev.collection_evaluator
        assert ce is not None
        assert ev.collection_evaluator is ce

    def test_lazy_scalar_function_evaluator(self) -> None:
        frame = _make_frame()
        ev = BindingExpressionEvaluator(frame)
        assert ev._scalar_function_evaluator is None
        sf = ev.scalar_function_evaluator
        assert sf is not None
        assert ev.scalar_function_evaluator is sf


# ---------------------------------------------------------------------------
# Tests for evaluate() dispatch
# ---------------------------------------------------------------------------


class TestEvaluateLiterals:
    """Test evaluation of literal expressions."""

    def test_integer_literal(self) -> None:
        frame = _make_frame(3)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(IntegerLiteral(value=42))
        assert list(result) == [42, 42, 42]

    def test_float_literal(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(FloatLiteral(value=3.14))
        assert list(result) == [3.14, 3.14]

    def test_string_literal(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(StringLiteral(value="hello"))
        assert list(result) == ["hello", "hello"]

    def test_boolean_literal_true(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(BooleanLiteral(value=True))
        assert list(result) == [True, True]

    def test_boolean_literal_false(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(BooleanLiteral(value=False))
        assert list(result) == [False, False]

    def test_null_literal(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(NullLiteral())
        assert result.iloc[0] is None
        assert result.iloc[1] is None


class TestEvaluateVariable:
    """Test evaluation of variable references."""

    def test_known_variable(self) -> None:
        frame = _make_frame(3)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(Variable(name="p"))
        assert list(result) == [1, 2, 3]

    def test_unknown_variable_raises(self) -> None:
        from pycypher.exceptions import VariableNotFoundError

        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        with pytest.raises(VariableNotFoundError):
            ev.evaluate(Variable(name="unknown_var"))


class TestEvaluateComparison:
    """Test evaluation of comparison expressions."""

    def test_equality(self) -> None:
        frame = _make_frame(3)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Comparison(
                operator="=",
                left=IntegerLiteral(value=1),
                right=IntegerLiteral(value=1),
            ),
        )
        assert all(result)

    def test_not_equal(self) -> None:
        frame = _make_frame(3)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Comparison(
                operator="<>",
                left=IntegerLiteral(value=1),
                right=IntegerLiteral(value=2),
            ),
        )
        assert all(result)

    def test_less_than(self) -> None:
        frame = _make_frame(3)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Comparison(
                operator="<",
                left=IntegerLiteral(value=1),
                right=IntegerLiteral(value=2),
            ),
        )
        assert all(result)

    def test_comparison_with_null_returns_null(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Comparison(
                operator="=",
                left=NullLiteral(),
                right=IntegerLiteral(value=1),
            ),
        )
        assert all(v is None for v in result)

    def test_unsupported_comparison_raises(self) -> None:
        from pycypher.exceptions import UnsupportedOperatorError

        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate(
                Comparison(
                    operator="===",
                    left=IntegerLiteral(value=1),
                    right=IntegerLiteral(value=1),
                ),
            )


class TestEvaluateNullCheck:
    """Test evaluation of IS NULL / IS NOT NULL."""

    def test_is_null(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            NullCheck(operator="IS NULL", operand=NullLiteral()),
        )
        assert all(result)

    def test_is_not_null(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            NullCheck(operator="IS NOT NULL", operand=IntegerLiteral(value=5)),
        )
        assert all(result)

    def test_is_null_on_non_null(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            NullCheck(operator="IS NULL", operand=IntegerLiteral(value=5)),
        )
        assert not any(result)


class TestEvaluateUnary:
    """Test evaluation of unary operators."""

    def test_unary_plus(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Unary(operator="+", operand=IntegerLiteral(value=42)),
        )
        assert list(result) == [42, 42]

    def test_unary_minus(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Unary(operator="-", operand=IntegerLiteral(value=42)),
        )
        assert list(result) == [-42, -42]

    def test_unary_minus_null_propagates(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(Unary(operator="-", operand=NullLiteral()))
        assert all(v is None for v in result)

    def test_unsupported_unary_raises(self) -> None:
        from pycypher.exceptions import UnsupportedOperatorError

        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        with pytest.raises(UnsupportedOperatorError):
            ev.evaluate(Unary(operator="~", operand=IntegerLiteral(value=1)))


class TestEvaluateBooleanLogic:
    """Test evaluation of AND, OR, NOT, XOR expressions."""

    def test_and_true_true(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            And(
                operands=[
                    BooleanLiteral(value=True),
                    BooleanLiteral(value=True),
                ],
            ),
        )
        for v in result:
            assert v

    def test_and_true_false(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            And(
                operands=[
                    BooleanLiteral(value=True),
                    BooleanLiteral(value=False),
                ],
            ),
        )
        for v in result:
            assert not v

    def test_or_false_true(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Or(
                operands=[
                    BooleanLiteral(value=False),
                    BooleanLiteral(value=True),
                ],
            ),
        )
        for v in result:
            assert v

    def test_not_true(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(Not(operand=BooleanLiteral(value=True)))
        for v in result:
            assert not v

    def test_xor_true_false(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Xor(
                operands=[
                    BooleanLiteral(value=True),
                    BooleanLiteral(value=False),
                ],
            ),
        )
        for v in result:
            assert v


class TestEvaluateArithmetic:
    """Test evaluation of arithmetic expressions."""

    def test_addition(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Arithmetic(
                operator="+",
                left=IntegerLiteral(value=3),
                right=IntegerLiteral(value=4),
            ),
        )
        assert list(result) == [7, 7]

    def test_subtraction(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Arithmetic(
                operator="-",
                left=IntegerLiteral(value=10),
                right=IntegerLiteral(value=3),
            ),
        )
        assert list(result) == [7, 7]

    def test_multiplication(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            Arithmetic(
                operator="*",
                left=IntegerLiteral(value=3),
                right=IntegerLiteral(value=4),
            ),
        )
        assert list(result) == [12, 12]


class TestEvaluateCaseExpression:
    """Test evaluation of CASE expressions."""

    def test_case_when_matched(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            CaseExpression(
                expression=None,
                when_clauses=[
                    WhenClause(
                        condition=BooleanLiteral(value=True),
                        result=StringLiteral(value="yes"),
                    ),
                ],
                else_expr=StringLiteral(value="no"),
            ),
        )
        assert list(result) == ["yes", "yes"]

    def test_case_else_fallback(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)
        result = ev.evaluate(
            CaseExpression(
                expression=None,
                when_clauses=[
                    WhenClause(
                        condition=BooleanLiteral(value=False),
                        result=StringLiteral(value="yes"),
                    ),
                ],
                else_expr=StringLiteral(value="no"),
            ),
        )
        assert list(result) == ["no", "no"]


class TestNullSafeHelper:
    """Test the _null_safe static method."""

    def test_null_safe_replaces_none(self) -> None:
        s = pd.Series([True, None, False, None])
        result = BindingExpressionEvaluator._null_safe(s)
        assert list(result) == [True, False, False, False]

    def test_null_safe_no_nulls(self) -> None:
        s = pd.Series([True, False, True])
        result = BindingExpressionEvaluator._null_safe(s)
        assert list(result) == [True, False, True]


class TestUnsupportedExpression:
    """Test that unsupported expressions raise NotImplementedError."""

    def test_unsupported_expression_type(self) -> None:
        frame = _make_frame(2)
        ev = BindingExpressionEvaluator(frame)

        class FakeExpression:
            """A fake expression type not handled by evaluate()."""

        with pytest.raises(NotImplementedError, match="not yet supported"):
            ev.evaluate(FakeExpression())  # type: ignore[arg-type]
