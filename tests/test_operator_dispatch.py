"""Exhaustive tests for every operator path in BindingExpressionEvaluator.

These tests assert the semantics of every operator in all four dispatch tables
(_ARITH_OPS, _CMP_OPS, _UNARY_OPS, _NULL_CHECK_OPS) so the Table-Driven
refactor can be verified correct before and after the change.
"""

import pandas as pd
import pytest
from pycypher.ast_models import (
    Arithmetic,
    Comparison,
    FloatLiteral,
    IntegerLiteral,
    NullCheck,
    NullLiteral,
    StringLiteral,
    Unary,
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


@pytest.fixture
def simple_context() -> Context:
    df = pd.DataFrame({ID_COLUMN: [1, 2, 3], "val": [10, 20, 30]})
    table = EntityTable(
        entity_type="T",
        identifier="T",
        column_names=[ID_COLUMN, "val"],
        source_obj_attribute_map={"val": "val"},
        attribute_map={"val": "val"},
        source_obj=df,
    )
    return Context(
        entity_mapping=EntityMapping(mapping={"T": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


@pytest.fixture
def frame(simple_context: Context) -> BindingFrame:
    """Single-row frame for deterministic scalar evaluation."""
    return BindingFrame(
        bindings=pd.DataFrame({"_x": [0]}),
        type_registry={},
        context=simple_context,
    )


def _int(val: int) -> IntegerLiteral:
    return IntegerLiteral(value=val)


def _float(val: float) -> FloatLiteral:
    return FloatLiteral(value=val)


def _str(val: str) -> StringLiteral:
    return StringLiteral(value=val)


def _eval(frame: BindingFrame, expr) -> object:
    return BindingExpressionEvaluator(frame).evaluate(expr).iloc[0]


# ---------------------------------------------------------------------------
# Arithmetic operators
# ---------------------------------------------------------------------------


class TestArithmeticOperators:
    def test_add(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Arithmetic(operator="+", left=_int(3), right=_int(4)))
            == 7
        )

    def test_subtract(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Arithmetic(operator="-", left=_int(10), right=_int(3))
            )
            == 7
        )

    def test_multiply(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Arithmetic(operator="*", left=_int(3), right=_int(4)))
            == 12
        )

    def test_divide(self, frame: BindingFrame) -> None:
        assert _eval(
            frame,
            Arithmetic(operator="/", left=_float(10.0), right=_float(4.0)),
        ) == pytest.approx(2.5)

    def test_modulo(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Arithmetic(operator="%", left=_int(10), right=_int(3))
            )
            == 1
        )

    def test_power(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Arithmetic(operator="^", left=_int(2), right=_int(8)))
            == 256
        )

    def test_unsupported_operator_raises(self, frame: BindingFrame) -> None:
        with pytest.raises(
            ValueError, match="Unsupported arithmetic operator"
        ):
            _eval(
                frame, Arithmetic(operator="//", left=_int(10), right=_int(3))
            )


# ---------------------------------------------------------------------------
# Comparison operators
# ---------------------------------------------------------------------------


class TestComparisonOperators:
    def test_equal_true(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator="=", left=_int(5), right=_int(5)))
            == True
        )  # noqa: E712

    def test_equal_false(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator="=", left=_int(5), right=_int(6)))
            == False
        )  # noqa: E712

    def test_not_equal_true(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator="<>", left=_int(5), right=_int(6))
            )
            == True
        )  # noqa: E712

    def test_not_equal_false(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator="<>", left=_int(5), right=_int(5))
            )
            == False
        )  # noqa: E712

    def test_less_than_true(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator="<", left=_int(3), right=_int(5)))
            == True
        )  # noqa: E712

    def test_less_than_false(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator="<", left=_int(5), right=_int(3)))
            == False
        )  # noqa: E712

    def test_greater_than_true(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator=">", left=_int(5), right=_int(3)))
            == True
        )  # noqa: E712

    def test_greater_than_false(self, frame: BindingFrame) -> None:
        assert (
            _eval(frame, Comparison(operator=">", left=_int(3), right=_int(5)))
            == False
        )  # noqa: E712

    def test_less_than_or_equal_equal(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator="<=", left=_int(5), right=_int(5))
            )
            == True
        )  # noqa: E712

    def test_less_than_or_equal_less(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator="<=", left=_int(4), right=_int(5))
            )
            == True
        )  # noqa: E712

    def test_less_than_or_equal_greater(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator="<=", left=_int(6), right=_int(5))
            )
            == False
        )  # noqa: E712

    def test_greater_than_or_equal_equal(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator=">=", left=_int(5), right=_int(5))
            )
            == True
        )  # noqa: E712

    def test_greater_than_or_equal_greater(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator=">=", left=_int(6), right=_int(5))
            )
            == True
        )  # noqa: E712

    def test_greater_than_or_equal_less(self, frame: BindingFrame) -> None:
        assert (
            _eval(
                frame, Comparison(operator=">=", left=_int(4), right=_int(5))
            )
            == False
        )  # noqa: E712

    def test_unsupported_operator_raises(self, frame: BindingFrame) -> None:
        with pytest.raises(
            ValueError, match="Unsupported comparison operator"
        ):
            _eval(
                frame, Comparison(operator="!=", left=_int(1), right=_int(2))
            )


# ---------------------------------------------------------------------------
# Unary operators
# ---------------------------------------------------------------------------


class TestUnaryOperators:
    def test_unary_plus(self, frame: BindingFrame) -> None:
        assert _eval(frame, Unary(operator="+", operand=_int(7))) == 7

    def test_unary_minus(self, frame: BindingFrame) -> None:
        assert _eval(frame, Unary(operator="-", operand=_int(7))) == -7

    def test_unary_minus_float(self, frame: BindingFrame) -> None:
        assert _eval(
            frame, Unary(operator="-", operand=_float(3.5))
        ) == pytest.approx(-3.5)

    def test_unsupported_unary_raises(self, frame: BindingFrame) -> None:
        with pytest.raises(ValueError, match="Unsupported unary operator"):
            _eval(frame, Unary(operator="~", operand=_int(1)))


# ---------------------------------------------------------------------------
# Null-check operators
# ---------------------------------------------------------------------------


class TestNullCheckOperators:
    def test_is_null_on_null(self, frame: BindingFrame) -> None:
        result = _eval(
            frame, NullCheck(operator="IS NULL", operand=NullLiteral())
        )
        assert result == True  # noqa: E712

    def test_is_null_on_non_null(self, frame: BindingFrame) -> None:
        result = _eval(frame, NullCheck(operator="IS NULL", operand=_int(5)))
        assert result == False  # noqa: E712

    def test_is_not_null_on_non_null(self, frame: BindingFrame) -> None:
        result = _eval(
            frame, NullCheck(operator="IS NOT NULL", operand=_str("hello"))
        )
        assert result == True  # noqa: E712

    def test_is_not_null_on_null(self, frame: BindingFrame) -> None:
        result = _eval(
            frame, NullCheck(operator="IS NOT NULL", operand=NullLiteral())
        )
        assert result == False  # noqa: E712

    def test_unknown_null_check_raises(self, frame: BindingFrame) -> None:
        with pytest.raises((ValueError, NotImplementedError)):
            _eval(
                frame,
                NullCheck(operator="IS SOMETHING", operand=NullLiteral()),
            )
