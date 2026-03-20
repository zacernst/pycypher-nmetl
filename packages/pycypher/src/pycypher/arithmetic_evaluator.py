"""ArithmeticExpressionEvaluator - Specialized evaluator for arithmetic and comparison operations.

This module provides focused evaluation of arithmetic operations extracted from the
BindingExpressionEvaluator god object. This is Phase 1 of the systematic refactoring
to decompose the 2904-line evaluator into specialized, maintainable components.

Architecture Loop 277 - Phase 1 Implementation
"""

from __future__ import annotations

import logging
import operator as _op
import time
from typing import TYPE_CHECKING, Any, cast

import numpy as np

if TYPE_CHECKING:
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
import pandas as pd
from shared.helpers import is_null_value
from shared.logger import LOGGER

from pycypher.binding_frame import BindingFrame
from pycypher.exceptions import (
    IncompatibleOperatorError,
    UnsupportedOperatorError,
)
from pycypher.types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)


def _cypher_div(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Divide two Series using openCypher semantics.

    * integer / integer → integer (truncation toward zero, matching Neo4j)
    * anything else     → float  (standard true division)

    Args:
        left: Left-hand dividend Series.
        right: Right-hand divisor Series.

    Returns:
        A Series of quotients; integer dtype when both operands are integer,
        float dtype otherwise.
    """
    if pd.api.types.is_integer_dtype(left) and pd.api.types.is_integer_dtype(
        right,
    ):
        zero_mask = right == 0
        null_mask = left.isna() | right.isna()
        replace_mask = zero_mask | null_mask
        if replace_mask.any():
            result = np.trunc(
                _op.truediv(
                    left.astype(float),
                    right.where(~zero_mask, other=1.0),
                ),
            ).astype(object)
            result[replace_mask] = None
            return result
        # True-divide for precision, then truncate toward zero and restore int64.
        return np.trunc(_op.truediv(left, right)).astype("int64")

    # Handle null cases for non-integer division (but allow division by zero → infinity)
    null_mask = left.isna() | right.isna()
    if null_mask.any():
        # Float division by zero produces infinity, only null inputs produce null
        result = _op.truediv(left, right).astype(object)
        result[null_mask] = None
        return result

    return _op.truediv(left, right)


def _cypher_mod(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Compute remainder using openCypher (truncating-toward-zero) semantics.

    Python's ``%`` operator uses floored division (sign follows divisor), but
    Neo4j uses C-style truncating remainder (sign follows dividend):

    * ``-5 % 3  = -2``  (Neo4j) vs ``1`` (Python)
    * ``5  % -3 =  2``  (Neo4j) vs ``-1`` (Python)
    * ``x  % 0  = null`` (Neo4j) vs error/nan (Python/numpy)

    ``numpy.fmod`` implements the truncating remainder matching Neo4j behaviour.
    Zero divisors are post-hoc replaced with ``None``.

    Args:
        left: Left-hand dividend Series.
        right: Right-hand divisor Series.

    Returns:
        A Series of remainders with the sign of the dividend; ``object`` dtype
        when any divisor is zero or null, numeric dtype otherwise.
    """
    zero_mask = right == 0
    null_mask = left.isna() | right.isna()
    replace_mask = zero_mask | null_mask
    if replace_mask.any():
        # Use float intermediary to avoid numpy integer-mod-by-zero warnings
        arr = np.fmod(
            left.astype(float),
            right.where(~zero_mask, other=1.0),
        ).astype(object)
        result = pd.Series(arr, index=left.index)
        result[replace_mask] = None
        return result
    result = pd.Series(
        np.fmod(
            left.to_numpy(dtype=float, na_value=np.nan),
            right.to_numpy(dtype=float, na_value=np.nan),
        ),
        index=left.index,
    )
    # Preserve integer dtype when both inputs are integral
    if pd.api.types.is_integer_dtype(left) and pd.api.types.is_integer_dtype(
        right,
    ):
        return result.astype(left.dtype)
    return result


def _cypher_pow(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Raise *left* to the power *right* using openCypher semantics.

    * integer ^ negative_integer → float  (numpy rejects this; cast to float first)
    * all other combinations     → delegated to ``operator.pow``

    Args:
        left: Base Series.
        right: Exponent Series.

    Returns:
        A Series of powers; float dtype when the exponent contains negative
        values and the base is integer, numeric dtype otherwise.
    """
    import operator as _op

    # Handle null values
    null_mask = left.isna() | right.isna()

    if (
        pd.api.types.is_integer_dtype(left)
        and pd.api.types.is_integer_dtype(right)
        and (right < 0).any()
    ):
        result = _op.pow(left.astype(float), right)
    else:
        result = _op.pow(left, right)

    # Apply null handling if needed
    if null_mask.any():
        result = result.astype(object)
        result[null_mask] = None

    return result


def _first_non_null_val(series: FrameSeries) -> Any:
    """Get the first non-null value from a Series for type sampling.

    Uses early-terminating iteration over the underlying numpy array
    instead of ``series.dropna().iloc[0]``, which allocates a full copy
    of the Series just to read one element.  For the common case (first
    element is non-null), this is O(1) instead of O(n).

    Args:
        series: Input Series

    Returns:
        First non-null value, or None if all values are null
    """
    for v in series.values:
        if v is None:
            continue
        try:
            if v != v:  # NaN != NaN
                continue
        except (TypeError, ValueError):
            pass  # pd.NA raises TypeError; treat as null
        else:
            return v
    return None


_is_null_val = is_null_value  # Canonical null-check from shared.helpers


def _is_date_str(v: object) -> bool:
    """Return True when *v* is a ``'YYYY-MM-DD'`` ISO date string."""
    return isinstance(v, str) and len(v) == 10 and v[4] == "-" and v[7] == "-"


def _is_datetime_str(v: object) -> bool:
    """Return True when *v* is an ISO datetime string (contains ``'T'``)."""
    return isinstance(v, str) and "T" in v and len(v) >= 16 and v[4] == "-"


def _is_duration_dict(v: object) -> bool:
    """Return True when *v* is a duration component dict (has ``'days'`` key)."""
    return isinstance(v, dict) and "days" in v


def _is_temporal_val(v: object) -> bool:
    """Return True when *v* is any recognised temporal value."""
    return _is_date_str(v) or _is_datetime_str(v) or _is_duration_dict(v)


def _temporal_arith_pair(op: str, left: object, right: object) -> object:
    """Compute a single-row temporal arithmetic result for operator *op*.

    Handles all combinations of date strings, datetime strings, and duration
    dicts.  Returns ``None`` when either operand is null.

    Args:
        op: Arithmetic operator (``'+'`` or ``'-'``).
        left:  Left-hand value (date string, datetime string, or duration dict).
        right:  Right-hand value (date string, datetime string, or duration dict).

    Returns:
        A new date string, datetime string, or duration dict; ``None`` on null
        input or an unsupported type combination.

    Raises:
        TemporalArithmeticError: When temporal arithmetic is attempted on incompatible types
    """
    from datetime import date as _date_cls
    from datetime import datetime as _datetime_cls
    from datetime import timedelta

    if _is_null_val(left) or _is_null_val(right):
        return None

    import calendar

    def _zero_duration() -> dict[str, int]:
        return {
            "years": 0,
            "months": 0,
            "weeks": 0,
            "days": 0,
            "hours": 0,
            "minutes": 0,
            "seconds": 0,
            "milliseconds": 0,
            "microseconds": 0,
            "nanoseconds": 0,
        }

    def _delta_to_duration(delta: timedelta) -> dict[str, int]:
        d = _zero_duration()
        d["days"] = delta.days
        d["seconds"] = delta.seconds
        d["microseconds"] = delta.microseconds
        return d

    def _apply_duration_to_date(d: _date_cls, dur: dict[str, int], sign: int) -> _date_cls:
        """Add (*sign* = 1) or subtract (*sign* = -1) *dur* from date *d*.

        Month and year arithmetic applies first with end-of-month clamping, then
        day/hour/minute/second components are applied via :class:`timedelta`.
        """
        # 1. Month/year arithmetic (must come before timedelta to clamp correctly)
        total_months = d.month + sign * (
            dur.get("months", 0) + 12 * dur.get("years", 0)
        )
        target_year = d.year + (total_months - 1) // 12
        target_month = ((total_months - 1) % 12) + 1
        max_day = calendar.monthrange(target_year, target_month)[1]
        d = _date_cls(target_year, target_month, min(d.day, max_day))

        # 2. Day/time components via timedelta
        td = timedelta(
            days=sign * (dur.get("days", 0) + 7 * dur.get("weeks", 0)),
            hours=sign * dur.get("hours", 0),
            minutes=sign * dur.get("minutes", 0),
            seconds=sign * dur.get("seconds", 0),
            milliseconds=sign * dur.get("milliseconds", 0),
            microseconds=sign * dur.get("microseconds", 0),
        )
        return d + td

    def _apply_duration_to_datetime(dt: _datetime_cls, dur: dict[str, int], sign: int) -> _datetime_cls:
        """Add (*sign* = 1) or subtract (*sign* = -1) *dur* from datetime *dt*."""
        # Month/year arithmetic first
        total_months = dt.month + sign * (
            dur.get("months", 0) + 12 * dur.get("years", 0)
        )
        target_year = dt.year + (total_months - 1) // 12
        target_month = ((total_months - 1) % 12) + 1
        max_day = calendar.monthrange(target_year, target_month)[1]
        dt = dt.replace(
            year=target_year,
            month=target_month,
            day=min(dt.day, max_day),
        )

        # Remaining components via timedelta
        td = timedelta(
            days=sign * (dur.get("days", 0) + 7 * dur.get("weeks", 0)),
            hours=sign * dur.get("hours", 0),
            minutes=sign * dur.get("minutes", 0),
            seconds=sign * dur.get("seconds", 0),
            milliseconds=sign * dur.get("milliseconds", 0),
            microseconds=sign * dur.get("microseconds", 0),
        )
        return dt + td

    # date + duration  /  date - duration
    if _is_date_str(left) and _is_duration_dict(right):
        d = _date_cls.fromisoformat(cast(str, left))
        return _apply_duration_to_date(
            d,
            right,
            sign=1 if op == "+" else -1,
        ).isoformat()

    # duration + date  (commutative addition only)
    if _is_duration_dict(left) and _is_date_str(right) and op == "+":
        d = _date_cls.fromisoformat(cast(str, right))
        return _apply_duration_to_date(d, left, sign=1).isoformat()

    # date - date  → duration
    if _is_date_str(left) and _is_date_str(right) and op == "-":
        delta = _date_cls.fromisoformat(
            cast(str, left)
        ) - _date_cls.fromisoformat(cast(str, right))
        return _delta_to_duration(delta)

    # datetime + duration  /  datetime - duration
    if _is_datetime_str(left) and _is_duration_dict(right):
        dt = _datetime_cls.fromisoformat(cast(str, left))
        result = _apply_duration_to_datetime(
            dt,
            right,
            sign=1 if op == "+" else -1,
        )
        # Return naive ISO string without timezone suffix when input was naive
        return result.isoformat()

    # duration + datetime  (commutative addition only)
    if _is_duration_dict(left) and _is_datetime_str(right) and op == "+":
        dt = _datetime_cls.fromisoformat(cast(str, right))
        return _apply_duration_to_datetime(dt, left, sign=1).isoformat()

    # datetime - datetime  → duration
    if _is_datetime_str(left) and _is_datetime_str(right) and op == "-":
        delta = _datetime_cls.fromisoformat(
            cast(str, left),
        ) - _datetime_cls.fromisoformat(cast(str, right))
        return _delta_to_duration(delta)

    # duration + duration  /  duration - duration
    if _is_duration_dict(left) and _is_duration_dict(right):
        sign = 1 if op == "+" else -1
        left_dur = cast(dict[str, Any], left)
        right_dur = cast(dict[str, Any], right)
        result = _zero_duration()
        for key in result:
            result[key] = left_dur.get(key, 0) + sign * right_dur.get(key, 0)
        return result

    # Unsupported combination — raise a helpful TypeError rather than silently
    # returning null, which would hide user mistakes like `date + 7` (forgot
    # to wrap in duration()).
    left_type = type(left).__name__
    right_type = type(right).__name__
    from pycypher.exceptions import TemporalArithmeticError

    example = "date('2024-01-01') + duration({days: 7})"
    raise TemporalArithmeticError(op, left_type, right_type, example)


#: Binary arithmetic operators dispatch table
_ARITH_OPS: dict[str, Any] = {
    "+": _op.add,
    "-": _op.sub,
    "*": _op.mul,
    "/": _cypher_div,
    "%": _cypher_mod,
    "^": _cypher_pow,
}

#: Binary comparison operators dispatch table
_CMP_OPS: dict[str, Any] = {
    "=": _op.eq,
    "<>": _op.ne,
    "<": _op.lt,
    ">": _op.gt,
    "<=": _op.le,
    ">=": _op.ge,
}

#: Unary arithmetic operators dispatch table
_UNARY_OPS: dict[str, Any] = {
    "+": lambda s: s,
    "-": _op.neg,
}


class ArithmeticExpressionEvaluator:
    r"""Specialized evaluator for arithmetic and comparison expressions.

    This class handles all arithmetic operations (+, -, \*, /, %, ^),
    comparison operations (=, <>, <, >, <=, >=), and unary operations (+x, -x)
    that were previously part of the BindingExpressionEvaluator god object.

    The evaluator maintains identical semantics to the original implementation
    while providing a focused, testable interface for arithmetic operations.
    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialize arithmetic evaluator with binding frame context.

        Args:
            frame: BindingFrame providing variable and expression evaluation context
        """
        self.frame = frame

    def evaluate_arithmetic(
        self,
        op: str,
        left_expr: Any,
        right_expr: Any,
        expression_evaluator: ExpressionEvaluatorProtocol,  # Will be the main evaluator for recursive calls
    ) -> FrameSeries:
        r"""Evaluate a binary arithmetic expression.

        Handles arithmetic operations (+, -, \*, /, %, ^) with proper null handling,
        temporal arithmetic support, and Cypher-compliant semantics.

        Args:
            op: Arithmetic operator string
            left_expr: Left operand expression
            right_expr: Right operand expression
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with arithmetic results

        Raises:
            UnsupportedOperatorError: If operator is not supported
            TypeError: If operands have incompatible types
        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug("arithmetic: op=%r  rows=%d", op, len(self.frame))
        left = expression_evaluator.evaluate(left_expr)
        right = expression_evaluator.evaluate(right_expr)

        # Handle temporal arithmetic before dispatching to numeric operators
        left_sample = _first_non_null_val(left)
        right_sample = _first_non_null_val(right)
        if _is_temporal_val(left_sample) or _is_temporal_val(right_sample):
            result = left.combine(
                right,
                lambda left, r: _temporal_arith_pair(op, left, r),
            )
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "arithmetic: op=%r  elapsed=%.4fs",
                    op,
                    time.perf_counter() - _t0,
                )
            return result

        handler = _ARITH_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op, list(_ARITH_OPS), category="arithmetic"
            )

        try:
            result = handler(left, right)
        except TypeError as exc:
            left_type = type(_first_non_null_val(left)).__name__
            right_type = type(_first_non_null_val(right)).__name__
            raise IncompatibleOperatorError(
                operator=op,
                left_type=left_type,
                right_type=right_type,
                suggestion="Both operands must be of compatible numeric or string types.",
            ) from exc
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "arithmetic: op=%r  elapsed=%.4fs",
                op,
                time.perf_counter() - _t0,
            )
        return result

    def evaluate_comparison(
        self,
        op: str,
        left_expr: Any,
        right_expr: Any,
        expression_evaluator: ExpressionEvaluatorProtocol,  # Will be the main evaluator for recursive calls
    ) -> FrameSeries:
        """Evaluate a binary comparison expression.

        Handles comparison operations (=, <>, <, >, <=, >=) with proper
        three-valued logic (null handling) consistent with Cypher semantics.

        Args:
            op: Comparison operator string
            left_expr: Left operand expression
            right_expr: Right operand expression
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Boolean Series with comparison results

        Raises:
            UnsupportedOperatorError: If operator is not supported
        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug("comparison: op=%r  rows=%d", op, len(self.frame))
        left = expression_evaluator.evaluate(left_expr)
        right = expression_evaluator.evaluate(right_expr)

        handler = _CMP_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op, list(_CMP_OPS), category="comparison"
            )

        # Three-valued logic: any comparison involving null → null
        null_mask = left.isna() | right.isna()
        if null_mask.any():
            result = handler(left, right).astype(object)
            result[null_mask] = None
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "comparison: op=%r  elapsed=%.4fs",
                    op,
                    time.perf_counter() - _t0,
                )
            return result
        result = handler(left, right)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "comparison: op=%r  elapsed=%.4fs",
                op,
                time.perf_counter() - _t0,
            )
        return result

    def evaluate_unary(
        self,
        op: str,
        operand_expr: Any,
        expression_evaluator: ExpressionEvaluatorProtocol,  # Will be the main evaluator for recursive calls
    ) -> FrameSeries:
        """Evaluate a unary arithmetic operator.

        Handles unary operations (+x, -x) with proper null propagation.

        Args:
            op: Unary operator string (+ or -)
            operand_expr: Expression to apply operator to
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with unary operation results

        Raises:
            UnsupportedOperatorError: If operator is not supported
        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug("unary: op=%r  rows=%d", op, len(self.frame))
        series = expression_evaluator.evaluate(operand_expr)
        handler = _UNARY_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op, list(_UNARY_OPS), category="unary"
            )

        # Propagate null: object-dtype Series with None values cause TypeError
        # in operator.neg. Apply null-safe negation.
        null_mask = series.isna()
        if null_mask.any() and series.dtype == object:
            result = handler(series.fillna(0)).astype(object)
            result[null_mask] = None
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "unary: op=%r  elapsed=%.4fs",
                    op,
                    time.perf_counter() - _t0,
                )
            return result
        result = handler(series)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "unary: op=%r  elapsed=%.4fs",
                op,
                time.perf_counter() - _t0,
            )
        return result
