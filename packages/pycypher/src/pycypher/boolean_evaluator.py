"""BooleanExpressionEvaluator - Specialized evaluator for boolean logic operations.

This module provides focused evaluation of boolean operations extracted from the
BindingExpressionEvaluator god object. This is Phase 2 of the systematic refactoring
to decompose the 2904-line evaluator into specialized, maintainable components.

Architecture Loop 277 - Phase 2 Implementation
"""

from __future__ import annotations

import logging
import operator as _op
import time
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from pycypher.ast_models import Expression
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
import pandas as pd
from shared.logger import LOGGER

from pycypher.binding_frame import BindingFrame
from pycypher.exceptions import UnsupportedOperatorError
from pycypher.cypher_types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)


def _bool_series(val: bool, n: int) -> pd.Series:
    """Create a length-*n* boolean Series without ``[val] * n`` list alloc."""
    return pd.Series(np.full(n, val), dtype=bool)


def kleene_and(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Kleene three-valued AND: null AND false = false; null AND true = null.

    Vectorised via numpy — no Python-level loop.  ``isna()`` catches all null
    sentinels (None, np.nan, pd.NA), making this more correct than the previous
    implementation which only checked ``None`` and NaN floats.
    """
    l_null = left.isna()
    r_null = right.isna()
    # Fill null positions with True before bool-cast so the cast itself is safe;
    # the result for those positions is overridden by the np.where logic below.
    l_false = ~l_null & ~left.where(~l_null, True).astype(bool)
    r_false = ~r_null & ~right.where(~r_null, True).astype(bool)
    return pd.Series(
        np.where(
            l_false | r_false,
            False,
            np.where(l_null | r_null, None, True),  # type: ignore[call-overload]  # numpy stub limitation
        ),
        dtype=object,
        index=left.index,
    )


def kleene_or(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Kleene three-valued OR: null OR true = true; null OR false = null.

    Vectorised via numpy — no Python-level loop.
    """
    l_null = left.isna()
    r_null = right.isna()
    # Fill null positions with False before bool-cast.
    l_true = ~l_null & left.where(~l_null, False).astype(bool)
    r_true = ~r_null & right.where(~r_null, False).astype(bool)
    return pd.Series(
        np.where(
            l_true | r_true,
            True,
            np.where(l_null | r_null, None, False),  # type: ignore[call-overload]  # numpy stub limitation
        ),
        dtype=object,
        index=left.index,
    )


def kleene_xor(left: FrameSeries, right: FrameSeries) -> FrameSeries:
    """Kleene three-valued XOR: null XOR anything = null.

    Vectorised via numpy — no Python-level loop.
    """
    l_null = left.isna()
    r_null = right.isna()
    l_bool = left.where(~l_null, False).astype(bool)
    r_bool = right.where(~r_null, False).astype(bool)
    return pd.Series(
        np.where(l_null | r_null, None, l_bool.values ^ r_bool.values),
        dtype=object,
        index=left.index,
    )


def kleene_not(s: FrameSeries) -> FrameSeries:
    """Kleene three-valued NOT: NOT true = false, NOT false = true, NOT null = null.

    Vectorised via numpy — no Python-level loop.  ``isna()`` catches all null
    sentinels (None, np.nan, pd.NA), consistent with the other Kleene functions.
    """
    null = s.isna()
    s_bool = s.where(~null, False).astype(bool)
    return pd.Series(
        np.where(null, None, ~s_bool.values),
        dtype=object,
        index=s.index,
    )


#: Boolean fold operators used by boolean chain evaluation.
#: Each entry is ``(binary_op, identity_element)``.  ``identity_element`` is the
#: result returned when the operand list is empty (True for AND, False for OR/XOR).
_BOOL_FOLD_OPS: dict[str, tuple[Any, bool]] = {
    "and": (_op.and_, True),
    "or": (_op.or_, False),
    "xor": (_op.xor, False),
}


class BooleanExpressionEvaluator:
    """Specialized evaluator for boolean logic expressions.

    This class handles all boolean operations (AND, OR, NOT, XOR) with proper
    Kleene three-valued logic semantics that were previously part of the
    BindingExpressionEvaluator god object.

    The evaluator maintains identical semantics to the original implementation
    while providing a focused, testable interface for boolean operations.
    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialize boolean evaluator with binding frame context.

        Args:
            frame: BindingFrame providing variable and expression evaluation context

        """
        self.frame = frame

    def evaluate_and(
        self,
        operands: list[Expression],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate Cypher AND — Kleene three-valued left-fold.

        Args:
            operands: List of expressions to AND together
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with boolean AND results using Kleene logic

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: AND  operands=%d  rows=%d",
                len(operands),
                len(self.frame),
            )
        if not operands:
            return _bool_series(True, len(self.frame))
        result = expression_evaluator.evaluate(operands[0])
        for operand in operands[1:]:
            result = kleene_and(result, expression_evaluator.evaluate(operand))
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: AND  elapsed=%.4fs", time.perf_counter() - _t0
            )
        return result

    def evaluate_or(
        self,
        operands: list[Expression],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate Cypher OR — Kleene three-valued left-fold.

        Args:
            operands: List of expressions to OR together
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with boolean OR results using Kleene logic

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: OR  operands=%d  rows=%d",
                len(operands),
                len(self.frame),
            )
        if not operands:
            return _bool_series(False, len(self.frame))
        result = expression_evaluator.evaluate(operands[0])
        for operand in operands[1:]:
            result = kleene_or(result, expression_evaluator.evaluate(operand))
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: OR  elapsed=%.4fs", time.perf_counter() - _t0
            )
        return result

    def evaluate_not(
        self,
        operand_expr: Expression,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate Cypher NOT expr — three-valued boolean negation.

        Three-valued logic: NOT true → false, NOT false → true,
        NOT null → null.  The WHERE clause's fillna(False) converts
        the null to false for filtering, correctly excluding null rows.

        Args:
            operand_expr: Expression to negate
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with boolean NOT results using Kleene logic

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug("boolean: NOT  rows=%d", len(self.frame))
        operand = expression_evaluator.evaluate(operand_expr)
        result = kleene_not(operand)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: NOT  elapsed=%.4fs", time.perf_counter() - _t0
            )
        return result

    def evaluate_xor(
        self,
        operands: list[Expression],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate Cypher XOR — Kleene three-valued left-fold (null XOR x = null).

        Args:
            operands: List of expressions to XOR together
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            Series with boolean XOR results using Kleene logic

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: XOR  operands=%d  rows=%d",
                len(operands),
                len(self.frame),
            )
        if not operands:
            return _bool_series(False, len(self.frame))
        result = expression_evaluator.evaluate(operands[0])
        for operand in operands[1:]:
            result = kleene_xor(result, expression_evaluator.evaluate(operand))
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: XOR  elapsed=%.4fs", time.perf_counter() - _t0
            )
        return result

    def evaluate_bool_chain(
        self,
        key: str,
        operands: list[Expression],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a multi-operand boolean fold using the boolean fold ops table.

        All operands are null-coerced to False before the binary operation
        so that missing values do not propagate through filter predicates.

        Args:
            key: One of "and", "or", or "xor"
            operands: List of Cypher AST expression nodes
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            A boolean Series of per-row results

        Raises:
            ValueError: If key is not a supported boolean operation

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: op=%r  operands=%d  rows=%d",
                key,
                len(operands),
                len(self.frame),
            )
        if key not in _BOOL_FOLD_OPS:
            raise UnsupportedOperatorError(
                key,
                list(_BOOL_FOLD_OPS),
                category="boolean",
            )

        bin_op, identity = _BOOL_FOLD_OPS[key]
        if not operands:
            return _bool_series(identity, len(self.frame))

        # Use null-safe evaluation (fillna(False)) for chain operations
        result = self._null_safe(expression_evaluator.evaluate(operands[0]))
        for operand in operands[1:]:
            result = bin_op(
                result,
                self._null_safe(expression_evaluator.evaluate(operand)),
            )
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "boolean: op=%r  elapsed=%.4fs",
                key,
                time.perf_counter() - _t0,
            )
        return result

    def _null_safe(self, series: FrameSeries) -> FrameSeries:
        """Convert null values to False for null-safe boolean operations.

        Args:
            series: Input Series that may contain null values

        Returns:
            Series with nulls replaced by False

        """
        return series.fillna(False)
