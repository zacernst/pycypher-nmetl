"""ComparisonEvaluator — comparison, null-check, unary, and CASE expression evaluation.

Extracted from :mod:`pycypher.binding_evaluator` to isolate the "dispatch table
+ null propagation" family of operations into a focused, independently testable
module.

Handles:

- Binary comparison predicates (``=``, ``<>``, ``<``, ``>``, ``<=``, ``>=``)
- Null-check predicates (``IS NULL``, ``IS NOT NULL``)
- Unary arithmetic operators (``+``, ``-``)
- CASE expressions (searched and simple forms)
"""

from __future__ import annotations

import logging
import operator as _op
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.constants import _null_series
from pycypher.exceptions import UnsupportedOperatorError
from pycypher.types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

if TYPE_CHECKING:
    from pycypher.ast_models import Expression, WhenClause
    from pycypher.binding_frame import BindingFrame
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol

# ---------------------------------------------------------------------------
# Operator dispatch tables
# ---------------------------------------------------------------------------

#: Binary comparison operators.
_CMP_OPS: dict[str, Any] = {
    "=": _op.eq,
    "<>": _op.ne,
    "<": _op.lt,
    ">": _op.gt,
    "<=": _op.le,
    ">=": _op.ge,
}

#: Unary numeric operators.
_UNARY_OPS: dict[str, Any] = {
    "+": lambda s: s,
    "-": _op.neg,
}

#: Null-check operators (unary; applied to a Series).
_NULL_CHECK_OPS: dict[str, Any] = {
    "IS NULL": pd.Series.isna,
    "IS NOT NULL": pd.Series.notna,
}


class ComparisonEvaluator:
    """Evaluates comparison, null-check, unary, and CASE expressions.

    All methods accept an ``evaluator`` parameter — the parent
    :class:`~pycypher.binding_evaluator.BindingExpressionEvaluator` — so that
    sub-expressions can be recursively evaluated without circular imports.

    Attributes:
        frame: The :class:`BindingFrame` providing variable bindings.

    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialise with a binding frame.

        Args:
            frame: The :class:`BindingFrame` against which expressions are
                evaluated.

        """
        self.frame = frame

    # ------------------------------------------------------------------
    # Comparisons
    # ------------------------------------------------------------------

    def evaluate_comparison(
        self,
        op: str,
        left_expr: Expression,
        right_expr: Expression,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a binary comparison expression (``=``, ``<>``, ``<``, ``>``, etc.).

        Dispatches to the ``_CMP_OPS`` table, which maps operator strings to
        vectorised pandas comparison operations.

        Args:
            op: Comparison operator string (e.g. ``"="``, ``"<>"``, ``"<="``).
            left_expr: Left-hand operand expression.
            right_expr: Right-hand operand expression.
            evaluator: Parent expression evaluator for recursive evaluation.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported comparison operator.

        """
        left = evaluator.evaluate(left_expr)
        right = evaluator.evaluate(right_expr)
        handler = _CMP_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op,
                list(_CMP_OPS),
                category="comparison",
            )
        if _DEBUG_ENABLED and left.dtype != right.dtype:
            LOGGER.debug(
                "comparison %s: type coercion left=%s right=%s",
                op,
                left.dtype,
                right.dtype,
            )
        # Three-valued logic: any comparison involving null → null
        null_mask = left.isna() | right.isna()
        if null_mask.any():
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "comparison %s: null propagation for %d/%d rows",
                    op,
                    null_mask.sum(),
                    len(null_mask),
                )
            result = handler(left, right).astype(object)
            result[null_mask] = None
            return result
        return handler(left, right)

    # ------------------------------------------------------------------
    # Null checks
    # ------------------------------------------------------------------

    def evaluate_null_check(
        self,
        op: str,
        operand_expr: Expression,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate an ``IS NULL`` or ``IS NOT NULL`` predicate.

        Args:
            op: One of ``"IS NULL"`` or ``"IS NOT NULL"``.
            operand_expr: The expression whose nullness is tested.
            evaluator: Parent expression evaluator for recursive evaluation.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported null check operator.

        """
        series = evaluator.evaluate(operand_expr)
        handler = _NULL_CHECK_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op,
                list(_NULL_CHECK_OPS),
                category="null check",
            )
        return handler(series)

    # ------------------------------------------------------------------
    # Unary operators
    # ------------------------------------------------------------------

    def evaluate_unary(
        self,
        op: str,
        operand_expr: Expression,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a unary arithmetic operator (``+`` or ``-``).

        Unary ``+`` is a no-op; unary ``-`` negates the series.

        Args:
            op: Unary operator string — ``"+"`` or ``"-"``.
            operand_expr: The expression to apply the operator to.
            evaluator: Parent expression evaluator for recursive evaluation.

        Returns:
            A numeric ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported unary operator.

        """
        series = evaluator.evaluate(operand_expr)
        handler = _UNARY_OPS.get(op)
        if handler is None:
            raise UnsupportedOperatorError(
                op,
                list(_UNARY_OPS),
                category="unary",
            )
        # Propagate null: object-dtype Series with None values cause TypeError
        # in operator.neg (e.g. -null literal).  Apply null-safe negation.
        null_mask = series.isna()
        if null_mask.any() and series.dtype == object:
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "unary %s: null-safe path for %d null rows (object dtype)",
                    op,
                    null_mask.sum(),
                )
            result = handler(series.fillna(0)).astype(object)
            result[null_mask] = None
            return result
        return handler(series)

    # ------------------------------------------------------------------
    # CASE expression
    # ------------------------------------------------------------------

    def evaluate_case(
        self,
        case_expr: Expression | None,
        when_clauses: list[WhenClause],
        else_expr: Expression | None,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a CASE expression vectorially using ``pd.Series.where``.

        Supports both *searched* CASE (``CASE WHEN cond THEN val …``) and
        *simple* CASE (``CASE expr WHEN match THEN val …``).

        The algorithm iterates WHEN clauses in **reverse** and repeatedly
        applies::

            result = then_series.where(condition_series, result)

        so that the *first* matching WHEN clause wins.

        Args:
            case_expr: The *simple* CASE discriminant expression, or ``None``
                for a *searched* CASE.
            when_clauses: Ordered list of :class:`~pycypher.ast_models.WhenClause`
                nodes.
            else_expr: Optional ELSE expression; produces ``None`` when absent.
            evaluator: Parent expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of per-row CASE results.

        """
        n = len(self.frame)

        # Build the ELSE (default) series
        if else_expr is not None:
            result = evaluator.evaluate(else_expr)
        else:
            result = _null_series(n)

        # Evaluate the simple CASE discriminant once (if present)
        discriminant: pd.Series | None = (
            evaluator.evaluate(case_expr) if case_expr is not None else None
        )

        if _DEBUG_ENABLED:
            LOGGER.debug(
                "CASE: %d WHEN clauses, discriminant=%s, has_else=%s",
                len(when_clauses),
                discriminant is not None,
                else_expr is not None,
            )
        # Apply WHEN clauses in reverse so the first clause takes priority
        for clause in reversed(when_clauses):
            then = evaluator.evaluate(clause.result)

            if discriminant is not None:
                # Simple CASE: compare discriminant == clause.condition value
                match_val = evaluator.evaluate(clause.condition)
                cond = discriminant == match_val
            else:
                # Searched CASE: clause.condition is a boolean expression
                cond = evaluator.evaluate(clause.condition).fillna(False)

            # Where cond is True, use then; where False, keep result
            result = then.where(cond, result)

        return result
