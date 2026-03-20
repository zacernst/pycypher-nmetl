"""AggregationExpressionEvaluator - Specialized evaluator for aggregation operations.

This module provides focused evaluation of aggregation operations extracted from the
BindingExpressionEvaluator god object. This is Phase 3 of the systematic refactoring
to decompose the 2904-line evaluator into specialized, maintainable components.

Architecture Loop 280 - Phase 3 Implementation
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from pycypher.ast_models import Arithmetic, CountStar, FunctionInvocation
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
from shared.logger import LOGGER

from pycypher.binding_frame import BindingFrame
from pycypher.constants import _normalize_func_args
from pycypher.types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)


def _agg_sum(values: FrameSeries) -> float | None:
    """Return the null-ignoring sum of *values*, or ``None`` for all-null input.

    Follows the Cypher specification: ``sum()`` over an all-null or empty
    Series returns ``null`` (``None``), not zero.

    Args:
        values: Numeric series to aggregate; may contain nulls.

    Returns:
        ``float`` sum, or ``None`` if every value is null/missing.

    """
    non_null = values.dropna()
    if non_null.empty:
        # Cypher spec: sum() over all-null or empty input returns null.
        return None
    result = non_null.sum()
    return float(result) if pd.notna(result) else None


def _agg_avg(values: FrameSeries) -> float | None:
    """Return the null-ignoring arithmetic mean of *values*.

    Args:
        values: Numeric series to average; may contain nulls.

    Returns:
        ``float`` mean, or ``None`` if every value is null/missing.

    """
    non_null = values.dropna()
    return float(non_null.mean()) if not non_null.empty else None


def _agg_min(values: FrameSeries) -> Any:
    """Return the minimum of *values*, ignoring nulls.

    Args:
        values: Series to find minimum of; may contain nulls.

    Returns:
        Minimum value, or ``None`` if all values are null.

    """
    non_null = values.dropna()
    return non_null.min() if not non_null.empty else None


def _agg_max(values: FrameSeries) -> Any:
    """Return the maximum of *values*, ignoring nulls.

    Args:
        values: Series to find maximum of; may contain nulls.

    Returns:
        Maximum value, or ``None`` if all values are null.

    """
    non_null = values.dropna()
    return non_null.max() if not non_null.empty else None


def _agg_percentile_cont(values: pd.Series, percentile: float) -> float | None:
    """Continuous percentile (linear interpolation), null-ignoring.

    Equivalent to :func:`pandas.Series.quantile` with
    ``interpolation='linear'`` (the pandas default) — matches Neo4j's
    ``percentileCont`` function exactly.

    Args:
        values: Numeric series to find percentile of.
        percentile: Percentile value in [0.0, 1.0].

    Returns:
        ``float`` percentile, or ``None`` if input is all-null.

    """
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.quantile(percentile))


def _agg_percentile_disc(values: pd.Series, percentile: float) -> float | None:
    """Discrete percentile (lower interpolation), null-ignoring.

    Selects the nearest actual dataset value at or below the target quantile,
    matching Neo4j's ``percentileDisc`` function exactly.

    Args:
        values: Numeric series to find percentile of.
        percentile: Percentile value in [0.0, 1.0].

    Returns:
        Actual value from the dataset, or ``None`` if input is all-null.

    """
    non_null = values.dropna()
    if non_null.empty:
        return None
    return float(non_null.quantile(percentile, interpolation="lower"))


def _agg_stdev(values: FrameSeries) -> float | None:
    """Sample standard deviation (ddof=1, Bessel's correction), null-ignoring.

    Returns ``None`` when there are fewer than 2 non-null values (undefined for
    sample size < 2).

    """
    non_null = values.dropna()
    return float(non_null.std()) if len(non_null) >= 2 else None


def _agg_stdevp(values: FrameSeries) -> float | None:
    """Population standard deviation (ddof=0), null-ignoring.

    Returns ``None`` when the input is empty (all-null or zero rows).

    """
    non_null = values.dropna()
    return float(non_null.std(ddof=0)) if not non_null.empty else None


#: Two-argument aggregations (value expression + percentile parameter).
_PERCENTILE_AGGREGATIONS: frozenset[str] = frozenset(
    {"percentilecont", "percentiledisc"},
)

#: Aggregation function dispatch table.  Keys are lower-case function names.
_AGG_OPS: dict[str, Any] = {
    "collect": lambda values: values.tolist(),
    "count": lambda values: int(values.notna().sum()),
    "sum": _agg_sum,
    "avg": _agg_avg,
    "min": _agg_min,
    "max": _agg_max,
    "stdev": _agg_stdev,
    "stdevp": _agg_stdevp,
}

#: Complete set of known aggregation function names for validation and error messages.
KNOWN_AGGREGATIONS: frozenset[str] = (
    frozenset(_AGG_OPS.keys()) | _PERCENTILE_AGGREGATIONS
)

# Arithmetic operators for aggregation arithmetic
_ARITH_OPS: dict[str, Any] = {
    "+": lambda left, r: left + r,
    "-": lambda left, r: left - r,
    "*": lambda left, r: left * r,
    "/": lambda left, r: left / r if r != 0 else None,
    "%": lambda left, r: left % r if r != 0 else None,
    "^": lambda left, r: left**r,
}


class AggregationExpressionEvaluator:
    """Specialized evaluator for aggregation expressions.

    This class handles all aggregation operations (SUM, AVG, MIN, MAX, COUNT,
    percentiles, standard deviation) that were previously part of the
    BindingExpressionEvaluator god object.

    The evaluator maintains identical semantics to the original implementation
    while providing a focused, testable interface for aggregation operations.
    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialize aggregation evaluator with binding frame context.

        Args:
            frame: BindingFrame providing variable and expression evaluation context
        """
        self.frame = frame

    def evaluate_aggregation(
        self,
        agg_expression: FunctionInvocation | CountStar | Arithmetic,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> Any:
        """Evaluate an aggregation expression and return a scalar value.

        Supports ``collect``, ``count`` / ``COUNT(*)``, ``sum``, ``avg``,
        ``min``, ``max``.  Also handles :class:`~pycypher.ast_models.Arithmetic`
        nodes whose branches contain aggregations (e.g. ``count(*) + 1`` or
        ``sum(p.salary) * 2``).

        Args:
            agg_expression: A :class:`~pycypher.ast_models.FunctionInvocation`,
                :class:`~pycypher.ast_models.CountStar`, or
                :class:`~pycypher.ast_models.Arithmetic` node.
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            A scalar aggregated value.

        Raises:
            ValueError: For unsupported aggregation functions or missing arguments.
        """
        from pycypher.ast_models import (
            Arithmetic,
            CountStar,
            FunctionInvocation,
        )

        _agg_type = type(agg_expression).__name__
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "aggregation dispatch: %s  frame_size=%d",
                _agg_type,
                len(self.frame),
        )

        if isinstance(agg_expression, CountStar):
            return len(self.frame)

        # Binary arithmetic wrapping aggregations: evaluate each branch as a
        # scalar and apply the operator.  Null-safe guards for division and
        # modulo are kept explicit; standard ops reuse _ARITH_OPS.
        if isinstance(agg_expression, Arithmetic):
            left_val = expression_evaluator._eval_as_scalar(
                agg_expression.left,
            )
            right_val = expression_evaluator._eval_as_scalar(
                agg_expression.right,
            )
            op = agg_expression.operator
            if op in ("/", "÷"):
                return left_val / right_val if right_val else None
            if op == "%":
                return left_val % right_val if right_val else None
            if op == "^":
                import math as _math

                return _math.pow(left_val, right_val)
            handler = _ARITH_OPS.get(op)
            if handler is None:
                from pycypher.exceptions import IncompatibleOperatorError

                raise IncompatibleOperatorError(
                    operator=op,
                    left_type=type(left_val).__name__,
                    right_type=type(right_val).__name__,
                    suggestion="Supported arithmetic operators: +, -, *, /, %, ^",
                )
            return handler(left_val, right_val)

        if not isinstance(agg_expression, FunctionInvocation):
            from pycypher.exceptions import ASTConversionError

            raise ASTConversionError(
                f"Expected FunctionInvocation or CountStar, "
                f"got {type(agg_expression).__name__}",
                node_type=type(agg_expression).__name__,
            )

        func_name_lower = agg_expression.function_name.lower()

        arguments = agg_expression.arguments or {}
        args = _normalize_func_args(arguments)
        # Fall back to the aggregation-specific "expression" key if present
        arg_expr: Any = (
            args[0]
            if args
            else (
                arguments.get("expression")
                if isinstance(arguments, dict)
                else None
            )
        )

        if arg_expr is None and func_name_lower != "count":
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name=func_name_lower,
                expected_args=1,
                actual_args=0,
                argument_description="an expression to aggregate",
            )

        if func_name_lower == "count" and arg_expr is None:
            return len(self.frame)

        if arg_expr is None:
            from pycypher.exceptions import FunctionArgumentError

            raise FunctionArgumentError(
                function_name=func_name_lower,
                expected_args=1,
                actual_args=0,
                argument_description="an expression to aggregate",
            )
        values = expression_evaluator.evaluate(arg_expr)

        # Respect the DISTINCT modifier: deduplicate before aggregating
        distinct: bool = bool(
            isinstance(agg_expression.arguments, dict)
            and agg_expression.arguments.get("distinct", False),
        )
        if distinct:
            values = values.dropna().drop_duplicates()

        # Two-argument percentile functions require the second argument to be
        # evaluated as the percentile parameter before dispatching.
        if func_name_lower in _PERCENTILE_AGGREGATIONS:
            if len(args) < 2:
                from pycypher.exceptions import FunctionArgumentError

                raise FunctionArgumentError(
                    func_name_lower,
                    2,
                    len(args),
                    "expression and percentile in [0.0, 1.0]",
                )
            pct_series = expression_evaluator.evaluate(args[1])
            pct_val = float(pct_series.iloc[0])
            if func_name_lower == "percentilecont":
                return _agg_percentile_cont(values, pct_val)
            return _agg_percentile_disc(values, pct_val)

        agg_handler = _AGG_OPS.get(func_name_lower)
        if agg_handler is None:
            from pycypher.exceptions import UnsupportedFunctionError

            from shared.helpers import suggest_close_match

            supported_functions = sorted(_AGG_OPS) + sorted(
                _PERCENTILE_AGGREGATIONS,
            )
            exc = UnsupportedFunctionError(
                func_name_lower,
                supported_functions,
                "aggregation",
            )
            hint = suggest_close_match(func_name_lower, supported_functions)
            if hint:
                exc.args = (f"{exc.args[0]}{hint}",)
            raise exc
        return agg_handler(values)

    def evaluate_aggregation_grouped(
        self,
        agg_expression: FunctionInvocation | CountStar,
        group_df: pd.DataFrame,
        group_key_aliases: list[str],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> pd.Series | None:
        """Vectorised grouped aggregation — one scalar per group.

        Args:
            agg_expression: A FunctionInvocation or CountStar aggregation node.
            group_df: DataFrame with the grouping structure.
            group_key_aliases: List of group key column aliases.
            expression_evaluator: Main expression evaluator for recursive evaluation

        Returns:
            A Series of aggregated values (one per group), or None if
            the aggregation function is unsupported.

        """
        from pycypher.ast_models import CountStar, FunctionInvocation

        if isinstance(agg_expression, CountStar):
            # Count(*) per group = group size
            grouped = group_df.groupby(
                group_key_aliases,
                sort=False,
                dropna=False,
            )
            return pd.Series(grouped.size().values)

        if not isinstance(agg_expression, FunctionInvocation):
            return None

        func_name_lower = agg_expression.function_name.lower()
        arguments = agg_expression.arguments or {}
        args = _normalize_func_args(arguments)
        arg_expr: Any = (
            args[0]
            if args
            else (
                arguments.get("expression")
                if isinstance(arguments, dict)
                else None
            )
        )

        if func_name_lower == "count":
            if arg_expr is None:
                # COUNT() with no argument = COUNT(*) = group size
                grouped = group_df.groupby(
                    group_key_aliases,
                    sort=False,
                    dropna=False,
                )
                return pd.Series(grouped.size().values)

            # Check for DISTINCT modifier in COUNT(DISTINCT expression)
            distinct: bool = bool(
                isinstance(agg_expression.arguments, dict)
                and agg_expression.arguments.get("distinct", False),
            )

            # COUNT(expression) per group = non-null count within each group
            values = expression_evaluator.evaluate(arg_expr)
            # PERF: Use assign() instead of copy() + column mutation
            temp_df = group_df.assign(__agg_value__=values)
            grouped = temp_df.groupby(
                group_key_aliases,
                sort=False,
                dropna=False,
            )["__agg_value__"]

            if distinct:
                # COUNT(DISTINCT expression) = count unique non-null values per group
                def _distinct_count(s: FrameSeries) -> int:
                    distinct_values = s.dropna().drop_duplicates()
                    return int(distinct_values.notna().sum())

                return pd.Series(grouped.agg(_distinct_count).values)
            # Regular COUNT(expression) = count non-null values per group
            return pd.Series(grouped.count().values)

        if arg_expr is None:
            return None

        values = expression_evaluator.evaluate(arg_expr)
        # PERF: Use assign() instead of copy() + column mutation
        temp_df = group_df.assign(__agg_value__=values)

        # Handle DISTINCT modifier for grouped aggregations
        distinct: bool = bool(
            isinstance(agg_expression.arguments, dict)
            and agg_expression.arguments.get("distinct", False),
        )
        grouped = temp_df.groupby(group_key_aliases, sort=False, dropna=False)[
            "__agg_value__"
        ]

        if func_name_lower in _PERCENTILE_AGGREGATIONS:
            if len(args) < 2:
                return None
            pct_val = float(expression_evaluator.evaluate(args[1]).iloc[0])

            def agg_fn(s: pd.Series, p: float = pct_val) -> float:
                if func_name_lower == "percentilecont":
                    return _agg_percentile_cont(s, p)
                return _agg_percentile_disc(s, p)

            return pd.Series(grouped.agg(agg_fn).values)

        agg_handler = _AGG_OPS.get(func_name_lower)
        if agg_handler is None:
            return None

        if distinct:

            def _distinct_agg(s: FrameSeries) -> Any:
                return agg_handler(s.dropna().drop_duplicates())

            return pd.Series(grouped.agg(_distinct_agg).values)

        # Fast path: use pandas-native aggregation strings / groupby methods
        # when possible for performance
        native_agg_map = {
            "sum": "sum",
            "avg": "mean",
            "min": "min",
            "max": "max",
        }

        if func_name_lower in native_agg_map:
            if func_name_lower == "sum":
                # pandas sum() returns 0 for all-null groups by default.
                # min_count=1 makes it return NaN instead, matching Cypher's
                # null semantics (sum over all-null → null).
                result = grouped.sum(min_count=1)
            else:
                pandas_agg = native_agg_map[func_name_lower]
                result = grouped.agg(pandas_agg)
            return pd.Series(result.values)

        # General case: apply the aggregation handler to each group
        return pd.Series(grouped.agg(agg_handler).values)
