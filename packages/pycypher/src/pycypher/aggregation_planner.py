"""AggregationPlanner — aggregation detection and grouped evaluation.

Extracted from :mod:`pycypher.star` to isolate the cohesive aggregation
planning family (``_contains_aggregation``, ``_aggregate_items``) into
a focused, independently testable module.

Handles:

- Recursive aggregation detection in expression trees
- Three aggregation modes: no-agg, full-table, grouped
- Dual-purpose min/max function disambiguation
- Vectorised grouped aggregation with per-group fallback
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.aggregation_evaluator import KNOWN_AGGREGATIONS
from pycypher.ast_models import (
    BinaryExpression,
    CountStar,
    FunctionInvocation,
    ListLiteral,
    Not,
    NullCheck,
    Unary,
)
from pycypher.constants import _normalize_func_args
from pycypher.scalar_functions import ScalarFunctionRegistry

if TYPE_CHECKING:
    from pycypher.binding_frame import BindingFrame

#: Dual-purpose functions that are scalar when called with a list literal,
#: aggregation otherwise.
_DUAL_PURPOSE: frozenset[str] = frozenset({"min", "max"})

#: Graph-introspection function names that are NOT aggregations.
_KNOWN_GRAPH_FUNCTIONS: frozenset[str] = frozenset(
    {
        "labels",
        "type",
        "startnode",
        "endnode",
        "elementid",
        "nodes",
        "relationships",
    },
)


class AggregationPlanner:
    """Detects aggregations in expressions and evaluates projection items.

    Stateless — all state is passed via method parameters.
    """

    def contains_aggregation(self, expression: Any) -> bool:
        """Check recursively whether *expression* contains any aggregate function.

        Walks the full expression tree so that aggregations nested inside
        arithmetic or logical operators are detected correctly — e.g.
        ``count(p.name) + 1`` or ``sum(p.salary) * 2``.

        Args:
            expression: AST expression node to inspect.

        Returns:
            True if the expression or any descendant is an aggregation function.

        """
        if expression is None:
            return False

        # Base aggregation nodes
        if isinstance(expression, CountStar):
            return True

        if isinstance(expression, FunctionInvocation):
            func_name = expression.function_name
            if isinstance(func_name, str):
                func_name_lower = func_name.lower()

                # Dual-purpose functions (min/max): scalar when called with a
                # list literal, aggregation otherwise.
                if (
                    func_name_lower in _DUAL_PURPOSE
                    and func_name_lower in KNOWN_AGGREGATIONS
                ):
                    args = _normalize_func_args(expression.arguments)
                    if args and isinstance(args[0], ListLiteral):
                        # List literal → scalar path; recurse into args.
                        return self._contains_aggregation_in_func_args(
                            expression.arguments,
                        )
                    # Non-list argument → aggregation.
                    return True
                registry = ScalarFunctionRegistry.get_instance()
                if registry.has_function(func_name_lower):
                    # Known scalar function — still recurse into arguments in
                    # case they contain aggregations (e.g. toUpper(count(n))).
                    return self._contains_aggregation_in_func_args(
                        expression.arguments,
                    )
                if func_name_lower in KNOWN_AGGREGATIONS:
                    return True
                # Graph-introspection functions are handled as pre-evaluation
                # intercepts in BindingExpressionEvaluator._eval_scalar_function
                # rather than registered in the scalar-function registry.  They
                # are not aggregations; suppress the "Unknown function" warning.
                if func_name_lower in _KNOWN_GRAPH_FUNCTIONS:
                    return self._contains_aggregation_in_func_args(
                        expression.arguments,
                    )
                LOGGER.warning(
                    msg=f"Unknown function '{func_name}', treating as scalar function",
                )
                return self._contains_aggregation_in_func_args(
                    expression.arguments,
                )

        # Binary expressions: check both branches
        if isinstance(expression, BinaryExpression):
            if self.contains_aggregation(expression.left):
                return True
            if self.contains_aggregation(expression.right):
                return True
            # Or / And / Xor also carry an operands list
            operands = getattr(expression, "operands", [])
            return any(
                self.contains_aggregation(op) for op in (operands or [])
            )

        # Single-operand wrappers
        if isinstance(expression, (Not, NullCheck, Unary)):
            return self.contains_aggregation(
                getattr(expression, "operand", None),
            )

        return False

    def _contains_aggregation_in_func_args(self, arguments: Any) -> bool:
        """Return True if any argument expression contains an aggregation.

        Handles the ``{"arguments": [...]}`` / ``{"args": [...]}`` / bare-list
        shapes that ``FunctionInvocation.arguments`` can take.
        """
        for arg in _normalize_func_args(arguments):
            if self.contains_aggregation(arg):
                return True
        return False

    def _simple_projection(
        self,
        items: list[Any],
        frame: BindingFrame,
        evaluator: Any,
    ) -> pd.DataFrame:
        """Evaluate non-aggregation projection items, batching PropertyLookups.

        When multiple RETURN/WITH items are PropertyLookups on the same
        variable (e.g. ``RETURN p.name, p.age, p.email``), this method
        batches them through :meth:`BindingFrame.get_properties_batch` to
        amortize entity-type resolution and cache lookups across all
        properties of the same variable.

        Items that are not simple PropertyLookups are evaluated individually
        via the standard expression evaluator.

        Args:
            items: List of ``ReturnItem`` / ``WithItem`` nodes with ``.alias`` set.
            frame: Input :class:`~pycypher.binding_frame.BindingFrame`.
            evaluator: :class:`BindingExpressionEvaluator` for non-batch items.

        Returns:
            A ``pd.DataFrame`` with one column per item.

        """
        from pycypher.ast_models import PropertyLookup
        from pycypher.ast_models import Variable as _Var

        # Group PropertyLookup items by variable name for batching.
        # Only batch variables that are known entity/relationship types in the
        # type_registry — scalar variables (from UNWIND, WITH aliases, etc.)
        # must go through the standard evaluator path which handles map/dict
        # property access.
        var_prop_groups: dict[str, list[tuple[str, str]]] = {}
        non_batch_items: list[Any] = []

        for item in items:
            expr = item.expression
            if (
                isinstance(expr, PropertyLookup)
                and isinstance(expr.expression, _Var)
                and expr.expression.name in frame.type_registry
            ):
                var_name = expr.expression.name
                if var_name not in var_prop_groups:
                    var_prop_groups[var_name] = []
                var_prop_groups[var_name].append(
                    (item.alias, expr.property),
                )
            else:
                non_batch_items.append(item)

        result_columns: dict[str, Any] = {}

        # Batch-fetch grouped PropertyLookups on entity/relationship variables
        for var_name, alias_prop_pairs in var_prop_groups.items():
            prop_names = [prop for _, prop in alias_prop_pairs]
            batch_results = frame.get_properties_batch(var_name, prop_names)
            for alias, prop in alias_prop_pairs:
                result_columns[alias] = batch_results[prop].reset_index(
                    drop=True,
                )

        # Evaluate remaining items individually
        for item in non_batch_items:
            result_columns[item.alias] = evaluator.evaluate(
                item.expression,
            ).reset_index(drop=True)

        # Preserve original item ordering
        ordered_aliases = [item.alias for item in items]
        return pd.DataFrame(
            {alias: result_columns[alias] for alias in ordered_aliases},
        )

    def aggregate_items(
        self,
        items: list[Any],
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Evaluate projection items (with optional aggregation) against a BindingFrame.

        Alias inference must be complete on all items before calling this method.

        Handles three modes:

        * **No aggregation** — vectorized expression evaluation, one column per item.
        * **Full-table aggregation** — reduces all rows to a single aggregated row.
        * **Grouped aggregation** — groups by non-aggregation keys, computes
          aggregations per group.

        Args:
            items: List of ``ReturnItem`` / ``WithItem`` nodes with ``.alias`` set.
            frame: Input :class:`~pycypher.binding_frame.BindingFrame`.

        Returns:
            A plain ``pd.DataFrame``.

        """
        from pycypher.binding_evaluator import BindingExpressionEvaluator

        agg_items = [
            i for i in items if self.contains_aggregation(i.expression)
        ]
        non_agg_items = [
            i for i in items if not self.contains_aggregation(i.expression)
        ]
        evaluator = BindingExpressionEvaluator(frame)

        if not agg_items:
            # Simple projection — batch PropertyLookups on the same variable
            # to amortize entity-type resolution and cache lookups.
            return self._simple_projection(items, frame, evaluator)

        if not non_agg_items:
            # Full-table aggregation (no GROUP BY)
            return pd.DataFrame(
                {
                    item.alias: [
                        evaluator.evaluate_aggregation(item.expression),
                    ]
                    for item in agg_items
                },
            )

        # Grouped aggregation (vectorised path)
        # Evaluate GROUP BY key expressions once over the full frame.
        group_key_aliases = [item.alias for item in non_agg_items]
        group_df = pd.DataFrame(
            {
                item.alias: evaluator.evaluate(item.expression).reset_index(
                    drop=True,
                )
                for item in non_agg_items
            },
        )
        groupby_key = (
            group_key_aliases[0]
            if len(group_key_aliases) == 1
            else group_key_aliases
        )

        # Attempt a single-pass vectorised aggregate for each agg item.
        # Items whose expressions cannot be vectorised (e.g. Arithmetic
        # wrapping aggregations) fall back to the per-group evaluator loop.
        agg_series_map: dict[str, pd.Series] = {}
        fallback_items: list[Any] = []
        for item in agg_items:
            series = evaluator.evaluate_aggregation_grouped(
                item.expression,
                group_df,
                group_key_aliases,
            )
            if series is not None:
                agg_series_map[item.alias] = series
            else:
                fallback_items.append(item)

        # Compute the GroupBy object once and reuse for both the unique-group
        # skeleton and the per-group fallback iteration below.
        _groupby_obj = group_df.groupby(groupby_key, sort=False)

        # Build the unique-group skeleton DataFrame (one row per group, group
        # key columns first, in first-seen order matching groupby sort=False).
        unique_groups: pd.DataFrame = (
            _groupby_obj.first().reset_index()
        )[group_key_aliases]

        # Attach vectorised agg results — positional alignment is guaranteed
        # because both use sort=False against the same group_df.
        for alias, series in agg_series_map.items():
            unique_groups[alias] = series.values

        # Per-group fallback for any remaining items.
        if fallback_items:
            for item in fallback_items:
                unique_groups[item.alias] = None  # initialise column

            for group_vals, group_idx in _groupby_obj.groups.items():
                mask = pd.Series(False, index=range(len(frame)))
                mask.iloc[list(group_idx)] = True
                group_frame = frame.filter(mask)
                group_evaluator = BindingExpressionEvaluator(group_frame)

                # Locate the row in unique_groups that matches this group.
                if isinstance(group_vals, tuple):
                    row_mask = pd.Series(True, index=unique_groups.index)
                    for alias, val in zip(
                        group_key_aliases,
                        group_vals,
                        strict=False,
                    ):
                        row_mask &= unique_groups[alias] == val
                else:
                    row_mask = (
                        unique_groups[group_key_aliases[0]] == group_vals
                    )
                row_idx = unique_groups.index[row_mask]

                for item in fallback_items:
                    unique_groups.loc[row_idx, item.alias] = (
                        group_evaluator.evaluate_aggregation(item.expression)
                    )

        all_aliases = group_key_aliases + [item.alias for item in agg_items]
        return unique_groups[all_aliases]
