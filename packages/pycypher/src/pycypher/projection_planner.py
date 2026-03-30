"""ProjectionPlanner — RETURN/WITH clause evaluation and projection modifiers.

Extracted from :mod:`pycypher.star` to isolate the cohesive projection
family (alias inference, disambiguation, RETURN evaluation, WITH evaluation,
DISTINCT/ORDER BY/SKIP/LIMIT modifiers) into a focused, independently
testable module.

Handles:

- Alias inference from expression AST nodes (Variable, PropertyLookup, etc.)
- Alias disambiguation for colliding inferred names
- RETURN clause evaluation (projection + aggregation + modifiers)
- WITH clause evaluation (projection + aggregation + WHERE + modifiers)
- DISTINCT, ORDER BY (ASC/DESC with NULLS FIRST/LAST), SKIP, LIMIT
"""

from __future__ import annotations

from collections import Counter as _Counter
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pandas as pd
from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.aggregation_planner import AggregationPlanner
    from pycypher.binding_frame import BindingFrame
    from pycypher.expression_renderer import ExpressionRenderer


class ProjectionPlanner:
    """Evaluates RETURN and WITH clauses, applying projection modifiers.

    Stateless aside from injected dependencies — all mutable state is
    passed via method parameters.

    Args:
        agg_planner: :class:`~pycypher.aggregation_planner.AggregationPlanner`
            for aggregation detection and grouped evaluation.
        renderer: :class:`~pycypher.expression_renderer.ExpressionRenderer`
            for generating human-readable expression text.
        where_fn: Callback to apply a WHERE filter to a frame.  Signature:
            ``(where_expr, result_frame, fallback_frame=None) -> BindingFrame``.

    """

    def __init__(
        self,
        agg_planner: AggregationPlanner,
        renderer: ExpressionRenderer,
        where_fn: Callable[..., BindingFrame],
    ) -> None:
        """Initialise the projection planner.

        Args:
            agg_planner: Detects aggregation expressions and drives
                grouped evaluation in WITH/RETURN clauses.
            renderer: Generates human-readable column names when no
                explicit ``AS`` alias is provided.
            where_fn: Callback to apply a WHERE filter to a
                :class:`BindingFrame`.

        """
        self._agg_planner = agg_planner
        self._renderer = renderer
        self._where_fn = where_fn

    def infer_alias(self, expression: Any) -> str | None:
        """Infer a display alias from an expression when none is given.

        For ``Variable`` returns the variable name, for ``PropertyLookup``
        returns the property name (unqualified), and for all other expression
        types delegates to the expression renderer which generates a
        human-readable text representation.  Returns ``None`` only for
        expression types that have no sensible short representation.

        Args:
            expression: AST expression node to inspect.

        Returns:
            A string alias, or ``None`` if no representation is available.

        """
        from pycypher.ast_models import PropertyLookup
        from pycypher.ast_models import Variable as _Var

        if isinstance(expression, _Var):
            return expression.name
        if isinstance(expression, PropertyLookup):
            return expression.property
        return self._renderer.render(expression)

    def qualify_alias(self, expression: Any) -> str | None:
        """Return a fully-qualified ``'var.prop'`` alias for a PropertyLookup.

        Used by :meth:`return_from_frame` to disambiguate colliding inferred
        aliases.  Returns ``None`` for any expression type other than
        ``PropertyLookup(Variable, prop)``.

        Args:
            expression: AST expression node.

        Returns:
            A ``"var.prop"`` string, or ``None``.

        """
        from pycypher.ast_models import PropertyLookup
        from pycypher.ast_models import Variable as _Var

        if isinstance(expression, PropertyLookup) and isinstance(
            expression.expression,
            _Var,
        ):
            return f"{expression.expression.name}.{expression.property}"
        return None

    def apply_projection_modifiers(
        self,
        df: pd.DataFrame,
        clause: Any,
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Apply DISTINCT, ORDER BY, SKIP, and LIMIT to a projected DataFrame.

        Called at the end of both :meth:`return_from_frame` and
        :meth:`with_to_binding_frame` so that both RETURN and WITH honour the
        full set of projection modifiers defined in the AST.

        Args:
            df: The fully-projected result DataFrame (aliases as columns).
            clause: AST ``Return`` or ``With`` node carrying the modifiers.
            frame: The pre-projection :class:`~pycypher.binding_frame.BindingFrame`
                used to evaluate ORDER BY expressions that reference original
                entity variables.

        Returns:
            A new ``pd.DataFrame`` with modifiers applied.

        """
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame

        # ── DISTINCT ─────────────────────────────────────────────────────────
        if getattr(clause, "distinct", False):
            df = df.drop_duplicates().reset_index(drop=True)

        # ── ORDER BY ─────────────────────────────────────────────────────────
        order_by = getattr(clause, "order_by", None)
        if order_by:
            # Evaluator that first tries the post-projection frame (aliases),
            # then falls back to the original pre-projection frame.
            post_frame = BindingFrame(
                bindings=df,
                type_registry={},
                context=frame.context,
            )
            sort_col_names: list[str] = []
            ascending_flags: list[bool] = []
            null_col_names: list[tuple[str, bool]] = []
            temp_df = df

            # Whether any item has an explicit NULLS FIRST/LAST clause.
            # When all placements are default (None = NULLS LAST), the plain
            # sort_values path is used to preserve compatibility with
            # unhashable-valued sort columns (e.g. list columns from collect()).
            has_explicit_nulls = any(
                getattr(oi, "nulls_placement", None) is not None
                for oi in order_by
            )

            for idx, order_item in enumerate(order_by):
                sort_col = f"__sort_{idx}__"
                try:
                    evaluator = BindingExpressionEvaluator(post_frame)
                    sort_series = evaluator.evaluate(
                        order_item.expression,
                    ).reset_index(drop=True)
                except (ValueError, KeyError) as _sort_exc:
                    # VariableNotFoundError (ValueError subclass) is expected
                    # when the ORDER BY expression references a variable that
                    # was not carried into the projected frame.  Fall back to
                    # the full pre-projection frame which still has all bindings.
                    LOGGER.debug(
                        "ORDER BY: expression evaluation failed on post-projection frame "
                        "(%s), falling back to pre-projection frame for sort key %d",
                        _sort_exc,
                        idx,
                    )
                    evaluator = BindingExpressionEvaluator(frame)
                    sort_series = evaluator.evaluate(
                        order_item.expression,
                    ).reset_index(drop=True)
                temp_df[sort_col] = sort_series.values[: len(temp_df)]
                sort_col_names.append(sort_col)
                ascending_flags.append(order_item.ascending)

                if has_explicit_nulls:
                    # Add a null indicator for per-column null placement:
                    # 0 = non-null, 1 = null.  Sorting ascending puts nulls
                    # last (NULLS LAST); descending puts nulls first (NULLS FIRST).
                    null_col = f"__null_{idx}__"
                    temp_df[null_col] = temp_df[sort_col].isna().astype(int)
                    nulls_first = (
                        getattr(order_item, "nulls_placement", None) == "first"
                    )
                    null_col_names.append((null_col, not nulls_first))

            if has_explicit_nulls:
                # Interleave [null_ind_0, sort_0, null_ind_1, sort_1, ...]
                interleaved_by: list[str] = []
                interleaved_asc: list[bool] = []
                for (null_col, null_asc), sort_col, val_asc in zip(
                    null_col_names,
                    sort_col_names,
                    ascending_flags,
                    strict=False,
                ):
                    interleaved_by.extend([null_col, sort_col])
                    interleaved_asc.extend([null_asc, val_asc])
                temp_df = temp_df.sort_values(
                    by=interleaved_by,
                    ascending=interleaved_asc,
                )
                drop_cols = sort_col_names + [nc for nc, _ in null_col_names]
            else:
                temp_df = temp_df.sort_values(
                    by=sort_col_names,
                    ascending=ascending_flags,
                )
                drop_cols = sort_col_names

            df = temp_df.drop(columns=drop_cols).reset_index(drop=True)

        # ── SKIP ─────────────────────────────────────────────────────────────
        # ``skip`` is an int (literal) or an Expression AST node (e.g.
        # Parameter).  Evaluate non-int values through ``frame``'s evaluator
        # (which holds the query's Context including any bound parameters).
        skip_val: Any = getattr(clause, "skip", None)
        if skip_val is not None:
            if not isinstance(skip_val, int):
                evaluator = BindingExpressionEvaluator(frame)
                skip_val = int(evaluator.evaluate(skip_val).iloc[0])
            df = df.iloc[int(skip_val) :].reset_index(drop=True)

        # ── LIMIT ────────────────────────────────────────────────────────────
        # Same int-or-Expression handling as SKIP above.
        limit_val: Any = getattr(clause, "limit", None)
        if limit_val is not None:
            if not isinstance(limit_val, int):
                evaluator = BindingExpressionEvaluator(frame)
                limit_val = int(evaluator.evaluate(limit_val).iloc[0])
            df = df.iloc[: int(limit_val)].reset_index(drop=True)

        return df

    def return_from_frame(
        self,
        return_clause: Any,
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Evaluate a RETURN clause against a BindingFrame and return results.

        Handles simple expression projection and aggregation (full-table and
        grouped).  Delegates to :class:`AggregationPlanner`.  Applies DISTINCT,
        ORDER BY, SKIP, and LIMIT after projection via
        :meth:`apply_projection_modifiers`.

        Args:
            return_clause: AST :class:`~pycypher.ast_models.Return` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        Returns:
            A plain ``pd.DataFrame`` with columns matching RETURN aliases.

        """
        items = return_clause.items

        # RETURN * — return all user-visible (non-internal) columns unchanged,
        # then apply any ORDER BY / SKIP / LIMIT modifiers.
        if not items:
            visible = [
                col
                for col in frame.bindings.columns
                if not col.startswith("_")
            ]
            result = frame.bindings[visible].reset_index(drop=True)
            return self.apply_projection_modifiers(
                result,
                return_clause,
                frame,
            )

        for item in items:
            if item.alias is None:
                item.alias = self.infer_alias(item.expression)

        # Disambiguate colliding inferred aliases.  When two or more items
        # share the same alias (e.g. ``p.name`` and ``f.name`` both infer to
        # ``"name"``), upgrade every colliding item to its fully-qualified
        # ``"var.prop"`` form so no column is silently dropped.  Explicit AS
        # aliases are never touched (they were set before this loop).
        alias_counts = _Counter(item.alias for item in items)
        for item in items:
            if alias_counts[item.alias] > 1:
                qualified = self.qualify_alias(item.expression)
                if qualified is not None:
                    item.alias = qualified

        result = self._agg_planner.aggregate_items(items, frame)
        return self.apply_projection_modifiers(result, return_clause, frame)

    def with_to_binding_frame(
        self,
        with_clause: Any,
        frame: BindingFrame,
    ) -> BindingFrame:
        """Translate a WITH clause into a new BindingFrame.

        Supports three modes:

        * **Simple projection** (no aggregations): evaluate each expression and
          build a new frame where the columns are the aliases.
        * **Full aggregation** (all items are aggregations): reduce all rows to
          a single aggregated row.
        * **Grouped aggregation** (mixed items): group by non-aggregation keys,
          then compute aggregations per group.

        The resulting frame has ``type_registry = {}`` since the columns are
        computed values rather than entity IDs.  ``Variable`` lookups in
        subsequent WHERE / RETURN clauses will fall through to plain column
        access in ``bindings``.

        Args:
            with_clause: AST :class:`~pycypher.ast_models.With` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        Returns:
            A new :class:`~pycypher.binding_frame.BindingFrame` with alias columns.

        """
        from pycypher.binding_frame import BindingFrame

        # WITH * — pass all bindings through unchanged (items is empty list)
        if not with_clause.items:
            result_frame = frame
            if with_clause.where is not None:
                result_frame = self._where_fn(
                    with_clause.where,
                    result_frame,
                )
            modified_df = self.apply_projection_modifiers(
                result_frame.bindings,
                with_clause,
                frame,
            )
            return BindingFrame(
                bindings=modified_df,
                type_registry=result_frame.type_registry,
                context=frame.context,
            )

        # Infer missing aliases
        for item in with_clause.items:
            if item.alias is None:
                item.alias = self.infer_alias(item.expression)

        # Preserve type_registry entries for variables passed through unchanged
        # (e.g. `WITH p, ...` where p is a graph entity).
        from pycypher.ast_models import Variable as _Var

        preserved_types: dict[str, str] = {}
        for item in with_clause.items:
            if isinstance(item.expression, _Var):
                src_name = item.expression.name
                alias = item.alias or src_name
                if src_name in frame.type_registry:
                    preserved_types[alias] = frame.type_registry[src_name]

        result_frame = BindingFrame(
            bindings=self._agg_planner.aggregate_items(
                with_clause.items,
                frame,
            ),
            type_registry=preserved_types,
            context=frame.context,
        )

        # Apply optional WHERE predicate.
        if with_clause.where is not None:
            result_frame = self._where_fn(
                with_clause.where,
                result_frame,
                fallback_frame=frame,
            )

        # Apply DISTINCT, ORDER BY, SKIP, LIMIT modifiers.
        modified_df = self.apply_projection_modifiers(
            result_frame.bindings,
            with_clause,
            frame,
        )
        return BindingFrame(
            bindings=modified_df,
            type_registry=preserved_types,
            context=frame.context,
        )
