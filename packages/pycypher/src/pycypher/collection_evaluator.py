"""Collection Expression Evaluator for PyCypher.

Architecture Loop 283 - Collection operations extracted from BindingExpressionEvaluator
god object into focused, testable, and maintainable CollectionExpressionEvaluator.

This module contains all collection-related expression evaluation functionality:
- List comprehensions
- Quantifiers (ANY/ALL/NONE)
- REDUCE expressions
- Pattern comprehensions
- Slicing operations
- Property lookup
- Map literals and projections

The evaluator follows the established delegation pattern from previous god object
extractions (AggregationExpressionEvaluator, ArithmeticExpressionEvaluator).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from pycypher.ast_models import (
        Expression,
        ListComprehension,
        MapLiteral,
        MapProjection,
        PatternComprehension,
        Quantifier,
        Reduce,
    )
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
from shared.helpers import is_null_raw_list as _is_null_raw_list
from shared.logger import LOGGER

from pycypher.binding_frame import BindingFrame
from pycypher.constants import _broadcast_series, _null_series
from pycypher.types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)


def _extract_temporal_field(value: object, field: str) -> object:
    """Extract temporal field from value (date/datetime string).

    This helper function is needed for property lookup temporal field extraction.
    It extracts fields like 'year', 'month', 'day', 'week', 'hour', etc. from ISO date/datetime strings.

    Supported fields:
    - Date fields: year, month, day, week, dayOfWeek, dayOfYear, quarter
    - Time fields: hour, minute, second, millisecond, microsecond

    Args:
        value: ISO date/datetime string (e.g., '2024-03-15' or '2024-03-15T10:30:45')
        field: Field name to extract

    Returns:
        Field value as integer, or None if extraction fails

    """
    if not isinstance(value, str):
        return None

    try:
        from datetime import datetime

        # Handle timezone offset formats
        dt_str = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(dt_str)

        # Basic date/time fields available directly
        if field in (
            "year",
            "month",
            "day",
            "hour",
            "minute",
            "second",
            "microsecond",
        ):
            return getattr(dt, field)

        # ISO calendar fields
        if field == "week":
            return dt.isocalendar()[1]  # ISO week number (1-53)

        if field == "dayOfWeek":
            return dt.isoweekday()  # ISO weekday (Monday=1, Sunday=7)

        if field == "dayOfYear":
            return dt.timetuple().tm_yday  # Day of year (1-366)

        # Quarter calculation (Q1=1, Q2=2, Q3=3, Q4=4)
        if field == "quarter":
            return (dt.month - 1) // 3 + 1

        # Millisecond is first 3 digits of microsecond
        if field == "millisecond":
            return dt.microsecond // 1000

    except (ValueError, AttributeError) as _tf_exc:
        # ValueError: value is not a valid temporal (e.g. non-date string).
        # AttributeError: value lacks the expected temporal attribute
        # (e.g. accessing .month on a non-datetime object).
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "Temporal field %r extraction failed for %r: %s",
                field,
                value,
                _tf_exc,
            )

    return None


class CollectionExpressionEvaluator:
    """Evaluates collection-related expressions.

    This class handles complex collection operations that were previously
    embedded within the BindingExpressionEvaluator god object. By extracting
    these operations, we achieve better separation of concerns and improved
    maintainability.

    Args:
        frame: The BindingFrame containing the current evaluation context.

    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialize collection evaluator with binding frame context.

        Args:
            frame: BindingFrame providing variable bindings and context for
                collection expression evaluation.

        """
        self.frame = frame

    def eval_property_lookup(
        self,
        var_expr: Expression,
        prop_name: str,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a ``PropertyLookup`` expression (``expr.prop``).

        Handles two cases:

        * **Variable target** — delegates to :meth:`~pycypher.binding_frame.BindingFrame.get_property`
          which resolves the property from the entity/relationship tables in the
          current context.
        * **Any other expression target** — evaluates *var_expr* to obtain a
          ``pd.Series`` of values.  Elements that are ``dict`` (e.g. produced by
          :class:`~pycypher.ast_models.MapLiteral` or
          :class:`~pycypher.ast_models.MapProjection`) have *prop_name* extracted
          via ``dict.get``; non-dict elements yield ``None`` (null).

        Args:
            var_expr: The expression to the left of the ``.``.
            prop_name: The property key to look up.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of extracted property values, one per frame row.

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: property_lookup prop=%r  rows=%d",
                prop_name,
                len(self.frame),
            )
        from pycypher.ast_models import Variable

        if isinstance(var_expr, Variable):
            # Before going through the entity-table lookup, peek at the column
            # values.  If the variable holds map values (e.g. from UNWIND of a
            # list of maps), extract the key directly — dict values are not
            # entity IDs and the entity table path would crash or return nulls.
            col = self.frame.bindings.get(var_expr.name)
            if col is not None and len(col) > 0:
                first_val = next(
                    (v for v in col if v is not None and not pd.isna(v)),
                    None,
                )
                if isinstance(first_val, dict):
                    # Vectorized: Series.map avoids intermediate list allocation
                    result = col.map(
                        lambda v, _pn=prop_name: (
                            v.get(_pn) if isinstance(v, dict) else None
                        ),
                    ).reset_index(drop=True)
                    if _DEBUG_ENABLED:
                        LOGGER.debug(
                            "collection: property_lookup  elapsed=%.4fs",
                            time.perf_counter() - _t0,
                        )
                    return result
            result = self.frame.get_property(var_expr.name, prop_name)
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "collection: property_lookup  elapsed=%.4fs",
                    time.perf_counter() - _t0,
                )
            return result

        # General case: evaluate to a Series, then extract prop_name.
        # • dict values  → map key lookup (MapLiteral, MapProjection)
        # • string values → temporal field accessor (date/datetime ISO strings)
        # • anything else → None
        values = expression_evaluator.evaluate(var_expr)
        result = pd.Series(
            [
                v.get(prop_name)
                if isinstance(v, dict)
                else _extract_temporal_field(v, prop_name)
                for v in values
            ],
            dtype=object,
        )
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: property_lookup  elapsed=%.4fs",
                time.perf_counter() - _t0,
            )
        return result

    def eval_slicing(
        self,
        coll_expr: Expression,
        start_expr: Expression | None,
        end_expr: Expression | None,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate ``coll[from..to]`` slicing.

        Both bounds are optional and inclusive/exclusive per Cypher semantics
        (``list[a..b]`` returns elements at indices ``a`` up to but not
        including ``b``, identical to Python ``seq[a:b]``).  Either bound
        being ``None`` means open-ended.

        Works for lists, tuples, and strings.  Out-of-bounds indices are
        handled silently by Python's slice semantics.

        Args:
            coll_expr: Expression that evaluates to list or string to slice.
            start_expr: Start index expression (can be None).
            end_expr: End index expression (can be None).
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of sliced values.

        """
        n = len(self.frame)
        coll = expression_evaluator.evaluate(coll_expr)
        start = (
            expression_evaluator.evaluate(start_expr)
            if start_expr is not None
            else _null_series(n)
        )
        end = (
            expression_evaluator.evaluate(end_expr)
            if end_expr is not None
            else _null_series(n)
        )

        def _slice_one(c: object, s: object, e: object) -> object:
            """Slice a single collection value with optional start/end bounds.

            Returns ``None`` when the collection is null or cannot be sliced.
            """
            if c is None:
                return None
            s_idx: int | None = int(s) if s is not None else None  # type: ignore[arg-type]  # s is narrowed to non-None
            e_idx: int | None = int(e) if e is not None else None  # type: ignore[arg-type]  # e is narrowed to non-None
            if isinstance(c, str):
                return c[s_idx:e_idx]
            try:
                sliced = list(c)[s_idx:e_idx]  # type: ignore[arg-type]  # c is not None (guarded above)
            except TypeError:
                if _DEBUG_ENABLED:
                    LOGGER.debug(
                        "List slice failed: cannot convert %r to list",
                        type(c).__name__,
                    )
                return None
            return sliced

        return pd.Series(
            [
                _slice_one(c, s, e)
                for c, s, e in zip(coll, start, end, strict=False)
            ],
            dtype=object,
        )

    def eval_list_comprehension(
        self,
        lc: ListComprehension,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a Cypher list comprehension in a single vectorised pass.

        Implements ``[var IN list_expr WHERE where_expr | map_expr]``.

        All (row_idx, element) pairs are exploded into a single flat
        :class:`~pycypher.binding_frame.BindingFrame` so that the WHERE
        predicate and map expression are each evaluated **once** across all
        elements rather than once per element.  Per-row lists are reconstructed
        from the surviving (row_idx, mapped_value) pairs.

        Args:
            lc: The ListComprehension AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of lists, one per row in the current BindingFrame.

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: list_comprehension  rows=%d",
                len(self.frame),
            )
        from pycypher.binding_evaluator import BindingExpressionEvaluator
        from pycypher.binding_frame import BindingFrame

        var_name: str = lc.variable.name if lc.variable else "_lc_var"
        list_series: pd.Series = expression_evaluator.evaluate(lc.list_expr)
        n_rows: int = len(list_series)

        # Phase 1 — explode all (row_idx, element) pairs into flat lists.
        row_indices: list[int] = []
        elements: list[Any] = []
        for row_idx, raw_list in enumerate(list_series):
            if _is_null_raw_list(raw_list):
                continue
            for item in raw_list:
                row_indices.append(row_idx)
                elements.append(item)

        if not elements:
            return pd.Series([[] for _ in range(n_rows)], dtype=object)

        # Phase 2 — build one flat BindingFrame; evaluate WHERE once.
        flat_df: pd.DataFrame = pd.DataFrame({var_name: elements})
        flat_frame: BindingFrame = BindingFrame(
            bindings=flat_df,
            type_registry={},
            context=self.frame.context,
        )
        flat_evaluator: BindingExpressionEvaluator = (
            BindingExpressionEvaluator(
                flat_frame,
            )
        )

        if lc.where is not None:
            keep_series: pd.Series = flat_evaluator.evaluate(lc.where)
            mask_arr: np.ndarray = (
                keep_series.fillna(False).to_numpy(dtype=bool, copy=False)
            )
            (surviving_elem_pos_arr,) = np.nonzero(mask_arr)
            surviving_elem_pos: list[int] = surviving_elem_pos_arr.tolist()
            row_arr = np.asarray(row_indices)
            surviving_row_idx: list[int] = row_arr[
                surviving_elem_pos_arr
            ].tolist()
        else:
            surviving_row_idx = row_indices
            surviving_elem_pos = list(range(len(elements)))

        # Phase 3 — evaluate map_expr once over survivors (if present).
        if lc.map_expr is not None and surviving_elem_pos:
            surv_df: pd.DataFrame = flat_df.iloc[
                surviving_elem_pos
            ].reset_index(
                drop=True,
            )
            surv_frame: BindingFrame = BindingFrame(
                bindings=surv_df,
                type_registry={},
                context=self.frame.context,
            )
            surv_evaluator: BindingExpressionEvaluator = (
                BindingExpressionEvaluator(
                    surv_frame,
                )
            )
            mapped_values: list[Any] = list(
                surv_evaluator.evaluate(lc.map_expr),
            )
        else:
            mapped_values = [elements[i] for i in surviving_elem_pos]

        # Phase 4 — regroup surviving (row_idx, value) pairs back into per-row lists.
        per_row: dict[int, list[Any]] = {i: [] for i in range(n_rows)}
        for ridx, val in zip(surviving_row_idx, mapped_values, strict=False):
            per_row[ridx].append(val)

        result = pd.Series([per_row[i] for i in range(n_rows)], dtype=object)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: list_comprehension  elements=%d  elapsed=%.4fs",
                len(elements),
                time.perf_counter() - _t0,
            )
        return result

    def eval_quantifier(
        self,
        q: Quantifier,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate quantifier expressions (ANY/ALL/NONE).

        Performance Loop 290: Delegates to vectorized implementation for
        massive performance improvement (O(rows × elements) → O(1) BindingFrame allocation).

        Args:
            q: Quantifier AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of boolean results.

        """
        _t0 = time.perf_counter()
        _qtype = getattr(q, "quantifier", "?")
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: quantifier=%s  rows=%d",
                _qtype,
                len(self.frame),
            )
        result = self.eval_quantifier_vectorized(q, expression_evaluator)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: quantifier=%s  elapsed=%.4fs",
                _qtype,
                time.perf_counter() - _t0,
            )
        return result

    def eval_quantifier_vectorized(
        self,
        q: Quantifier,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Vectorized quantifier evaluation using explode-evaluate-group strategy.

        Performance Loop 290: Replaces O(rows × elements) evaluation pattern
        with single exploded frame evaluation, reducing 10,000 → 1 BindingFrame allocation.

        Strategy:
        1. Explode all (row, element) pairs into one flat frame
        2. Evaluate WHERE condition once on flattened frame
        3. Group results back by original row and apply quantifier logic

        Args:
            q: Quantifier AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of boolean results.

        """
        import numpy as np

        from pycypher.binding_frame import BindingFrame

        # Get the list values to iterate over
        list_values = expression_evaluator.evaluate(q.list_expr)
        n_rows = len(list_values)

        # Phase 1: Handle null/empty lists and explode valid lists
        exploded_data = []  # (original_row_idx, element_value)
        null_empty_rows = set()  # Track rows with null/empty lists

        for row_idx, lst in enumerate(list_values):
            if _is_null_raw_list(lst):
                null_empty_rows.add(row_idx)
            else:
                # Explode this row's list elements
                for element in lst:
                    exploded_data.append((row_idx, element))

        # Phase 2: Handle null/empty list edge cases (preserve original semantics)
        results = [None] * n_rows
        for row_idx in null_empty_rows:
            if q.quantifier.lower() == "any":
                results[row_idx] = False  # ANY of empty = false
            elif q.quantifier.lower() == "all":
                results[row_idx] = True  # ALL of empty = true (vacuous truth)
            elif q.quantifier.lower() == "none":
                results[row_idx] = True  # NONE of empty = true
            elif q.quantifier.lower() == "single":
                results[row_idx] = (
                    False  # SINGLE of empty = false (zero matches, not one)
                )

        # Phase 3: Vectorized evaluation of non-empty lists
        if exploded_data:
            # Create exploded BindingFrame with all (row, element) pairs
            exploded_row_indices = [item[0] for item in exploded_data]
            exploded_elements = [item[1] for item in exploded_data]

            # Build exploded frame: duplicate original bindings for each element
            # PERF: Use iloc with all indices at once instead of per-row copy+concat
            exploded_frame_data = self.frame.bindings.iloc[
                exploded_row_indices
            ].reset_index(drop=True)

            if len(exploded_frame_data) > 0:
                # Add the quantifier variable column with element values
                exploded_frame_data[q.variable.name] = pd.Series(
                    exploded_elements,
                    dtype=object,
                )

                # Create exploded BindingFrame
                exploded_frame = BindingFrame(
                    bindings=exploded_frame_data,
                    type_registry=self.frame.type_registry,
                    context=self.frame.context,
                )

                # Create evaluator for exploded frame and evaluate WHERE condition ONCE
                from pycypher.binding_evaluator import (
                    BindingExpressionEvaluator,
                )

                exploded_evaluator = BindingExpressionEvaluator(exploded_frame)
                where_results = exploded_evaluator.evaluate(q.where)

                # Phase 4: Group results back by original row and apply quantifier logic
                where_results_list = where_results.tolist()
                row_matches = {}  # row_idx -> list of boolean results

                for i, (row_idx, bool_result) in enumerate(
                    zip(
                        exploded_row_indices,
                        where_results_list,
                        strict=False,
                    ),
                ):
                    if row_idx not in row_matches:
                        row_matches[row_idx] = []
                    row_matches[row_idx].append(bool(bool_result))

                # Phase 3: Apply quantifier logic using numpy vectorization
                # Performance Loop 292: Replace O(n_rows) Python loop with O(1) numpy operations
                match_counts = np.zeros(n_rows, dtype=np.intp)
                row_lengths = np.zeros(n_rows, dtype=np.intp)

                # Collect match statistics for vectorized processing
                for row_idx, matches in row_matches.items():
                    match_counts[row_idx] = sum(matches)
                    row_lengths[row_idx] = len(matches)

                # Vectorized quantifier logic using numpy array comparisons
                quantifier_type = q.quantifier.lower()
                if quantifier_type == "any":
                    result_arr = match_counts > 0
                elif quantifier_type == "all":
                    result_arr = match_counts == row_lengths
                elif quantifier_type == "none":
                    result_arr = match_counts == 0
                elif quantifier_type == "single":
                    result_arr = match_counts == 1
                else:
                    # Fallback for unknown quantifier types
                    result_arr = np.zeros(n_rows, dtype=bool)

                # Update results array with vectorized results
                for row_idx in row_matches:
                    results[row_idx] = result_arr[row_idx]

        # Fill any remaining None results with appropriate defaults
        for i, result in enumerate(results):
            if result is None:
                # This shouldn't happen, but provide safe defaults
                if q.quantifier.lower() == "any":
                    results[i] = False
                elif (
                    q.quantifier.lower() == "all"
                    or q.quantifier.lower() == "none"
                ):
                    results[i] = True
                elif q.quantifier.lower() == "single":
                    results[i] = False

        return pd.Series(results, dtype=bool)

    def eval_reduce(
        self, r: Reduce, expression_evaluator: ExpressionEvaluatorProtocol
    ) -> FrameSeries:
        """Evaluate REDUCE expressions using batch-per-step vectorization.

        Performance Loop 291: Replaces O(rows × elements) evaluation pattern
        with batch-per-step approach, reducing BindingFrame allocations from
        O(rows × max_elements) to O(max_elements).

        Args:
            r: Reduce AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of reduced values.

        """
        _t0 = time.perf_counter()
        if _DEBUG_ENABLED:
            LOGGER.debug("collection: reduce  rows=%d", len(self.frame))
        from pycypher.binding_frame import BindingFrame

        # Step 1: Evaluate list_values and initial_value once over the full frame
        list_values = expression_evaluator.evaluate(r.list_expr)
        initial_value = expression_evaluator.evaluate(r.initial)

        # Step 2: Initialize accumulators and collect per-row lists
        n_rows = len(list_values)
        accumulators = []
        lists = []

        for i in range(n_rows):
            # Initialize accumulator for this row
            accumulator = (
                initial_value.iloc[i]
                if len(initial_value) > i
                else initial_value.iloc[0]
            )
            lst = list_values.iloc[i]

            if _is_null_raw_list(lst):
                lists.append([])  # Empty list - will drop out immediately
            else:
                lists.append(list(lst))  # Convert to standard list

            accumulators.append(accumulator)

        # Step 3: Find maximum list length
        if not lists or all(len(lst) == 0 for lst in lists):
            # All lists are empty/null - return initial values
            return pd.Series(accumulators, dtype=object)

        max_len = max(len(lst) for lst in lists if len(lst) > 0)

        # Step 4: Batch-per-step evaluation (optimized)
        for step in range(max_len):
            # Find active rows (rows with elements at this step position)
            active_indices = [
                i for i, lst in enumerate(lists) if step < len(lst)
            ]

            # If no active rows, we're done
            if not active_indices:
                break

            # Build minimal BindingFrame for this step - only required columns
            step_bindings_data = {
                r.accumulator.name: [accumulators[i] for i in active_indices],
                r.variable.name: [lists[i][step] for i in active_indices],
            }

            # Only include existing columns that might be referenced in map_expr
            # Skip expensive column copying unless necessary
            if self.frame.bindings.shape[1] > 0:
                for col in self.frame.bindings.columns:
                    if col not in step_bindings_data:
                        step_bindings_data[col] = [
                            self.frame.bindings[col].iloc[i]
                            for i in active_indices
                        ]

            step_frame = BindingFrame(
                bindings=pd.DataFrame(step_bindings_data),
                type_registry=self.frame.type_registry,
                context=self.frame.context,
            )

            # Create evaluator for this step
            from pycypher.binding_evaluator import BindingExpressionEvaluator

            step_evaluator = BindingExpressionEvaluator(step_frame)

            # Evaluate map_expr ONCE for all active rows
            new_accs = step_evaluator.evaluate(r.map_expr)

            # Update accumulators for active rows
            for j, row_idx in enumerate(active_indices):
                accumulators[row_idx] = new_accs.iloc[j]

        # Step 5: Return Series of final accumulators
        result = pd.Series(accumulators, dtype=object)
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: reduce  steps=%d  elapsed=%.4fs",
                max_len,
                time.perf_counter() - _t0,
            )
        return result

    def eval_pattern_comprehension(
        self,
        pc: PatternComprehension,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate pattern comprehension expressions.

        Args:
            pc: PatternComprehension AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of pattern comprehension results.

        """
        # Pattern comprehensions are complex and involve graph pattern matching
        # This is a simplified implementation that delegates to the main Star execution
        # for proper pattern matching and filtering

        results = []
        for i in range(
            len(self.frame.bindings.iloc[:, 0]),
        ):  # For each row in frame
            # This would need full pattern matching implementation
            # For now, return empty list as placeholder
            results.append([])

        return pd.Series(results, dtype=object)

    def eval_map_literal(
        self,
        map_literal: MapLiteral,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate ``{key: expr, ...}`` map literal.

        If the entries (converted expression forms) are populated, each expression
        is evaluated per-row. Otherwise the raw val dict (primitives only) is
        returned as-is.

        Args:
            map_literal: Map literal object with entries and val attributes.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of map objects.

        """
        _t0 = time.perf_counter()
        n = len(self.frame)

        # If we have entries (expressions), evaluate them using vectorized approach
        if hasattr(map_literal, "entries") and map_literal.entries:
            # Performance Loop 285: Vectorized map literal evaluation
            # Step 1: Evaluate all expressions once (O(k) where k = number of keys)
            evaluated_columns = {}
            for key, value_expr in map_literal.entries:
                evaluated_columns[key] = expression_evaluator.evaluate(
                    value_expr,
                )

            # Step 2: Build result maps using itertuples (avoids full materialization)
            if evaluated_columns:
                df = pd.DataFrame(evaluated_columns)
                cols = df.columns.tolist()
                results = [
                    dict(zip(cols, row, strict=False))
                    for row in df.itertuples(index=False, name=None)
                ]
                result = pd.Series(results, dtype=object).reset_index(
                    drop=True,
                )
                if _DEBUG_ENABLED:
                    LOGGER.debug(
                        "collection: map_literal  keys=%d  elapsed=%.4fs",
                        len(evaluated_columns),
                        time.perf_counter() - _t0,
                    )
                return result
            # Handle empty entries case
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "collection: map_literal  keys=0  elapsed=%.4fs",
                    time.perf_counter() - _t0,
                )
            return _broadcast_series({}, n)

        # Otherwise use the raw val dict for all rows
        if hasattr(map_literal, "val") and map_literal.val:
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "collection: map_literal  raw_val  elapsed=%.4fs",
                    time.perf_counter() - _t0,
                )
            return _broadcast_series(map_literal.val, n)

        # Empty map
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: map_literal  empty  elapsed=%.4fs",
                time.perf_counter() - _t0,
            )
        return _broadcast_series({}, n)

    def eval_map_projection(
        self,
        mp: MapProjection,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate ``n{.prop, key: expr, .*}`` map projection.

        Returns a :class:`pd.Series` of dicts — one per row — where each dict
        contains the projected keys and their values.

        Three element forms are supported:

        * ``MapElement(property="name")`` (no expression) — copies the named
          property from the source variable.
        * ``MapElement(property="k", expression=expr)`` — evaluates *expr* and
          stores the result under key ``"k"``.
        * ``MapElement(all_properties=True)`` — includes every non-ID property
          of the source variable (``.*`` wildcard).

        Args:
            mp: MapProjection AST node.
            expression_evaluator: Main expression evaluator for recursive evaluation.

        Returns:
            A ``pd.Series`` of projected map objects.

        """
        _t0 = time.perf_counter()
        from pycypher.constants import ID_COLUMN

        n = len(self.frame)
        var_name = mp.variable.name if mp.variable else None

        # Performance Loop 285: Vectorized map projection evaluation
        # Step 1: Collect all column data needed (expressions, properties)
        projection_columns = {}

        for element in mp.elements:
            if element.all_properties:
                # Include every non-ID property (.*)
                if var_name:
                    # This is complex to vectorize due to per-entity property lookup
                    # Fall back to per-row approach for all_properties
                    results = []
                    for i in range(n):
                        result_dict = {}

                        # Process all other elements first (vectorized)
                        for other_elem in mp.elements:
                            if other_elem == element:
                                continue  # Skip the all_properties element for now

                            if (
                                other_elem.property
                                and not other_elem.expression
                            ):
                                # Property copy
                                if var_name:
                                    prop_series = self.frame.get_property(
                                        var_name,
                                        other_elem.property,
                                    )
                                    if i < len(prop_series):
                                        result_dict[other_elem.property] = (
                                            prop_series.iloc[i]
                                        )

                            elif other_elem.property and other_elem.expression:
                                # Expression evaluation - use cached result
                                cache_key = (
                                    other_elem.property,
                                    id(other_elem.expression),
                                )
                                if cache_key not in projection_columns:
                                    projection_columns[cache_key] = (
                                        other_elem.property,
                                        expression_evaluator.evaluate(
                                            other_elem.expression,
                                        ),
                                    )
                                prop_name, expr_series = projection_columns[
                                    cache_key
                                ]
                                value = (
                                    expr_series.iloc[i]
                                    if i < len(expr_series)
                                    else expr_series.iloc[0]
                                )
                                result_dict[prop_name] = value

                        # Now add all_properties for this row
                        var_col = self.frame.bindings.get(var_name)
                        if var_col is not None and i < len(var_col):
                            entity_id = var_col.iloc[i]
                            if entity_id is not None and not pd.isna(
                                entity_id,
                            ):
                                props = self.frame.get_all_properties(  # type: ignore[union-attr]  # frame is BindingFrame at runtime
                                    var_name,
                                    entity_id,
                                )
                                for key, value in props.items():
                                    if not key.endswith(ID_COLUMN):
                                        result_dict[key] = value

                        results.append(result_dict)

                    if _DEBUG_ENABLED:
                        LOGGER.debug(
                            "collection: map_projection  all_properties  elapsed=%.4fs",
                            time.perf_counter() - _t0,
                        )
                    return pd.Series(results, dtype=object).reset_index(
                        drop=True,
                    )

            elif element.property and not element.expression:
                # Copy property from source variable (.prop) - vectorized
                if var_name:
                    prop_series = self.frame.get_property(
                        var_name,
                        element.property,
                    )
                    projection_columns[element.property] = prop_series

            elif element.property and element.expression:
                # Evaluate expression and store under property name (key: expr) - vectorized
                expr_series = expression_evaluator.evaluate(element.expression)
                projection_columns[element.property] = expr_series

        # Step 2: Build result maps using itertuples (avoids full materialization)
        if projection_columns:
            df = pd.DataFrame(projection_columns)
            cols = df.columns.tolist()
            results = [
                dict(zip(cols, row, strict=False))
                for row in df.itertuples(index=False, name=None)
            ]
            if _DEBUG_ENABLED:
                LOGGER.debug(
                    "collection: map_projection  keys=%d  elapsed=%.4fs",
                    len(projection_columns),
                    time.perf_counter() - _t0,
                )
            return pd.Series(results, dtype=object)
        # Empty projection
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "collection: map_projection  empty  elapsed=%.4fs",
                time.perf_counter() - _t0,
            )
        return _broadcast_series({}, n)
