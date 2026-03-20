"""
Scalar Function Evaluator for Cypher Scalar and Graph Introspection Functions.

This module provides the ScalarFunctionEvaluator class, extracted from BindingExpressionEvaluator
as part of Architecture Loop Phase 5. It handles evaluation of scalar functions including
graph introspection functions that require pre-argument evaluation.

Architecture Loop Phase 5 Note:
This extraction follows the proven delegation pattern established in previous loops,
maintaining 100% backward compatibility while achieving clear separation of concerns.
"""

import logging
import time
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol
from shared.logger import LOGGER

from pycypher.aggregation_evaluator import KNOWN_AGGREGATIONS
from pycypher.ast_models import Variable
from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX, BindingFrame
from pycypher.constants import _broadcast_series
from pycypher.types import FrameSeries

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

# Graph internal columns that are excluded from user-visible results
_GRAPH_INTERNAL_COLS: frozenset[str] = frozenset(
    {"__ID__", "__SOURCE__", "__TARGET__"},
)


class ScalarFunctionEvaluator:
    """
    Evaluates Cypher scalar functions and graph introspection functions.

    This class handles the evaluation of scalar function calls, with special handling
    for graph introspection functions that need to access variable metadata before
    argument evaluation. Functions handled include:

    * **Graph Introspection Functions** (pre-evaluation):
        - labels(n) → returns entity type label list
        - type(r) → returns relationship type string
        - keys(n) → returns property column names (excluding internal columns)
        - properties(n) → returns property dict (with shadow layer support)
        - startNode(r) → returns relationship source node ID
        - endNode(r) → returns relationship target node ID

    * **Path Length Function**:
        - length(path_var) → returns hop count from variable-length path

    * **Registry Delegation**: All other scalar functions are delegated to
      ScalarFunctionRegistry for processing.

    * **Aggregation Prevention**: Prevents aggregation functions from being
      used in scalar expression contexts.
    """

    def __init__(self, frame: BindingFrame) -> None:
        """
        Initialize scalar function evaluator with binding frame context.

        Args:
            frame: The BindingFrame containing variable bindings, type registry,
                and context for function evaluation.
        """
        self.frame = frame

    def evaluate_scalar_function(
        self,
        func_name: str,
        func_args: Any,
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """
        Main entry point for scalar function evaluation.

        Handles the following function families before delegating to the
        ScalarFunctionRegistry:

        * **``length(path_var)``** — returns the hop-count column stored by
          the variable-length-path expander (``_path_hops_<var>`` column).
        * **``labels(n)``** — returns ``[["Person"], …]`` using the
          type registry; does not touch the entity table.
        * **``type(r)``** — returns the relationship type string per row.
        * **``keys(n)``** — returns the list of user-visible property column
          names for the entity or relationship type, excluding internal
          ``__ID__``/``__SOURCE__``/``__TARGET__`` columns.
        * **``properties(n)``** — returns a ``dict`` of all user-visible
          properties per row, reading from the shadow layer if available.
        * **``startNode(r)`` / ``endNode(r)``** — returns the source or
          target node ID of each relationship row.

        If the function name matches a known aggregation (``count``, ``sum``,
        etc.) it raises :exc:`ValueError` to prevent accidental use in scalar
        contexts (e.g. ``WHERE count(*) > 3`` is invalid Cypher).

        All other functions are passed to the
        :class:`~pycypher.scalar_functions.ScalarFunctionRegistry` for
        dispatch (built-in scalar functions such as ``toInteger``, ``toLower``,
        ``substring``, etc., plus any user-registered functions).

        Args:
            func_name: The Cypher function name (case-insensitive matching is
                applied internally).
            func_args: Raw argument value from the AST — either a list of
                :class:`~pycypher.ast_models.Expression` nodes or a single
                expression; normalised to a list by
                :func:`_normalize_func_args`.
            expression_evaluator: Reference to the parent BindingExpressionEvaluator
                for recursive expression evaluation.

        Returns:
            A ``pd.Series`` of per-row results.

        Raises:
            ValueError: If *func_name* is an aggregation function used in a
                scalar context, or if the function is unknown to the registry.
        """
        _t0 = time.perf_counter()
        _nrows = len(self.frame) if hasattr(self.frame, "__len__") else -1
        if _DEBUG_ENABLED:
            LOGGER.debug("scalar_function: %s  rows=%d", func_name, _nrows)

        from pycypher.constants import _normalize_func_args

        name: str = func_name
        name_lower: str = name.lower()
        arg_expressions: list[Any] = _normalize_func_args(func_args)

        # --- Graph introspection & special-case handlers ---
        # Each handler returns a pd.Series on match or None to pass through.
        # Tried in order (lazy); first non-None result wins.
        _handlers: tuple[tuple[str, Any], ...] = (
            ("path_length", lambda: self._eval_path_length(name, arg_expressions)),
            ("labels", lambda: self._eval_labels(name_lower, arg_expressions)),
            ("type", lambda: self._eval_type(name_lower, arg_expressions)),
            ("keys", lambda: self._eval_keys(name_lower, arg_expressions)),
            ("properties", lambda: self._eval_properties(name_lower, arg_expressions)),
            ("start_end_node", lambda: self._eval_start_end_node(name_lower, arg_expressions)),
            ("min_max", lambda: self._eval_min_max_special_case(name_lower, arg_expressions, expression_evaluator)),
        )
        for handler_name, handler_fn in _handlers:
            result = handler_fn()
            if result is not None:
                if _DEBUG_ENABLED:
                    LOGGER.debug(
                        "scalar_function: %s  handler=%s  elapsed=%.4fs",
                        func_name,
                        handler_name,
                        time.perf_counter() - _t0,
                    )
                return result

        # --- Aggregation guard ---
        self._validate_not_aggregation(name_lower, name)

        # --- Registry fallback for standard scalar functions ---
        result = self._eval_registry_function(
            name,
            arg_expressions,
            expression_evaluator,
        )
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "scalar_function: %s  handler=registry  elapsed=%.4fs",
                func_name,
                time.perf_counter() - _t0,
            )
        return result

    def _eval_path_length(
        self,
        name: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate length(path_var) for variable-length path hop counts.

        Returns the hop count column stored by _expand_variable_length_path.

        Args:
            name: Function name
            arg_expressions: Function arguments

        Returns:
            Series of hop counts if this is a path length call, None otherwise.
        """
        if (
            name.lower() == "length"
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            path_col = f"{PATH_HOP_COLUMN_PREFIX}{arg_expressions[0].name}"
            if path_col in self.frame.bindings.columns:
                return self.frame.bindings[path_col].reset_index(drop=True)
        return None

    def _eval_labels(
        self,
        name_lower: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate labels(n) to return entity type label list.

        Returns a list-per-row containing the entity type label(s).

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments

        Returns:
            Series of label lists if this is a labels call, None otherwise.
        """
        if (
            name_lower == "labels"
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            var_name = arg_expressions[0].name
            entity_type = self.frame.type_registry.get(var_name)
            if entity_type is not None:
                n = len(self.frame)
                return _broadcast_series([entity_type], n)
        return None

    def _eval_type(
        self,
        name_lower: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate type(r) to return relationship type string.

        Returns the relationship type string for every row.

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments

        Returns:
            Series of type strings if this is a type call, None otherwise.
        """
        if (
            name_lower == "type"
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            var_name = arg_expressions[0].name
            rel_type = self.frame.type_registry.get(var_name)
            if rel_type is not None:
                n = len(self.frame)
                return _broadcast_series(rel_type, n)
        return None

    def _eval_keys(
        self,
        name_lower: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate keys(n) to return property column names.

        Returns the list of user-visible property column names, excluding all
        internal columns (__ID__, __SOURCE__, __TARGET__). Checks both
        entity_mapping and relationship_mapping so that keys(r) works the same
        way as keys(n).

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments

        Returns:
            Series of property key lists if this is a keys call, None otherwise.
        """
        if (
            name_lower == "keys"
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            var_name = arg_expressions[0].name
            entity_type = self.frame.type_registry.get(var_name)
            if entity_type is not None:
                from pycypher.binding_frame import _source_to_pandas

                ctx = self.frame.context
                try:
                    if entity_type in ctx.entity_mapping.mapping:
                        raw_df = _source_to_pandas(
                            ctx.entity_mapping[entity_type].source_obj,
                        )
                    elif entity_type in ctx.relationship_mapping.mapping:
                        raw_df = _source_to_pandas(
                            ctx.relationship_mapping[entity_type].source_obj,
                        )
                    else:
                        n = len(self.frame)
                        return _broadcast_series([], n)
                    prop_keys = [
                        c
                        for c in raw_df.columns
                        if c not in _GRAPH_INTERNAL_COLS
                    ]
                    n = len(self.frame)
                    return _broadcast_series(prop_keys, n)
                except (KeyError, AttributeError):
                    if _DEBUG_ENABLED:
                        LOGGER.debug(
                            "keys() introspection failed for variable %r; falling through to registry",
                            var_name,
                            exc_info=True,
                        )
        return None

    def _eval_properties(
        self,
        name_lower: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate properties(n) to return property dictionary.

        Returns all user-visible properties of a node or relationship as a
        dict per row. Internal columns (__ID__, __SOURCE__, __TARGET__) are
        excluded so the caller sees only domain attributes. Supports shadow
        layer data when available.

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments

        Returns:
            Series of property dicts if this is a properties call, None otherwise.
        """
        if (
            name_lower == "properties"
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            var_name = arg_expressions[0].name
            entity_type = self.frame.type_registry.get(var_name)
            if entity_type is not None:
                from pycypher.binding_frame import _source_to_pandas
                from pycypher.constants import ID_COLUMN as _ID_COL

                ctx = self.frame.context
                shadow = getattr(ctx, "_shadow", {})
                try:
                    if entity_type in ctx.entity_mapping.mapping:
                        raw_df = shadow.get(
                            entity_type,
                        ) or _source_to_pandas(
                            ctx.entity_mapping[entity_type].source_obj,
                        )
                    elif entity_type in ctx.relationship_mapping.mapping:
                        raw_df = _source_to_pandas(
                            ctx.relationship_mapping[entity_type].source_obj,
                        )
                    else:
                        n = len(self.frame)
                        return _broadcast_series({}, n)

                    prop_cols = [
                        c
                        for c in raw_df.columns
                        if c not in _GRAPH_INTERNAL_COLS
                    ]
                    # Build an id → props dict lookup using vectorised
                    # to_dict('index') — 10-20× faster than iterrows() on
                    # large entity tables because the dict construction is
                    # handled in Cython rather than a Python loop.
                    if prop_cols:
                        prop_df = raw_df[prop_cols + [_ID_COL]].set_index(
                            _ID_COL,
                        )
                        props_lookup = prop_df.to_dict("index")
                    else:
                        # No properties, return empty dicts
                        props_lookup = {rid: {} for rid in raw_df[_ID_COL]}

                    id_series = self.frame.bindings[var_name]
                    return (
                        id_series.map(props_lookup)
                        .fillna({})
                        .reset_index(drop=True)
                    )
                except (KeyError, AttributeError):
                    if _DEBUG_ENABLED:
                        LOGGER.debug(
                            "properties() introspection failed for variable %r; falling through to registry",
                            var_name,
                            exc_info=True,
                        )
        return None

    def _eval_start_end_node(
        self,
        name_lower: str,
        arg_expressions: list[Any],
    ) -> pd.Series | None:
        """
        Evaluate startNode(r) / endNode(r) to return relationship endpoints.

        Returns the source (__SOURCE__) or target (__TARGET__) node ID for
        each relationship in the binding frame. The relationship variable
        binds to __ID__ values, so we build an id → source/target lookup.

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments

        Returns:
            Series of node IDs if this is a startNode/endNode call, None otherwise.
        """
        if (
            name_lower in {"startnode", "endnode"}
            and len(arg_expressions) == 1
            and isinstance(arg_expressions[0], Variable)
        ):
            var_name = arg_expressions[0].name
            rel_type = self.frame.type_registry.get(var_name)
            if rel_type is not None:
                from pycypher.binding_frame import _source_to_pandas
                from pycypher.constants import ID_COLUMN as _ID_COL
                from pycypher.constants import (
                    RELATIONSHIP_SOURCE_COLUMN as _SRC_COL,
                )
                from pycypher.relational_models import (
                    RELATIONSHIP_TARGET_COLUMN as _TGT_COL,
                )

                ctx = self.frame.context
                try:
                    raw_df = _source_to_pandas(
                        ctx.relationship_mapping[rel_type].source_obj,
                    )
                    endpoint_col = (
                        _SRC_COL if name_lower == "startnode" else _TGT_COL
                    )
                    lookup: pd.Series = raw_df.set_index(_ID_COL)[endpoint_col]
                    id_series = self.frame.bindings[var_name]
                    return id_series.map(lookup).reset_index(drop=True)
                except (KeyError, AttributeError):
                    if _DEBUG_ENABLED:
                        LOGGER.debug(
                            "startNode/endNode introspection failed for variable %r; falling through to registry",
                            var_name,
                            exc_info=True,
                        )
        return None

    def _eval_min_max_special_case(
        self,
        name_lower: str,
        arg_expressions: list[Any],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> pd.Series | None:
        """
        Handle min/max special case for list arguments.

        min/max are dual-purpose: aggregation over rows *or* scalar over a list.
        When called with a list-valued argument (e.g. min([1, 2, 3])) or with
        a null literal (e.g. min(null) → null), dispatch to the scalar registry
        instead of raising an aggregation error.

        Args:
            name_lower: Lowercase function name
            arg_expressions: Function arguments
            expression_evaluator: Parent evaluator for recursive evaluation

        Returns:
            Series result if this is a scalar min/max call, None otherwise.
        """
        if name_lower in {"min", "max"} and len(arg_expressions) == 1:
            _probe = expression_evaluator.evaluate(arg_expressions[0])
            _non_null = _probe.dropna()
            _is_list_arg = len(_non_null) > 0 and isinstance(
                _non_null.iloc[0],
                list,
            )
            # Object-dtype all-null means the argument was the `null` literal, not
            # a column of numeric values that are all missing.
            _is_null_literal = len(_non_null) == 0 and _probe.dtype == object
            if _is_list_arg or _is_null_literal:
                func_meta = (
                    expression_evaluator.scalar_registry._functions.get(
                        name_lower,
                    )
                )
                if func_meta is not None:
                    return func_meta.callable(_probe)
        return None

    def _validate_not_aggregation(self, name_lower: str, name: str) -> None:
        """
        Validate that the function is not an aggregation function in scalar context.

        Args:
            name_lower: Lowercase function name
            name: Original function name for error message

        Raises:
            WrongCypherTypeError: If the function is a known aggregation function
                used in a scalar expression context.
        """
        if name_lower in KNOWN_AGGREGATIONS:
            from pycypher.exceptions import WrongCypherTypeError

            raise WrongCypherTypeError(
                f"'{name}' is an aggregation function and cannot be used in a scalar "
                f"expression context (e.g. WHERE clause). Use it in RETURN or WITH instead."
            )

    def _eval_registry_function(
        self,
        name: str,
        arg_expressions: list[Any],
        expression_evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """
        Delegate to ScalarFunctionRegistry for standard scalar function evaluation.

        This handles all functions not covered by the special cases above,
        including standard scalar functions like toInteger, toLower, substring,
        etc., plus any user-registered functions.

        Args:
            name: Function name
            arg_expressions: Function arguments
            expression_evaluator: Parent evaluator for recursive evaluation

        Returns:
            Series result from ScalarFunctionRegistry.

        Raises:
            ValueError: If the function is unknown to the registry.
        """
        arg_series: list[pd.Series] = [
            expression_evaluator.evaluate(a) for a in arg_expressions
        ]

        # Zero-arg functions (e.g. rand(), pi(), e(), randomUUID()) must still
        # produce one value per row. Pass a dummy N-element series so the
        # implementation can key off len(s) to generate the right number of
        # outputs (e.g. N independent random values for rand()).
        if not arg_series:
            n = len(self.frame)
            dummy = pd.Series(range(n), dtype=int)
            func_meta = expression_evaluator.scalar_registry._functions.get(
                name.lower(),
            )
            if func_meta is not None and func_meta.max_args == 0:
                # True zero-arg function: call directly, bypassing arg-count check.
                return func_meta.callable(dummy)
            # Optional-arg function (min_args=0, max_args>=1): inject dummy.
            arg_series = [dummy]

        if _DEBUG_ENABLED:
            LOGGER.debug(
                msg=f"ScalarFunctionEvaluator: calling scalar '{name}' with {len(arg_series)} args",
            )
        return expression_evaluator.scalar_registry.execute(name, arg_series)
