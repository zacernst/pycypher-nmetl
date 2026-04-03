"""BindingExpressionEvaluator — expression evaluator for the BindingFrame IR.

Evaluates Cypher AST expressions against a :class:`~pycypher.binding_frame.BindingFrame`,
returning a ``pd.Series`` of per-row results.

Design
------

1. **No ``Relation`` dependency** — property lookups go through
   :meth:`~pycypher.binding_frame.BindingFrame.get_property` instead of joining a
   prefixed entity DataFrame.
2. **Clean return type** — returns a single ``pd.Series`` (not a tuple).
3. **``CaseExpression`` support** — handled via a vectorised ``pd.Series.where()``
   reduction (both searched ``CASE WHEN`` and simple ``CASE expr WHEN`` forms).
4. **Aggregation-aware** — ``evaluate_aggregation()`` returns a scalar for use in
   WITH and RETURN clause grouping.  Supported aggregation functions:
   ``collect``, ``count``, ``sum``, ``avg``, ``min``, ``max``,
   ``stdev`` (sample, ddof=1), ``stdevp`` (population, ddof=0),
   ``percentileCont(expr, p)`` (linear interpolation), and
   ``percentileDisc(expr, p)`` (lower/discrete interpolation).
5. **Null-safe boolean logic** — AND, OR, NOT, XOR are delegated to
   ``BooleanExpressionEvaluator`` which uses Kleene three-valued logic.
6. **Modular architecture** — arithmetic operations are delegated to
   ``ArithmeticExpressionEvaluator``, boolean logic to
   ``BooleanExpressionEvaluator``, aggregations to
   ``AggregationExpressionEvaluator``, collections to
   ``CollectionExpressionEvaluator``, and scalar functions to
   ``ScalarFunctionEvaluator``.
"""

from __future__ import annotations

import logging
import re
import time
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd
from shared.helpers import suggest_close_match
from shared.logger import LOGGER

# Canonical aggregation constant lives in aggregation_evaluator; re-export
# for backward compatibility with downstream importers (star.py, semantic_validator.py).
from pycypher.aggregation_evaluator import (
    KNOWN_AGGREGATIONS as KNOWN_AGGREGATIONS,
)
from pycypher.ast_models import (
    And,
    Arithmetic,
    BooleanLiteral,
    CaseExpression,
    Comparison,
    Exists,
    FloatLiteral,
    FunctionInvocation,
    IndexLookup,
    IntegerLiteral,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    MapLiteral,
    MapProjection,
    Not,
    NullCheck,
    NullLiteral,
    Or,
    Parameter,
    PatternComprehension,
    PropertyLookup,
    Quantifier,
    Reduce,
    Slicing,
    StringLiteral,
    StringPredicate,
    Unary,
    Variable,
    Xor,
)
from pycypher.constants import _normalize_func_args as _normalize_func_args
from pycypher.exceptions import VariableNotFoundError
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.cypher_types import FrameSeries

if TYPE_CHECKING:
    from pycypher.aggregation_evaluator import AggregationExpressionEvaluator
    from pycypher.arithmetic_evaluator import ArithmeticExpressionEvaluator
    from pycypher.ast_models import Expression
    from pycypher.binding_frame import BindingFrame
    from pycypher.boolean_evaluator import BooleanExpressionEvaluator
    from pycypher.collection_evaluator import CollectionExpressionEvaluator
    from pycypher.comparison_evaluator import ComparisonEvaluator
    from pycypher.exists_evaluator import ExistsEvaluator
    from pycypher.scalar_function_evaluator import ScalarFunctionEvaluator
    from pycypher.string_predicate_evaluator import StringPredicateEvaluator

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

# ---------------------------------------------------------------------------
# Scalar broadcast helper — avoids [val] * N Python list allocation
# ---------------------------------------------------------------------------


def _broadcast_scalar(
    val: object,
    n: int,
    *,
    dtype: type | str | None = None,
) -> pd.Series:
    """Create a length-*n* Series filled with *val*, avoiding ``[val] * n``.

    For numeric, boolean, and ``None`` scalars, uses ``np.full`` or
    ``np.empty`` + fill to skip the intermediate Python list that
    ``pd.Series([val] * n)`` must build.  For object-typed scalars
    (strings, lists, dicts) it uses ``np.empty(dtype=object)`` with a
    scalar assignment.

    Args:
        val: The scalar value to broadcast.
        n: Number of rows.
        dtype: Optional explicit dtype for the resulting Series.

    Returns:
        A ``pd.Series`` of length *n* with every element equal to *val*.

    """
    if n == 0:
        return pd.Series([], dtype=dtype or object)

    if val is None:
        arr = np.empty(n, dtype=object)
        arr[:] = None
        return pd.Series(arr, dtype=object)

    if dtype is not None:
        return pd.Series(np.full(n, val, dtype=dtype), dtype=dtype)

    if isinstance(val, bool):
        return pd.Series(np.full(n, val), dtype=bool)
    if isinstance(val, int):
        return pd.Series(np.full(n, val))
    if isinstance(val, float):
        return pd.Series(np.full(n, val))

    # Object-typed scalars (str, list, dict, etc.)
    # For containers (list, dict, tuple), numpy would try to unpack the value
    # on broadcast assignment, so we must assign element-by-element via .fill()
    # or use a Python-level fill.  For strings, scalar assignment works fine.
    arr = np.empty(n, dtype=object)
    if isinstance(val, (list, dict, tuple)):
        arr.fill(val)
    else:
        arr[:] = val
    return pd.Series(arr, dtype=object)


# ---------------------------------------------------------------------------
# ReDoS protection for the =~ regex operator
# ---------------------------------------------------------------------------

#: Maximum allowed regex pattern length (characters).
_MAX_REGEX_PATTERN_LENGTH: int = 1000

# Patterns known to cause catastrophic backtracking (nested quantifiers,
# alternation with overlap).  Detected via simple heuristics — not a full
# regex complexity analyser, but catches the most common ReDoS vectors.
_REDOS_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Nested quantifiers: (a+)+, (a*)+, (a+)*, (a*)*
    re.compile(r"\([^)]*[+*][^)]*\)[+*]"),
    # Alternation with overlapping repetition: (a|a)*
    re.compile(r"\(([^|)]+)\|(\1)\)[+*]"),
    # Quantifier on quantifier: a{1,100}{1,100}
    re.compile(r"\{[^}]+\}\s*\{[^}]+\}"),
)


def _validate_regex_pattern(pattern: str) -> None:
    """Raise ``ValueError`` if *pattern* could cause catastrophic backtracking.

    Checks:

    1. **Compilability** — rejects syntactically invalid regex.
    2. **Length limit** — caps pattern length to prevent memory abuse.
    3. **Known ReDoS vectors** — detects nested quantifiers and
       overlapping alternation that cause exponential backtracking.

    Args:
        pattern: User-supplied regex string from a Cypher ``=~`` predicate.

    Raises:
        ValueError: If the pattern is invalid, too long, or contains a
            known ReDoS vector.

    """
    if len(pattern) > _MAX_REGEX_PATTERN_LENGTH:
        msg = (
            f"Regex pattern exceeds maximum length of {_MAX_REGEX_PATTERN_LENGTH} "
            f"characters ({len(pattern)} given). This limit prevents "
            "denial-of-service via complex patterns."
        )
        raise ValueError(msg)

    try:
        re.compile(pattern)
    except re.error as exc:
        msg = f"Invalid regex pattern: {exc}"
        raise ValueError(msg) from exc

    for redos_re in _REDOS_PATTERNS:
        if redos_re.search(pattern):
            msg = (
                "Regex pattern contains a potentially dangerous construct "
                "(nested quantifiers or overlapping alternation) that could "
                "cause catastrophic backtracking. Simplify the pattern to "
                "avoid denial-of-service."
            )
            raise ValueError(msg)


# ---------------------------------------------------------------------------
# Operator dispatch tables
# ---------------------------------------------------------------------------
# Arithmetic operators have been moved to arithmetic_evaluator.py.
# Comparison, unary, and null-check operators have been moved to
# comparison_evaluator.py.
# Boolean fold operators and Kleene functions have been moved to
# boolean_evaluator.py.

# _GRAPH_INTERNAL_COLS has been moved to scalar_function_evaluator.py
# where it is actually used.


# ---------------------------------------------------------------------------
# Temporal field accessor dispatch table
# ---------------------------------------------------------------------------
# Maps openCypher temporal field names to callables that extract the field
# from a ``datetime.datetime`` or ``datetime.date`` object.  Fields that
# only exist on ``datetime`` (hour, minute, …) use ``getattr(d, f, None)``
# so they degrade gracefully when called on a ``date`` value.
#
# Usage: ``_TEMPORAL_FIELD_ACCESSORS[field](parsed_dt_or_date)``
#
#: Dispatch table for temporal field accessors: ``date.year``, ``datetime.hour`` etc.
_TEMPORAL_FIELD_ACCESSORS: dict[str, Any] = {
    "year": lambda d: d.year,
    "month": lambda d: d.month,
    "day": lambda d: d.day,
    "hour": lambda d: getattr(d, "hour", None),
    "minute": lambda d: getattr(d, "minute", None),
    "second": lambda d: getattr(d, "second", None),
    "millisecond": lambda d: (
        int(getattr(d, "microsecond", 0)) // 1000
        if getattr(d, "microsecond", None) is not None
        else None
    ),
    "microsecond": lambda d: getattr(d, "microsecond", None),
    "week": lambda d: d.isocalendar()[1],
    "dayOfWeek": lambda d: d.isoweekday(),
    "dayOfYear": lambda d: d.timetuple().tm_yday,
    "quarter": lambda d: (d.month - 1) // 3 + 1,
}


def _extract_temporal_field(value: object, field: str) -> object:
    """Extract *field* from a temporal ISO string value.

    Tries to parse *value* first as a ``datetime``, then as a ``date``.
    Returns ``None`` for non-string inputs, unknown fields, or unparseable
    strings.

    Args:
        value: A date/datetime ISO string (e.g. ``'2024-03-15'`` or
               ``'2024-03-15T10:30:00'``).
        field: The openCypher temporal field name (``'year'``, ``'month'``,
               ``'hour'``, etc.).

    Returns:
        The extracted field value as a Python int, or ``None`` if extraction
        is not possible.

    """
    if not isinstance(value, str):
        return None
    field_fn = _TEMPORAL_FIELD_ACCESSORS.get(field)
    if field_fn is None:
        return None
    from datetime import date, datetime

    # Try datetime first (richer field set)
    try:
        dt = datetime.fromisoformat(value)
        return field_fn(dt)
    except ValueError:
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "Temporal field %r: %r is not a valid datetime, trying date",
                field,
                value,
            )
    # Fall back to date
    try:
        d = date.fromisoformat(value)
        return field_fn(d)
    except ValueError:
        if _DEBUG_ENABLED:
            LOGGER.debug(
                "Temporal field extraction failed: %r is not a valid date/datetime string",
                value,
            )
        return None


# Aggregation dispatch helpers (_agg_sum, _agg_avg, etc.), _AGG_OPS, and
# _PERCENTILE_AGGREGATIONS now live exclusively in aggregation_evaluator.py.
# KNOWN_AGGREGATIONS is re-exported above for backward compatibility.


class BindingExpressionEvaluator:
    """Evaluates Cypher AST expressions against a :class:`BindingFrame`.

    All property lookups delegate to :meth:`BindingFrame.get_property`, which
    performs an ID-keyed join against the entity table stored in the context.
    No DataFrame column prefixes are ever used.

    Supported expression types:

    - ``PropertyLookup`` (``p.name``)
    - ``Variable`` references (``p``) — returns the ID column
    - Literal values (``IntegerLiteral``, ``FloatLiteral``, ``StringLiteral``,
      ``BooleanLiteral``, ``NullLiteral``, ``ListLiteral``)
    - ``Arithmetic`` operations (``+``, ``-``, ``*``, ``/``, ``%``, ``^``)
    - ``Unary`` operations (``+``, ``-``)
    - ``Comparison`` predicates (``=``, ``<>``, ``<``, ``>``, ``<=``, ``>=``)
    - ``NullCheck`` predicates (``IS NULL``, ``IS NOT NULL``)
    - ``StringPredicate`` (``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``, ``=~``, ``IN``)
    - Boolean logic (``And``, ``Or``, ``Not``, ``Xor``)
    - ``FunctionInvocation`` (dispatched through :class:`ScalarFunctionRegistry`)
    - ``CaseExpression`` (simple and searched CASE/WHEN/ELSE)
    - Graph introspection: ``labels(n)``, ``type(r)``, ``keys(n)``, ``properties(n)``
      (pre-evaluated intercepts that read the type registry before evaluating args)

    Attributes:
        frame: The :class:`BindingFrame` against which expressions are evaluated.
        scalar_registry: The singleton :class:`ScalarFunctionRegistry`.

    """

    def __init__(self, frame: BindingFrame) -> None:
        """Initialise the evaluator.

        Sub-evaluators are constructed lazily on first access via ``@property``
        descriptors.  This avoids the overhead of importing and instantiating
        all 5 sub-evaluator modules for every ``BindingExpressionEvaluator``
        construction — most queries only touch 1–2 of them.

        Args:
            frame: The :class:`BindingFrame` providing variable bindings and
                property-lookup access to the context.

        """
        self.frame = frame
        self.scalar_registry: ScalarFunctionRegistry = (
            ScalarFunctionRegistry.get_instance()
        )

        # Lazy-init backing fields for sub-evaluators (constructed on first access).
        # Typed as Optional[ConcreteType] via TYPE_CHECKING imports to enable
        # static analysis while keeping runtime imports lazy.
        self._arithmetic_evaluator: ArithmeticExpressionEvaluator | None = None
        self._boolean_evaluator: BooleanExpressionEvaluator | None = None
        self._aggregation_evaluator: AggregationExpressionEvaluator | None = (
            None
        )
        self._collection_evaluator: CollectionExpressionEvaluator | None = None
        self._comparison_evaluator: ComparisonEvaluator | None = None
        self._scalar_function_evaluator: ScalarFunctionEvaluator | None = None
        self._string_predicate_evaluator: StringPredicateEvaluator | None = (
            None
        )
        self._exists_evaluator: ExistsEvaluator | None = None

    @property
    def arithmetic_evaluator(self) -> ArithmeticExpressionEvaluator:
        """Lazy-init arithmetic expression evaluator."""
        if self._arithmetic_evaluator is None:
            from pycypher.arithmetic_evaluator import (
                ArithmeticExpressionEvaluator,
            )

            self._arithmetic_evaluator = ArithmeticExpressionEvaluator(
                self.frame,
            )
        return self._arithmetic_evaluator

    @property
    def boolean_evaluator(self) -> BooleanExpressionEvaluator:
        """Lazy-init boolean expression evaluator."""
        if self._boolean_evaluator is None:
            from pycypher.boolean_evaluator import BooleanExpressionEvaluator

            self._boolean_evaluator = BooleanExpressionEvaluator(self.frame)
        return self._boolean_evaluator

    @property
    def aggregation_evaluator(self) -> AggregationExpressionEvaluator:
        """Lazy-init aggregation expression evaluator."""
        if self._aggregation_evaluator is None:
            from pycypher.aggregation_evaluator import (
                AggregationExpressionEvaluator,
            )

            self._aggregation_evaluator = AggregationExpressionEvaluator(
                self.frame,
            )
        return self._aggregation_evaluator

    @property
    def collection_evaluator(self) -> CollectionExpressionEvaluator:
        """Lazy-init collection expression evaluator."""
        if self._collection_evaluator is None:
            from pycypher.collection_evaluator import (
                CollectionExpressionEvaluator,
            )

            self._collection_evaluator = CollectionExpressionEvaluator(
                self.frame,
            )
        return self._collection_evaluator

    @property
    def comparison_evaluator(self) -> ComparisonEvaluator:
        """Lazy-init comparison expression evaluator."""
        if self._comparison_evaluator is None:
            from pycypher.comparison_evaluator import ComparisonEvaluator

            self._comparison_evaluator = ComparisonEvaluator(self.frame)
        return self._comparison_evaluator

    @property
    def scalar_function_evaluator(self) -> ScalarFunctionEvaluator:
        """Lazy-init scalar function evaluator."""
        if self._scalar_function_evaluator is None:
            from pycypher.scalar_function_evaluator import (
                ScalarFunctionEvaluator,
            )

            self._scalar_function_evaluator = ScalarFunctionEvaluator(
                self.frame,
            )
        return self._scalar_function_evaluator

    @property
    def string_predicate_evaluator(self) -> StringPredicateEvaluator:
        """Lazy-init string predicate evaluator."""
        if self._string_predicate_evaluator is None:
            from pycypher.string_predicate_evaluator import (
                StringPredicateEvaluator,
            )

            self._string_predicate_evaluator = StringPredicateEvaluator()
        return self._string_predicate_evaluator

    @property
    def exists_evaluator(self) -> ExistsEvaluator:
        """Lazy-init exists/pattern-comprehension evaluator."""
        if self._exists_evaluator is None:
            from pycypher.exists_evaluator import ExistsEvaluator

            self._exists_evaluator = ExistsEvaluator(self.frame)
        return self._exists_evaluator

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def evaluate(self, expression: Expression) -> FrameSeries:
        """Evaluate *expression* and return a per-row result Series.

        The returned Series has the same length as ``self.frame`` and is
        indexed ``0 … N-1``.

        Args:
            expression: Any supported Cypher AST expression node.

        Returns:
            A ``pd.Series`` of per-row values.

        Raises:
            NotImplementedError: For expression types not yet handled.
            ValueError: For semantic errors (unknown variable, missing property).

        """
        if _DEBUG_ENABLED:
            _t0 = time.perf_counter()
            _expr_type = type(expression).__name__
            LOGGER.debug("evaluate: %s  rows=%d", _expr_type, len(self.frame))

        match expression:
            case PropertyLookup(expression=var_expr, property=prop_name):
                return self._eval_property_lookup(var_expr, prop_name)

            case Variable(name=var_name):
                return self._eval_variable(var_name)

            case Parameter(name=param_name):
                params = getattr(self.frame.context, "_parameters", {})
                if param_name not in params:
                    from pycypher.exceptions import MissingParameterError

                    example_usage = f"execute_query(..., parameters={{'{param_name}': value}})"
                    raise MissingParameterError(param_name, example_usage)
                val = params[param_name]
                return _broadcast_scalar(val, len(self.frame))

            case (
                IntegerLiteral(value=val)
                | FloatLiteral(value=val)
                | StringLiteral(
                    value=val,
                )
                | BooleanLiteral(value=val)
            ):
                return _broadcast_scalar(val, len(self.frame))

            case NullLiteral():
                return _broadcast_scalar(None, len(self.frame))

            case ListLiteral(elements=elements, value=val):
                if elements:
                    # Evaluate each element expression against the full frame,
                    # yielding one Series per element.  For a 2-row frame and
                    # [a, b], elem_series = [Series([a0,a1]), Series([b0,b1])].
                    # The resulting list-per-row is built by zipping: row 0 gets
                    # [a0, b0], row 1 gets [a1, b1].  This is the correct
                    # row-wise semantics for list literals containing variables.
                    elem_series = [self.evaluate(e) for e in elements]
                    # Stack element Series as columns, then extract rows as
                    # lists — one Cython pass via DataFrame.values.tolist()
                    # instead of O(n × m) Python iloc calls.
                    arr = np.column_stack(
                        [s.to_numpy(dtype=object) for s in elem_series],
                    )  # shape (n, m), dtype=object
                    return pd.Series(arr.tolist())
                lst = val
                return _broadcast_scalar(lst, len(self.frame))

            case MapLiteral() as ml:
                return self._eval_map_literal(ml)

            case Arithmetic(operator=op, left=left_expr, right=right_expr):
                return self._eval_arithmetic(op, left_expr, right_expr)

            case Comparison(operator=op, left=left_expr, right=right_expr):
                return self._eval_comparison(op, left_expr, right_expr)

            case NullCheck(operator=op, operand=operand_expr):
                return self._eval_null_check(op, operand_expr)

            case LabelPredicate(operand=operand_expr, labels=labels):
                return self._eval_label_predicate(operand_expr, labels)

            case StringPredicate(
                operator=op,
                left=left_expr,
                right=right_expr,
            ):
                return self._eval_string_predicate(op, left_expr, right_expr)

            case And(operands=operands):
                return self._eval_and(operands)

            case Or(operands=operands):
                return self._eval_or(operands)

            case Not(operand=operand_expr):
                return self._eval_not(operand_expr)

            case Xor(operands=operands):
                return self._eval_xor(operands)

            case Unary(operator=op, operand=operand_expr):
                return self._eval_unary(op, operand_expr)

            case FunctionInvocation(arguments=func_args) as func_inv:
                return self._eval_scalar_function(
                    func_inv.function_name,
                    func_args,
                )

            case CaseExpression(
                expression=case_expr,
                when_clauses=when_clauses,
                else_expr=else_expr,
            ):
                return self._eval_case(case_expr, when_clauses, else_expr)

            case IndexLookup(expression=coll_expr, index=idx_expr):
                coll = self.evaluate(coll_expr)
                idx = self.evaluate(idx_expr)

                def _index_one(row: object, i: object) -> object:
                    if row is None or i is None:
                        return None
                    if isinstance(row, dict):
                        # Map key access: map['key']
                        return row.get(i)  # type: ignore[arg-type]  # runtime key is Any
                    # List integer index access: list[0]
                    try:
                        return row[int(i)]  # type: ignore[call-overload,index]  # runtime i is numeric
                    except (IndexError, TypeError):
                        if _DEBUG_ENABLED:
                            LOGGER.debug(
                                "Index access failed: row=%r, index=%r",
                                row,
                                i,
                            )
                        return None

                # Vectorized: np.frompyfunc avoids intermediate list allocation
                _ufunc = np.frompyfunc(_index_one, 2, 1)
                return pd.Series(
                    _ufunc(coll.values, idx.values),
                    dtype=object,
                ).reset_index(drop=True)

            case Slicing(expression=coll_expr, start=start_expr, end=end_expr):
                return self._eval_slicing(coll_expr, start_expr, end_expr)

            case ListComprehension() as lc:
                return self._eval_list_comprehension(lc)

            case Quantifier() as q:
                return self._eval_quantifier(q)

            case Reduce() as r:
                return self._eval_reduce(r)

            case MapProjection() as mp:
                return self._eval_map_projection(mp)

            case PatternComprehension() as pc:
                return self._eval_pattern_comprehension(pc)

            case Exists(content=exists_content):
                return self._eval_exists(exists_content)

            case _:
                msg = (
                    f"Expression type '{type(expression).__name__}' is not yet "
                    "supported by BindingExpressionEvaluator."
                )
                if _DEBUG_ENABLED:
                    LOGGER.debug(
                        "evaluate: unsupported expression %s",
                        type(expression).__name__,
                    )
                raise NotImplementedError(
                    msg,
                )

    # ------------------------------------------------------------------
    # Property / variable access
    # ------------------------------------------------------------------

    def _eval_property_lookup(
        self,
        var_expr: Expression,
        prop_name: str,
    ) -> FrameSeries:
        """Evaluate a property lookup expression (e.g. ``n.name``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_property_lookup`
        which handles two cases:

        * **Variable target** — resolves the property from entity/relationship
          tables via the current :class:`BindingFrame`.
        * **Expression target** — evaluates *var_expr* first, then extracts
          *prop_name* from ``dict`` values (map literals) or temporal strings.

        Args:
            var_expr: The AST expression to the left of the ``.`` (typically a
                :class:`~pycypher.ast_models.Variable`).
            prop_name: The property key to look up.

        Returns:
            A ``pd.Series`` of property values, one per row in the frame.
            Returns ``None`` for rows where the property does not exist.

        """
        # Architecture Loop 283 - Phase 4: Delegate to CollectionExpressionEvaluator
        return self.collection_evaluator.eval_property_lookup(
            var_expr,
            prop_name,
            self,
        )

    def _eval_variable(self, var_name: str) -> FrameSeries:
        """Return the binding column for *var_name* from the current frame.

        Args:
            var_name: The Cypher variable name (must be present in
                ``self.frame.bindings``).

        Returns:
            A ``pd.Series`` of entity/relationship IDs, one per frame row.

        Raises:
            VariableNotFoundError: If *var_name* is not in the current binding
                frame.  Includes available variables and a "did you mean?"
                suggestion when a close match exists.

        """
        if var_name not in self.frame.bindings.columns:
            available = list(self.frame.var_names)
            hint = suggest_close_match(var_name, available)
            raise VariableNotFoundError(var_name, available, hint)
        return self.frame.bindings[var_name]

    # ------------------------------------------------------------------
    # Literals
    # ------------------------------------------------------------------

    # (handled inline in evaluate() via match)

    # ------------------------------------------------------------------
    # Arithmetic
    # ------------------------------------------------------------------

    def _eval_arithmetic(
        self,
        op: str,
        left_expr: Expression,
        right_expr: Expression,
    ) -> FrameSeries:
        """Evaluate a binary arithmetic expression (``+``, ``-``, ``*``, ``/``, ``%``, ``^``).

        Architecture Loop 277 - Phase 1: Delegates to ArithmeticExpressionEvaluator
        for focused, testable arithmetic operations.

        Args:
            op: Arithmetic operator string (e.g. ``"+"``, ``"*"``).
            left_expr: Left-hand operand expression.
            right_expr: Right-hand operand expression.

        Returns:
            A numeric ``pd.Series`` of per-row results.

        Raises:
            ValueError: If *op* is not a supported arithmetic operator.

        """
        return self.arithmetic_evaluator.evaluate_arithmetic(
            op,
            left_expr,
            right_expr,
            self,
        )

    # ------------------------------------------------------------------
    # Comparisons
    # ------------------------------------------------------------------

    def _eval_comparison(
        self,
        op: str,
        left_expr: Expression,
        right_expr: Expression,
    ) -> FrameSeries:
        """Evaluate a binary comparison expression (``=``, ``<>``, ``<``, ``>``, etc.).

        Delegates to :class:`~pycypher.comparison_evaluator.ComparisonEvaluator`.

        Args:
            op: Comparison operator string (e.g. ``"="``, ``"<>"``, ``"<="``).
            left_expr: Left-hand operand expression.
            right_expr: Right-hand operand expression.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported comparison operator.

        """
        return self.comparison_evaluator.evaluate_comparison(
            op,
            left_expr,
            right_expr,
            self,
        )

    def _eval_label_predicate(
        self,
        operand_expr: Expression,
        labels: list[str],
    ) -> FrameSeries:
        """Evaluate a label predicate ``n:Label`` or ``n:Label1:Label2``.

        For labeled-scan variables (``type_registry`` has an entry), all rows
        share the same entity type — a constant boolean Series is returned.
        For unlabeled-scan variables (type absent from registry, e.g. from
        ``MATCH (n)``), each row may have a different entity type; a per-row
        ID→type lookup is performed.

        In the current single-label entity model, a node belongs to exactly
        one type.  Consequently:

        * ``n:Person`` → ``True`` for all rows when *n* is a Person variable.
        * ``n:Animal`` → ``False`` for all rows when *n* is a Person variable.
        * ``n:Person:Employee`` → ``False`` for all rows (no entity has two
          distinct types simultaneously in this model).

        Args:
            operand_expr: Expression to evaluate (must be a ``Variable`` for
                label lookup to succeed; non-variable expressions return False).
            labels: One or more label names all of which must match.

        Returns:
            Boolean ``pd.Series`` of length equal to the frame size.

        """
        from pycypher.dataframe_utils import (
            source_to_pandas as _source_to_pandas,
        )
        from pycypher.constants import ID_COLUMN

        n_rows = len(self.frame.bindings)
        if not isinstance(operand_expr, Variable):
            return pd.Series([False] * n_rows, dtype=object)
        var_name: str = operand_expr.name
        if var_name not in self.frame.bindings.columns:
            return pd.Series([False] * n_rows, dtype=object)

        entity_type: str | None = self.frame.type_registry.get(var_name)

        if entity_type is not None and entity_type != "__MULTI__":
            # Fast path: labeled scan — constant result for all rows.
            match = all(entity_type == lbl for lbl in labels)
            return pd.Series([match] * n_rows, dtype=object)

        # Unlabeled scan or multi-type sentinel: build id→type Series via pd.concat.
        # pd.Series(data=etype, index=ids) broadcasts the scalar entity-type string
        # across all IDs for that table in a single C-level allocation — no Python
        # per-row loop.  pd.concat is also Cython-level.  This replaces the old
        # "for eid in ids: id_to_type[eid] = etype" inner loop that was O(n_entities)
        # Python iterations.
        type_parts: list[pd.Series] = []
        for etype, table in self.frame.context.entity_mapping.mapping.items():
            shadow = getattr(self.frame.context, "_shadow", {})
            raw_df = shadow.get(etype) or _source_to_pandas(table.source_obj)
            if ID_COLUMN in raw_df.columns:
                ids = raw_df[ID_COLUMN].dropna()
                if len(ids) > 0:
                    type_parts.append(
                        pd.Series(data=etype, index=ids, dtype=object),
                    )

        id_series: pd.Series = self.frame.bindings[var_name]
        if type_parts:
            id_to_type_series = pd.concat(type_parts)
            per_row_type: pd.Series = id_series.map(id_to_type_series)
        else:
            per_row_type = _broadcast_scalar(None, len(id_series))

        # Vectorised label check: pd.Series.eq() returns False for NaN/None
        # (not True, not NaN) — exactly the desired semantics when an entity
        # ID has no registered type.  No Python per-row call needed.
        if len(labels) == 1:
            result = per_row_type.eq(labels[0])
        else:
            # Multi-label AND: in the single-type entity model each entity has
            # exactly one type, so n:A:B is always False unless A == B.
            result = pd.concat(
                [per_row_type.eq(lbl) for lbl in labels],
                axis=1,
            ).all(axis=1)
        return result.astype(object)

    def _eval_null_check(
        self,
        op: str,
        operand_expr: Expression,
    ) -> FrameSeries:
        """Evaluate an ``IS NULL`` or ``IS NOT NULL`` predicate.

        Delegates to :class:`~pycypher.comparison_evaluator.ComparisonEvaluator`.

        Args:
            op: One of ``"IS NULL"`` or ``"IS NOT NULL"``.
            operand_expr: The expression whose nullness is tested.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported null check operator.

        """
        return self.comparison_evaluator.evaluate_null_check(
            op,
            operand_expr,
            self,
        )

    def _eval_string_predicate(
        self,
        op: str,
        left_expr: Expression,
        right_expr: Expression,
    ) -> FrameSeries:
        """Evaluate a string or membership predicate.

        Delegates to :class:`~pycypher.string_predicate_evaluator.StringPredicateEvaluator`.

        Args:
            op: The predicate operator string.
            left_expr: The expression producing the string or value to test.
            right_expr: The expression producing the pattern/list to test against.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            TypeError: If *left_expr* evaluates to a non-string, non-null Series
                and *op* is a string accessor operator.
            UnsupportedOperatorError: If *op* is not a recognised string predicate
                operator.

        """
        return self.string_predicate_evaluator.evaluate_string_predicate(
            op,
            left_expr,
            right_expr,
            self,
        )

    # ------------------------------------------------------------------
    # Boolean logic
    # ------------------------------------------------------------------

    @staticmethod
    def _null_safe(series: pd.Series) -> FrameSeries:
        """Return *series* with ``NaN``/``None`` replaced by ``False``.

        Cypher boolean operators treat ``null`` as ``false`` in filter
        predicates.  Calling this before any boolean operation ensures that
        absent or null-valued columns do not propagate ``NaN`` through the
        result frame and silently drop rows.

        Args:
            series: A boolean (or mixed-type) ``pd.Series``.

        Returns:
            The same series with nulls filled as ``False``.

        """
        return series.fillna(False)

    def _eval_bool_chain(
        self,
        key: str,
        operands: list[Expression],
    ) -> FrameSeries:
        """Evaluate a multi-operand boolean fold using the ``_BOOL_FOLD_OPS`` table.

        All operands are null-coerced to ``False`` before the binary operation
        so that missing values do not propagate through filter predicates.

        Args:
            key: One of ``"and"``, ``"or"``, or ``"xor"``.
            operands: List of Cypher AST expression nodes.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        """
        # Architecture Loop 277 - Phase 2: Delegate to BooleanExpressionEvaluator
        return self.boolean_evaluator.evaluate_bool_chain(key, operands, self)

    def _eval_and(self, operands: list[Expression]) -> FrameSeries:
        """Evaluate Cypher ``AND`` — Kleene three-valued left-fold."""
        # Architecture Loop 277 - Phase 2: Delegate to BooleanExpressionEvaluator
        return self.boolean_evaluator.evaluate_and(operands, self)

    def _eval_or(self, operands: list[Expression]) -> FrameSeries:
        """Evaluate Cypher ``OR`` — Kleene three-valued left-fold."""
        # Architecture Loop 277 - Phase 2: Delegate to BooleanExpressionEvaluator
        return self.boolean_evaluator.evaluate_or(operands, self)

    def _eval_not(self, operand_expr: Expression) -> FrameSeries:
        """Evaluate Cypher ``NOT expr`` — three-valued boolean negation.

        Three-valued logic: ``NOT true`` → false, ``NOT false`` → true,
        ``NOT null`` → null.  The WHERE clause's ``fillna(False)`` converts
        the null to false for filtering, correctly excluding null rows.

        Delegates to :func:`kleene_not` for vectorised numpy evaluation —
        no Python-level loop per row.
        """
        # Architecture Loop 277 - Phase 2: Delegate to BooleanExpressionEvaluator
        return self.boolean_evaluator.evaluate_not(operand_expr, self)

    def _eval_xor(self, operands: list[Expression]) -> FrameSeries:
        """Evaluate Cypher ``XOR`` — Kleene three-valued left-fold (null XOR x = null)."""
        # Architecture Loop 277 - Phase 2: Delegate to BooleanExpressionEvaluator
        return self.boolean_evaluator.evaluate_xor(operands, self)

    def _eval_unary(self, op: str, operand_expr: Expression) -> FrameSeries:
        """Evaluate a unary arithmetic operator (``+`` or ``-``).

        Delegates to :class:`~pycypher.comparison_evaluator.ComparisonEvaluator`.

        Args:
            op: Unary operator string — ``"+"`` or ``"-"``.
            operand_expr: The expression to apply the operator to.

        Returns:
            A numeric ``pd.Series`` of per-row results.

        Raises:
            UnsupportedOperatorError: If *op* is not a supported unary operator.

        """
        return self.comparison_evaluator.evaluate_unary(op, operand_expr, self)

    # ------------------------------------------------------------------
    # Scalar functions
    # ------------------------------------------------------------------

    def _eval_scalar_function(
        self,
        func_name: str,
        func_args: Any,
    ) -> FrameSeries:
        """Architecture Loop Phase 5: Delegated to ScalarFunctionEvaluator.

        Evaluate a Cypher scalar or graph-introspection function call.

        This method has been extracted to ScalarFunctionEvaluator to achieve
        clear separation of concerns while maintaining 100% backward compatibility.

        Args:
            func_name: The Cypher function name (case-insensitive matching is
                applied internally).
            func_args: Raw argument value from the AST — either a list of
                :class:`~pycypher.ast_models.Expression` nodes or a single
                expression; normalised by ScalarFunctionEvaluator.

        Returns:
            A ``pd.Series`` of per-row results.

        Raises:
            ValueError: If *func_name* is an aggregation function used in a
                scalar context, or if the function is unknown to the registry.

        """
        return self.scalar_function_evaluator.evaluate_scalar_function(
            func_name,
            func_args,
            self,
        )

    # ------------------------------------------------------------------
    # CASE expression
    # ------------------------------------------------------------------

    def _eval_case(
        self,
        case_expr: Expression | None,
        when_clauses: list[Any],
        else_expr: Expression | None,
    ) -> FrameSeries:
        """Evaluate a CASE expression vectorially using ``pd.Series.where``.

        Delegates to :class:`~pycypher.comparison_evaluator.ComparisonEvaluator`.

        Args:
            case_expr: The *simple* CASE discriminant expression, or ``None``
                for a *searched* CASE.
            when_clauses: Ordered list of :class:`~pycypher.ast_models.WhenClause`
                nodes.
            else_expr: Optional ELSE expression; produces ``None`` when absent.

        Returns:
            A ``pd.Series`` of per-row CASE results.

        """
        return self.comparison_evaluator.evaluate_case(
            case_expr,
            when_clauses,
            else_expr,
            self,
        )

    def _eval_slicing(
        self,
        coll_expr: Expression,
        start_expr: Expression | None,
        end_expr: Expression | None,
    ) -> FrameSeries:
        """Evaluate a list/string slicing expression (e.g. ``list[1..3]``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_slicing`.
        Follows Cypher slice semantics: ``list[a..b]`` returns elements at
        indices *a* up to but not including *b*, identical to Python
        ``seq[a:b]``.  Either bound may be ``None`` for an open-ended slice.

        Args:
            coll_expr: Expression evaluating to a list or string to slice.
            start_expr: Start index expression, or ``None`` for open start.
            end_expr: End index expression, or ``None`` for open end.

        Returns:
            A ``pd.Series`` of sliced values (lists or strings), one per row.
            Returns ``None`` for rows where the collection is null.

        """
        # Architecture Loop 283 - Phase 4: Delegate to CollectionExpressionEvaluator
        return self.collection_evaluator.eval_slicing(
            coll_expr,
            start_expr,
            end_expr,
            self,
        )

    def _eval_list_comprehension(self, lc: ListComprehension) -> FrameSeries:
        """Evaluate a Cypher list comprehension (``[x IN list WHERE pred | expr]``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_list_comprehension`
        which uses a vectorised explode-evaluate-group strategy: all
        ``(row, element)`` pairs are flattened into a single
        :class:`BindingFrame`, the WHERE predicate and map expression are each
        evaluated once across all elements, and results are regrouped per row.

        Args:
            lc: The :class:`~pycypher.ast_models.ListComprehension` AST node.

        Returns:
            A ``pd.Series`` of lists, one per row in the current frame.

        """
        # Architecture Loop 286: Complete CollectionExpressionEvaluator delegation
        return self.collection_evaluator.eval_list_comprehension(lc, self)

    def _eval_quantifier(self, q: Quantifier) -> FrameSeries:
        """Evaluate a quantifier predicate (``ANY``, ``ALL``, ``NONE``, or ``SINGLE``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_quantifier`
        which uses a vectorised explode-evaluate-group strategy for
        ``O(1)`` :class:`BindingFrame` allocations instead of ``O(rows)``.

        Args:
            q: The :class:`~pycypher.ast_models.Quantifier` AST node containing
                the quantifier type, iteration variable, list expression, and
                predicate.

        Returns:
            A ``pd.Series`` of boolean values, one per row.

        """
        # Architecture Loop 286: Complete CollectionExpressionEvaluator delegation
        return self.collection_evaluator.eval_quantifier(q, self)

    def _eval_reduce(self, r: Reduce) -> FrameSeries:
        """Evaluate a ``REDUCE`` expression (fold over a list with an accumulator).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_reduce` which
        uses a batch-per-step strategy: for each step index, all active rows
        are evaluated in a single :class:`BindingFrame`, reducing allocations
        from ``O(rows * max_elements)`` to ``O(max_elements)``.

        Args:
            r: The :class:`~pycypher.ast_models.Reduce` AST node containing
                the accumulator variable, iteration variable, initial value,
                list expression, and map expression.

        Returns:
            A ``pd.Series`` of reduced values, one per row.

        """
        # Architecture Loop 286: Complete CollectionExpressionEvaluator delegation
        return self.collection_evaluator.eval_reduce(r, self)

    def _eval_exists(self, content: Any) -> FrameSeries:
        """Evaluate an ``EXISTS { pattern }`` predicate.

        Delegates to :class:`~pycypher.exists_evaluator.ExistsEvaluator`.
        """
        return self.exists_evaluator.evaluate_exists(content, self)

    def _eval_pattern_comprehension(
        self,
        pc: PatternComprehension,
    ) -> FrameSeries:
        """Evaluate a Cypher pattern comprehension row by row.

        Delegates to :class:`~pycypher.exists_evaluator.ExistsEvaluator`.
        """
        return self.exists_evaluator.evaluate_pattern_comprehension(pc, self)

    # ------------------------------------------------------------------
    # Aggregation (scalar collapse, used by WITH/RETURN handlers)
    # ------------------------------------------------------------------

    def _eval_as_scalar(self, expression: Expression) -> Any:
        """Evaluate *expression* to a single Python scalar.

        Used when building up aggregate expressions from sub-parts: if the
        sub-expression is itself an aggregation (e.g. ``count(*)``) it is
        evaluated via :meth:`evaluate_aggregation`; otherwise it is evaluated
        via :meth:`evaluate` and the first row's value is used.

        Args:
            expression: Any AST expression node.

        Returns:
            A Python scalar.

        """
        from pycypher.ast_models import CountStar, FunctionInvocation

        if isinstance(expression, CountStar):
            return self.evaluate_aggregation(expression)

        if (
            isinstance(expression, FunctionInvocation)
            and expression.function_name.lower() in KNOWN_AGGREGATIONS
        ):
            return self.evaluate_aggregation(expression)

        # Non-aggregation: evaluate and take the scalar value.
        series = self.evaluate(expression)
        return series.iloc[0] if len(series) > 0 else None

    def _eval_map_literal(
        self,
        map_literal: MapLiteral,
    ) -> FrameSeries:
        """Evaluate a map literal expression (``{key: expr, ...}``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_map_literal`
        with the actual :class:`MapLiteral` AST node.

        Args:
            map_literal: The :class:`~pycypher.ast_models.MapLiteral` AST node.

        Returns:
            A ``pd.Series`` of ``dict`` objects, one per row in the frame.

        """
        return self.collection_evaluator.eval_map_literal(
            map_literal,
            self,
        )

    def _eval_map_projection(self, mp: MapProjection) -> FrameSeries:
        """Evaluate a map projection expression (``variable {.prop1, .prop2, key: expr}``).

        Delegates to :meth:`CollectionExpressionEvaluator.eval_map_projection`.
        Constructs a map per row by extracting the named properties from the
        base variable and merging any explicit key-expression pairs.

        Args:
            mp: The :class:`~pycypher.ast_models.MapProjection` AST node.

        Returns:
            A ``pd.Series`` of ``dict`` objects, one per row in the frame.

        """
        # Architecture Loop 284 - Phase 5: Delegate to CollectionExpressionEvaluator
        return self.collection_evaluator.eval_map_projection(mp, self)

    def evaluate_aggregation(self, agg_expression: Expression) -> Any:
        """Evaluate an aggregation expression and return a scalar value.

        Architecture Loop 280 - Phase 3: Delegates to AggregationExpressionEvaluator.

        Supports ``collect``, ``count`` / ``COUNT(*)``, ``sum``, ``avg``,
        ``min``, ``max``.  Also handles :class:`~pycypher.ast_models.Arithmetic`
        nodes whose branches contain aggregations (e.g. ``count(*) + 1`` or
        ``sum(p.salary) * 2``).

        Args:
            agg_expression: A :class:`~pycypher.ast_models.FunctionInvocation`,
                :class:`~pycypher.ast_models.CountStar`, or
                :class:`~pycypher.ast_models.Arithmetic` node.

        Returns:
            A scalar aggregated value.

        Raises:
            ValueError: For unsupported aggregation functions or missing arguments.

        """
        # Architecture Loop 280 - Phase 3: Delegate to specialized aggregation evaluator
        return self.aggregation_evaluator.evaluate_aggregation(
            agg_expression,
            self,
        )

    def evaluate_aggregation_grouped(
        self,
        agg_expression: Expression,
        group_df: pd.DataFrame,
        group_key_aliases: list[str],
    ) -> pd.Series | None:
        """Vectorised grouped aggregation — one scalar per group.

        Evaluates *agg_expression*'s inner argument **once** over the entire
        frame, then uses ``pd.Series.groupby().agg()`` to produce one
        aggregated value per group.  This replaces the previous pattern of
        creating a new :class:`BindingFrame` and
        :class:`BindingExpressionEvaluator` per group, cutting O(N_groups)
        object allocations and O(N_groups × frame_size) mask operations down
        to a single vectorised pandas pass.

        Args:
            agg_expression: A :class:`~pycypher.ast_models.FunctionInvocation`,
                :class:`~pycypher.ast_models.CountStar`, or
                :class:`~pycypher.ast_models.Arithmetic` aggregation node.
            group_df: DataFrame of evaluated GROUP BY key columns (one row per
                frame row, columns named by *group_key_aliases*).
            group_key_aliases: Ordered list of GROUP BY column names in
                *group_df*.

        Returns:
            A :class:`pd.Series` with one value per unique group (in
            first-seen order, matching the order produced by
            ``group_df.groupby(..., sort=False)``), or ``None`` when the
            expression cannot be vectorised (e.g. Arithmetic wrapping
            aggregations) — the caller should fall back to per-group
            evaluation in that case.

        """
        # Architecture Loop 280 - Phase 3: Delegate to specialized aggregation evaluator
        return self.aggregation_evaluator.evaluate_aggregation_grouped(
            agg_expression,
            group_df,
            group_key_aliases,
            self,
        )
