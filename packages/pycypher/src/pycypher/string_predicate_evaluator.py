"""StringPredicateEvaluator — string and membership predicate evaluation.

Extracted from :mod:`pycypher.binding_evaluator` to isolate the string
predicate family (``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``, ``=~``,
``IN``, ``NOT IN``) into a focused, independently testable module.

Includes ReDoS protection for the ``=~`` regex operator.
"""

from __future__ import annotations

import functools
import logging
import re
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from shared.helpers import is_null_value
from shared.logger import LOGGER

from pycypher.cypher_types import FrameSeries
from pycypher.exceptions import UnsupportedOperatorError, WrongCypherTypeError

if TYPE_CHECKING:
    from pycypher.ast_models import Expression
    from pycypher.evaluator_protocol import ExpressionEvaluatorProtocol

_DEBUG_ENABLED: bool = LOGGER.isEnabledFor(logging.DEBUG)

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


@functools.lru_cache(maxsize=256)
def _compile_regex(pattern: str) -> re.Pattern[str]:
    """Compile and cache a regex pattern.

    The LRU cache avoids recompiling the same pattern on every ``=~``
    evaluation.  The maxsize of 256 is generous for typical workloads
    while bounding memory usage.

    Args:
        pattern: A regex pattern string (already validated).

    Returns:
        Compiled :class:`re.Pattern`.

    """
    return re.compile(pattern)


def _validate_regex_pattern(pattern: str) -> re.Pattern[str]:
    """Validate and compile a regex pattern, raising on dangerous inputs.

    Checks:

    1. **Compilability** — rejects syntactically invalid regex.
    2. **Length limit** — caps pattern length to prevent memory abuse.
    3. **Known ReDoS vectors** — detects nested quantifiers and
       overlapping alternation that cause exponential backtracking.

    Args:
        pattern: User-supplied regex string from a Cypher ``=~`` predicate.

    Returns:
        The compiled :class:`re.Pattern` (cached for reuse).

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

    for redos_re in _REDOS_PATTERNS:
        if redos_re.search(pattern):
            msg = (
                "Regex pattern contains a potentially dangerous construct "
                "(nested quantifiers or overlapping alternation) that could "
                "cause catastrophic backtracking. Simplify the pattern to "
                "avoid denial-of-service."
            )
            raise ValueError(msg)

    try:
        return _compile_regex(pattern)
    except re.error as exc:
        msg = f"Invalid regex pattern: {exc}"
        raise ValueError(msg) from exc


class StringPredicateEvaluator:
    """Evaluates string and membership predicates.

    Supported operators:

    * ``"STARTS WITH"`` — vectorised ``str.startswith``
    * ``"ENDS WITH"`` — vectorised ``str.endswith``
    * ``"CONTAINS"`` — vectorised ``str.contains`` (literal, not regex)
    * ``"=~"`` — full-match regex via ``str.fullmatch`` with ReDoS protection
    * ``"IN"`` — membership test against a list (three-valued logic)
    * ``"NOT IN"`` — inverse membership test (three-valued logic)

    """

    def evaluate_string_predicate(
        self,
        op: str,
        left_expr: Expression,
        right_expr: Expression,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate a string or membership predicate.

        The right-hand operand is evaluated and its first element is used as
        the scalar pattern/set for all rows — Cypher string predicates always
        compare against a constant pattern.

        Args:
            op: The predicate operator string.
            left_expr: The expression producing the string or value to test.
            right_expr: The expression producing the pattern/list to test against.
            evaluator: Parent expression evaluator for recursive evaluation.

        Returns:
            A boolean ``pd.Series`` of per-row results.

        Raises:
            TypeError: If *left_expr* evaluates to a non-string, non-null Series
                and *op* is a string accessor operator.
            UnsupportedOperatorError: If *op* is not a recognised string predicate
                operator.

        """
        if _DEBUG_ENABLED:
            LOGGER.debug("string_predicate: op=%r", op)
        left = evaluator.evaluate(left_expr)
        right_val = evaluator.evaluate(right_expr).iloc[0]
        null_mask = left.isna()

        # Guard: string accessor operators require a string (or all-null) left
        # operand.
        if op in ("STARTS WITH", "ENDS WITH", "CONTAINS", "=~"):
            self._validate_string_operand(left, op)

        if op == "STARTS WITH":
            result = left.str.startswith(right_val, na=False).astype(object)
        elif op == "ENDS WITH":
            result = left.str.endswith(right_val, na=False).astype(object)
        elif op == "CONTAINS":
            result = left.str.contains(
                right_val,
                regex=False,
                na=False,
            ).astype(object)
        elif op == "=~":
            compiled = _validate_regex_pattern(right_val)
            result = left.str.fullmatch(compiled, na=False).astype(object)
        else:
            result = None

        if result is not None:
            if null_mask.any():
                result[null_mask] = None
            return result

        if op in ("IN", "NOT IN"):
            return self._eval_in_predicate(op, left, right_expr, evaluator)

        raise UnsupportedOperatorError(
            op,
            ["STARTS WITH", "ENDS WITH", "CONTAINS", "=~", "IN", "NOT IN"],
            category="string predicate",
        )

    @staticmethod
    def _validate_string_operand(left: pd.Series, op: str) -> None:
        """Validate that the left operand is string-typed for string predicates.

        Uses early-terminating iteration instead of ``left.dropna()`` to avoid
        an O(n) Series copy when only the dtype and first non-null value are needed.
        """
        # Fast path: numeric / bool dtypes can never be strings.
        # dtype.kind is O(1) on the original Series — no copy required.
        if left.dtype.kind not in ("O", "U", "S"):
            # Confirm at least one non-null exists before raising.
            if left.notna().any():
                msg = (
                    f"Operator {op!r} requires a string left-hand operand, "
                    f"but got {left.dtype!r}. "
                    f"Use toString() to convert if needed."
                )
                raise WrongCypherTypeError(msg)
            return

        # Object dtype — early-terminate on first non-null value.
        for v in left.values:
            if v is None:
                continue
            try:
                if v != v:  # NaN check (NaN != NaN)
                    continue
            except (TypeError, ValueError):
                continue  # pd.NA raises TypeError in boolean context
            # Found first non-null value — check type.
            if not isinstance(v, str):
                msg = (
                    f"Operator {op!r} requires a string left-hand operand, "
                    f"but got {type(v).__name__!r}. "
                    f"Use toString() to convert if needed."
                )
                raise WrongCypherTypeError(msg)
            return

    @staticmethod
    def _eval_in_predicate(
        op: str,
        left: pd.Series,
        right_expr: Expression,
        evaluator: ExpressionEvaluatorProtocol,
    ) -> FrameSeries:
        """Evaluate IN / NOT IN with three-valued logic."""
        # Re-evaluate the full right-hand Series (not just .iloc[0]) so
        # each row is tested against its own list when the right side is a
        # per-row list property (e.g. ``'python' IN node.tags``).
        right_series = evaluator.evaluate(right_expr)
        # Broadcast right_series if it evaluated to fewer rows than left
        if len(right_series) < len(left):
            right_series = pd.Series(
                [right_series.iloc[0]] * len(left),
                dtype=object,
            )

        def _row_in(lv: object, rv: object) -> object:
            """Three-valued IN: null LHS → null; null in RHS + no match → null."""
            _is_null_val = is_null_value

            if _is_null_val(lv):
                if isinstance(rv, (list, tuple)) and len(rv) == 0:
                    return False
                return None
            if isinstance(rv, (list, tuple)):
                for item in rv:
                    if not _is_null_val(item) and item == lv:
                        return True
                if any(_is_null_val(item) for item in rv):
                    return None
                return False
            # Scalar right-hand side: equality check
            return bool(lv == rv)

        # Vectorized: np.frompyfunc avoids intermediate list allocation
        _in_ufunc = np.frompyfunc(_row_in, 2, 1)
        raw_arr = _in_ufunc(left.values, right_series.values)
        if op == "NOT IN":
            # Three-valued negation: True→False, False→True, None→None
            _negate = np.frompyfunc(
                lambda v: None if v is None else not v,
                1,
                1,
            )
            raw_arr = _negate(raw_arr)
        return pd.Series(raw_arr, dtype=object)
