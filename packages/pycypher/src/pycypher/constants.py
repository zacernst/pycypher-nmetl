"""Shared constants and small utilities used across pycypher modules.

Extracting these from :mod:`relational_models` and :mod:`binding_evaluator`
breaks two circular-import cycles:

1. ``relational_models`` ↔ ``backend_engine`` (shared ``ID_COLUMN`` constant)
2. ``binding_evaluator`` ↔ ``scalar_function_evaluator`` (shared
   ``_normalize_func_args`` helper)
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Column-name constants
# ---------------------------------------------------------------------------

ID_COLUMN: str = "__ID__"
"""Name of the identity column inside source DataFrames."""

RELATIONSHIP_SOURCE_COLUMN: str = "__SOURCE__"
"""Source-node ID column in relationship DataFrames."""

RELATIONSHIP_TARGET_COLUMN: str = "__TARGET__"
"""Target-node ID column in relationship DataFrames."""


# ---------------------------------------------------------------------------
# Argument normalisation helper
# ---------------------------------------------------------------------------


def _null_series(n: int, *, index: Any = None) -> pd.Series:
    """Create a length-*n* object Series filled with ``None``.

    Avoids the ``[None] * n`` Python-list allocation that dominates at large *n*.

    Args:
        n: Number of elements.
        index: Optional pandas index to attach.

    Returns:
        A ``pd.Series`` of dtype ``object`` filled with ``None``.

    """
    arr = np.empty(n, dtype=object)
    arr[:] = None
    return pd.Series(arr, dtype=object, index=index)


def _broadcast_series(
    val: object,
    n: int,
    *,
    dtype: type | str | None = None,
    index: Any = None,
) -> pd.Series:
    """Create a length-*n* Series filled with *val*, avoiding ``[val] * n``.

    Handles all scalar types efficiently:

    * ``None`` → delegates to :func:`_null_series`.
    * ``bool`` / ``int`` / ``float`` → ``np.full`` with native dtype.
    * ``str`` → ``np.empty(dtype=object)`` with slice assignment.
    * Container (``list``, ``dict``, ``tuple``) → ``np.empty(dtype=object)``
      with ``.fill()`` to avoid numpy broadcast errors.

    Args:
        val: The scalar value to broadcast.
        n: Number of elements.
        dtype: Optional explicit dtype override.
        index: Optional pandas index to attach.

    Returns:
        A ``pd.Series`` of length *n*.

    """
    if n == 0:
        return pd.Series([], dtype=dtype or object, index=index)
    if val is None:
        return _null_series(n, index=index)
    if dtype is not None:
        return pd.Series(
            np.full(n, val, dtype=dtype),
            dtype=dtype,
            index=index,
        )
    if isinstance(val, bool):
        return pd.Series(np.full(n, val), dtype=bool, index=index)
    if isinstance(val, (int, float)):
        return pd.Series(np.full(n, val), index=index)
    # Object-typed scalars (str, list, dict, etc.)
    arr = np.empty(n, dtype=object)
    if isinstance(val, (list, dict, tuple)):
        arr.fill(val)
    else:
        arr[:] = val
    return pd.Series(arr, dtype=object, index=index)


class NullMaskResult:
    """Result of :func:`_init_null_result` — holds the pre-allocated result
    Series, the boolean mask of non-null positions, and the filtered non-null
    values.

    Attributes:
        result: Object-dtype Series of length ``len(s)`` initialised to ``None``.
        non_null_mask: Boolean Series — ``True`` where ``s`` is not null.
        non_null_vals: Filtered Series of non-null values, or ``None`` if the
            input was entirely null.

    """

    __slots__ = ("result", "non_null_mask", "non_null_vals")

    def __init__(
        self,
        result: pd.Series,
        non_null_mask: pd.Series,
        non_null_vals: pd.Series | None,
    ) -> None:
        self.result = result
        self.non_null_mask = non_null_mask
        self.non_null_vals = non_null_vals

    @property
    def all_null(self) -> bool:
        """Return ``True`` when every value in the input was null."""
        return self.non_null_vals is None


def _init_null_result(s: pd.Series) -> NullMaskResult:
    """Initialise a null-propagating result Series and non-null mask.

    This replaces the 5-line boilerplate repeated 25+ times across scalar
    function modules::

        result = _null_series(len(s), index=s.index)
        null_mask = s.isna()
        non_null_mask = ~null_mask
        if non_null_mask.any():
            non_null_vals = s[non_null_mask]

    Args:
        s: Input Series to split into null / non-null partitions.

    Returns:
        A :class:`NullMaskResult` with ``result`` (all-None Series),
        ``non_null_mask`` (boolean), and ``non_null_vals`` (filtered Series
        or ``None`` if all values are null).

    Example::

        nr = _init_null_result(s)
        if nr.all_null:
            return nr.result
        # Process nr.non_null_vals ...
        nr.result[nr.non_null_mask] = processed_values
        return nr.result

    """
    result = _null_series(len(s), index=s.index)
    non_null_mask = s.notna()
    if not non_null_mask.any():
        return NullMaskResult(result, non_null_mask, None)
    non_null_vals = s[non_null_mask]
    return NullMaskResult(result, non_null_mask, non_null_vals)


def _scalar_int(s: pd.Series, default: int = 0) -> int:
    """Extract a scalar integer from a single-element Series.

    Many scalar functions receive a "constant" argument as a 1-element
    Series.  This helper centralises the ``int(s.iloc[0]) if len(s) > 0
    else default`` pattern used 15+ times across scalar function modules.

    Args:
        s: A Series (typically 1-element) from the function dispatch.
        default: Value to return when *s* is empty.

    Returns:
        The integer value of the first element, or *default*.

    """
    return int(s.iloc[0]) if len(s) > 0 else default


def _scalar_str(s: pd.Series, default: str = "") -> str:
    """Extract a scalar string from a single-element Series.

    Args:
        s: A Series (typically 1-element) from the function dispatch.
        default: Value to return when *s* is empty.

    Returns:
        The string value of the first element, or *default*.

    """
    return str(s.iloc[0]) if len(s) > 0 else default


def _scalar_raw(s: pd.Series, default: Any = None) -> Any:
    """Extract a raw scalar value from a single-element Series.

    Args:
        s: A Series (typically 1-element) from the function dispatch.
        default: Value to return when *s* is empty.

    Returns:
        The first element unchanged, or *default*.

    """
    return s.iloc[0] if len(s) > 0 else default


def _scalar_int_opt(
    s: pd.Series | None,
    default: int = 0,
) -> int:
    """Extract a scalar integer from an optional Series parameter.

    For optional function arguments that may be ``None``.

    Args:
        s: An optional Series from the function dispatch.
        default: Value to return when *s* is ``None`` or empty.

    Returns:
        The integer value of the first element, or *default*.

    """
    if s is None or len(s) == 0:
        return default
    return int(s.iloc[0])


def _scalar_str_opt(
    s: pd.Series | None,
    default: str = " ",
) -> str:
    """Extract a scalar string from an optional Series parameter.

    Args:
        s: An optional Series from the function dispatch.
        default: Value to return when *s* is ``None`` or empty.

    Returns:
        The string value of the first element, or *default*.

    """
    if s is None or len(s) == 0:
        return default
    return str(s.iloc[0])


def _normalize_func_args(arguments: Any) -> list[Any]:
    """Return the flat argument list from a ``FunctionInvocation.arguments`` value.

    The grammar parser may deliver arguments in several shapes:

    * A bare ``list`` — returned directly.
    * A ``dict`` with an ``"arguments"`` key whose value is a list.
    * A ``dict`` with an ``"args"`` key whose value is a list.
    * Anything else — returns an empty list.

    Args:
        arguments: The ``arguments`` attribute of a
            :class:`~pycypher.ast_models.FunctionInvocation` node.

    Returns:
        A (possibly empty) list of AST expression nodes.

    """
    if isinstance(arguments, list):
        return arguments
    if isinstance(arguments, dict):
        for key in ("arguments", "args"):
            val = arguments.get(key)
            if isinstance(val, list):
                return val
    return []
