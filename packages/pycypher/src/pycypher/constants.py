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
    val: object, n: int, *, dtype: type | str | None = None, index: Any = None
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
            np.full(n, val, dtype=dtype), dtype=dtype, index=index
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
