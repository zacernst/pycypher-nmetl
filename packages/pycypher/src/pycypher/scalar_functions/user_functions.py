"""Register plain-Python scalar functions as Cypher functions.

Lets users write *scalar* Python functions (one Python value in, one Python
value out) and register them with the Cypher engine without having to
understand pandas vectorisation.  Each user function is wrapped row-wise:

    def double(x):
        return x * 2

becomes a ``pd.Series → pd.Series`` callable that null-propagates and applies
``double`` to each element.

Trade-off: the wrapper invokes the user function once per row in pure Python,
which is slow for large frames.  The built-in registry uses vectorised
numpy/pandas operations (see ``scalar_functions/__init__.py``).  This module
exists because user code is much easier to write in scalar form, and
correctness is more valuable than per-row throughput for a first cut.
"""

from __future__ import annotations

import inspect
import math
from typing import TYPE_CHECKING, Any

import pandas as pd

from shared.logger import LOGGER

from pycypher.scalar_functions import ScalarFunctionRegistry

if TYPE_CHECKING:
    from collections.abc import Callable

# Cap on per-row exception log lines emitted from a single function call, to
# prevent log floods when an entire column has bad data.  After this many
# rows, individual failures are suppressed but still counted in the summary.
_MAX_PER_ROW_LOG_LINES: int = 3


def _is_null(v: Any) -> bool:
    """Return True for Python None or float NaN."""
    return v is None or (isinstance(v, float) and math.isnan(v))


def _infer_arity(func: Callable[..., Any]) -> tuple[int, int | None]:
    """Return ``(min_args, max_args)`` from a callable's signature.

    - Required positional / positional-or-keyword params → contribute to both.
    - Optional positional params (with defaults) → contribute to ``max_args``.
    - ``*args`` → ``max_args`` becomes ``None`` (unbounded).
    - Keyword-only and ``**kwargs`` are ignored — Cypher only passes
      positional arguments.
    """
    sig = inspect.signature(func)
    min_args = 0
    max_fixed = 0
    has_varargs = False
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            has_varargs = True
            continue
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            max_fixed += 1
            if p.default is inspect.Parameter.empty:
                min_args += 1
    return min_args, None if has_varargs else max_fixed


def _description(func: Callable[..., Any]) -> str:
    """Return the first non-blank docstring line, or empty string."""
    doc = inspect.getdoc(func)
    if not doc:
        return ""
    for line in doc.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _wrap_row_wise(
    func: Callable[..., Any],
) -> Callable[..., pd.Series]:
    """Wrap a scalar Python function as a ``pd.Series → pd.Series`` callable.

    The wrapper:
    - Iterates the input Series in lockstep.
    - Returns ``None`` for any row where any input value is null.
    - Calls ``func(*row_values)`` for non-null rows.
    - Builds the result as ``pd.Series(values, index=first_arg.index)``.

    Error handling: if ``func`` raises on a particular row, the wrapper logs
    a WARNING with the row index, input values, and the exception summary,
    substitutes ``None`` for that row's output, and continues.  This means
    one bad row no longer aborts the entire query — but every failure is
    visible in the log instead of disappearing as a silent null cell.  A
    summary count is logged at the end if any rows failed.
    """

    def wrapped(*series_args: pd.Series) -> pd.Series:
        if not series_args:
            msg = (
                f"Wrapped function {func.__name__!r} was called with no "
                "arguments; user-defined Cypher functions must take at "
                "least one argument."
            )
            raise TypeError(msg)
        n = len(series_args[0])
        index = series_args[0].index
        out: list[Any] = []
        n_failures = 0
        for i in range(n):
            row = [s.iat[i] for s in series_args]
            if any(_is_null(v) for v in row):
                out.append(None)
                continue
            try:
                out.append(func(*row))
            except Exception as exc:  # noqa: BLE001 — log+null-fallback per row
                n_failures += 1
                if n_failures <= _MAX_PER_ROW_LOG_LINES:
                    LOGGER.warning(
                        "user function %s failed on row %d (inputs=%r): %s: %s",
                        func.__name__,
                        i,
                        row,
                        type(exc).__name__,
                        exc,
                        exc_info=True,
                    )
                elif n_failures == _MAX_PER_ROW_LOG_LINES + 1:
                    LOGGER.warning(
                        "user function %s: further per-row failures will be "
                        "suppressed; a summary count will be logged at the end",
                        func.__name__,
                    )
                out.append(None)
        if n_failures:
            LOGGER.warning(
                "user function %s: %d / %d row(s) raised an exception "
                "and were replaced with null in the output.",
                func.__name__,
                n_failures,
                n,
            )
        return pd.Series(out, index=index)

    return wrapped


def register_user_function(
    func: Callable[..., Any],
    *,
    name: str | None = None,
) -> None:
    """Register *func* as a Cypher scalar function.

    The user writes a plain Python scalar function — ``def f(x, y): ...`` —
    and this helper takes care of wrapping it row-wise, null-propagating,
    inferring arity from the signature, and pulling a description from the
    docstring.

    Args:
        func: A plain Python callable.  Must take at least one positional
            argument.
        name: Override the registered Cypher function name.  Defaults to
            ``func.__name__``.

    Raises:
        TypeError: If *func* takes zero positional arguments.
    """
    min_args, max_args = _infer_arity(func)
    if (max_args is not None and max_args == 0) or (
        min_args == 0 and max_args == 0
    ):
        msg = (
            f"Cannot register {func.__name__!r}: user-defined Cypher "
            "functions must take at least one positional argument."
        )
        raise TypeError(msg)
    ScalarFunctionRegistry.get_instance().register_function(
        name=name or func.__name__,
        callable=_wrap_row_wise(func),
        min_args=min_args,
        max_args=max_args,
        description=_description(func),
        example="",
    )
