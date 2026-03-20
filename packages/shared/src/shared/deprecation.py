"""Lightweight deprecation utilities for PyCypher API evolution.

Provides a ``deprecated()`` decorator and a ``emit_deprecation()`` helper
that standardise deprecation warnings across the codebase.  All warnings
include a removal version, a migration hint, and are emitted via the
standard :mod:`warnings` module so that users can filter them with
:func:`warnings.filterwarnings`.

Usage::

    from shared.deprecation import deprecated

    @deprecated(since="0.0.19", removed_in="0.1.0", alternative="new_func")
    def old_func(x: int) -> int:
        return x + 1

    # Calling old_func() emits:
    # DeprecationWarning: old_func is deprecated since v0.0.19 and will
    # be removed in v0.1.0. Use new_func instead.

For non-decorator contexts (e.g. ``__getattr__`` module-level hooks),
use :func:`emit_deprecation` directly::

    from shared.deprecation import emit_deprecation

    def __getattr__(name: str) -> type:
        if name == "OldClass":
            emit_deprecation("OldClass", since="0.0.19",
                             removed_in="0.1.0", alternative="NewClass")
            return NewClass
        raise AttributeError(name)

"""

from __future__ import annotations

import functools
import warnings
from typing import Any


def _build_message(
    name: str,
    *,
    since: str,
    removed_in: str = "",
    alternative: str = "",
    detail: str = "",
) -> str:
    """Build a standardised deprecation message.

    Args:
        name: Name of the deprecated symbol.
        since: Version where deprecation was introduced.
        removed_in: Version where the symbol will be removed.
        alternative: Replacement symbol name.
        detail: Additional migration context.

    Returns:
        Formatted deprecation message string.

    """
    parts = [f"{name} is deprecated since v{since}"]
    if removed_in:
        parts.append(f" and will be removed in v{removed_in}")
    parts.append(".")
    if alternative:
        parts.append(f" Use {alternative} instead.")
    if detail:
        parts.append(f" {detail}")
    return "".join(parts)


def emit_deprecation(
    name: str,
    *,
    since: str,
    removed_in: str = "",
    alternative: str = "",
    detail: str = "",
    stacklevel: int = 2,
) -> None:
    """Emit a standardised deprecation warning.

    Use this in ``__getattr__`` hooks or other non-decorator contexts.

    Args:
        name: Name of the deprecated symbol.
        since: Version where deprecation was introduced.
        removed_in: Version where the symbol will be removed.
        alternative: Replacement symbol name.
        detail: Additional migration guidance.
        stacklevel: Stack level for the warning (default 2 = caller's caller).

    """
    msg = _build_message(
        name,
        since=since,
        removed_in=removed_in,
        alternative=alternative,
        detail=detail,
    )
    warnings.warn(msg, DeprecationWarning, stacklevel=stacklevel)


def deprecated(
    *,
    since: str,
    removed_in: str = "",
    alternative: str = "",
    detail: str = "",
) -> Any:
    """Decorator that marks a function or class as deprecated.

    Emits a :class:`DeprecationWarning` on every call with a standardised
    message including version timeline and migration guidance.

    Args:
        since: Version where deprecation was introduced (e.g. ``"0.0.19"``).
        removed_in: Version where the symbol will be removed (e.g. ``"0.1.0"``).
        alternative: Name of the replacement (e.g. ``"new_func"``).
        detail: Additional migration context.

    Returns:
        A decorator that wraps the target with a deprecation warning.

    Example::

        @deprecated(since="0.0.19", removed_in="0.1.0", alternative="ContextBuilder")
        class OldBuilder:
            ...

    """

    def decorator(obj: Any) -> Any:
        name = getattr(obj, "__qualname__", getattr(obj, "__name__", str(obj)))
        msg = _build_message(
            name,
            since=since,
            removed_in=removed_in,
            alternative=alternative,
            detail=detail,
        )

        if isinstance(obj, type):
            # Class: wrap __init__ to emit warning on instantiation.
            original_init = obj.__init__

            @functools.wraps(original_init)
            def warned_init(self: Any, *args: Any, **kwargs: Any) -> None:
                warnings.warn(msg, DeprecationWarning, stacklevel=2)
                original_init(self, *args, **kwargs)

            obj.__init__ = warned_init
            return obj

        # Function/method: wrap with warning.
        @functools.wraps(obj)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return obj(*args, **kwargs)

        return wrapper

    return decorator
