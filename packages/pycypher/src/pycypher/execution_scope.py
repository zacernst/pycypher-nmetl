"""Per-query execution state, isolated across concurrent ``Star.execute_query()`` calls.

``Context`` (``relational_models.py``) is constructed once and shared for the
lifetime of a ``Star`` instance. Query-scoped state (bound parameters, the
mutation shadow layer, the query timeout deadline) used to live directly on
``Context`` as mutable attributes, which races when two ``execute_query()``
calls run concurrently on the same ``Star`` (threads, or
``execute_query_async`` via ``asyncio.to_thread``).

Each ``Context`` instance owns its own ``ContextVar`` (see
``Context._scope_var``), so isolation is two-dimensional: per *Context
instance* (two independent ``Context`` objects never share state, even in
the same thread — this matters for tests that build a bare ``Context()``
directly) and per *thread/async task* (concurrent calls sharing one
``Context`` get independent values automatically, since ``ContextVar.set``
only affects the calling ``contextvars.Context``).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Iterator

import pandas as pd


@dataclass
class ExecutionScope:
    """Per-query state formerly stored directly on ``Context``."""

    parameters: dict[str, Any] = field(default_factory=dict)
    memory_budget_bytes: int | None = None
    shadow: dict[str, pd.DataFrame] = field(default_factory=dict)
    shadow_rels: dict[str, pd.DataFrame] = field(default_factory=dict)
    query_deadline: float | None = None
    query_timeout_seconds: float | None = None


#: A ``ContextVar`` holding the active scope for one ``Context`` instance.
ScopeVar = ContextVar[ExecutionScope | None]


def new_scope_var() -> ScopeVar:
    """Create a fresh, unbound scope variable — one per ``Context`` instance."""
    return ContextVar("_context_scope_var", default=None)


def current_scope(var: ScopeVar) -> ExecutionScope:
    """Return the active scope for *var*, lazily creating one if none was pushed.

    Lazy creation covers call paths that never go through
    ``scoped_execution()`` (e.g. a bare ``Context()`` used directly in
    tests).
    """
    scope = var.get()
    if scope is None:
        scope = ExecutionScope()
        var.set(scope)
    return scope


def push_scope(var: ScopeVar) -> Token:
    """Install a fresh ``ExecutionScope`` on *var* and return a token for ``pop_scope``."""
    return var.set(ExecutionScope())


def pop_scope(var: ScopeVar, token: Token) -> None:
    """Restore the scope that was active on *var* before the matching ``push_scope``."""
    var.reset(token)


@contextmanager
def scoped_execution(var: ScopeVar) -> Iterator[ExecutionScope]:
    """Bracket a single query execution with an isolated ``ExecutionScope`` on *var*."""
    token = push_scope(var)
    try:
        yield current_scope(var)
    finally:
        pop_scope(var, token)
