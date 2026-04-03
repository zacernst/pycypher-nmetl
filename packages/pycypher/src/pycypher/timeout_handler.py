"""Timeout management for query execution.

Provides a context-manager that arms both cooperative and hard (SIGALRM)
timeouts and cleans up reliably in the ``finally`` block.

Usage::

    handler = TimeoutHandler(context, timeout_seconds=5.0, query_str="MATCH ...")
    with handler:
        # execute query — cooperative check_timeout() between clauses
        # SIGALRM fires if stuck in C extension beyond deadline
        ...

"""

from __future__ import annotations

import signal
import threading
import time
from types import TracebackType
from typing import Any

from pycypher.exceptions import QueryTimeoutError

__all__ = ["TimeoutHandler"]


class TimeoutHandler:
    """Arm and disarm query timeouts with cooperative + SIGALRM protection.

    The handler is a context manager.  On ``__enter__`` it:

    1. Sets the cooperative deadline on the ``Context`` (checked between clauses).
    2. Installs a SIGALRM handler on Unix main-thread (catches stuck C extensions).

    On ``__exit__`` it:

    1. Cancels the SIGALRM.
    2. Restores the previous signal handler.
    3. Clears the cooperative deadline on the ``Context``.

    Thread-safety: SIGALRM is only armed when running on the main thread.
    On non-main threads or non-Unix platforms, only the cooperative deadline
    is used.

    Args:
        context: The :class:`~pycypher.relational_models.Context` to set/clear
            the cooperative deadline on.
        timeout_seconds: Wall-clock timeout in seconds.  ``None`` means no timeout.
        query_str: Query text for inclusion in timeout error messages.
        start_time: Reference time (``time.perf_counter()``) for elapsed calculation.
            Defaults to the time of ``__enter__``.

    """

    def __init__(
        self,
        context: Any,
        *,
        timeout_seconds: float | None,
        query_str: str = "",
        start_time: float | None = None,
    ) -> None:
        self._context = context
        self._timeout_seconds = timeout_seconds
        self._query_str = query_str
        self._start_time = start_time
        self._alarm_set = False
        self._alarm_armed = False
        self._old_handler: Any = None

    @property
    def timeout_seconds(self) -> float | None:
        """The configured timeout, or ``None`` if no timeout."""
        return self._timeout_seconds

    def __enter__(self) -> TimeoutHandler:
        """Arm cooperative deadline and SIGALRM."""
        if self._start_time is None:
            self._start_time = time.perf_counter()

        # Cooperative deadline — checked by context.check_timeout() between clauses.
        self._context.set_deadline(self._timeout_seconds)

        # SIGALRM hard stop — Unix main-thread only.
        if (
            self._timeout_seconds is not None
            and self._timeout_seconds >= 0
            and hasattr(signal, "SIGALRM")
            and threading.current_thread() is threading.main_thread()
        ):
            self._alarm_armed = True
            _start = self._start_time
            _timeout = self._timeout_seconds
            _query_str = self._query_str

            def _alarm_handler(signum: int, frame: Any) -> None:
                if not self._alarm_armed:
                    return  # Stale alarm — discard silently
                elapsed = time.perf_counter() - _start
                raise QueryTimeoutError(
                    timeout_seconds=_timeout,
                    elapsed_seconds=elapsed,
                    query_fragment=_query_str,
                )

            self._old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
            signal.alarm(max(1, int(self._timeout_seconds + 1)))
            self._alarm_set = True

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Cancel SIGALRM and clear cooperative deadline."""
        # Cancel SIGALRM FIRST to prevent firing during cleanup.
        if self._alarm_set:
            self._alarm_armed = False
            signal.alarm(0)
            if self._old_handler is not None:
                signal.signal(signal.SIGALRM, self._old_handler)
            else:
                signal.signal(signal.SIGALRM, signal.SIG_DFL)
            self._alarm_set = False

        # Clear cooperative deadline.
        self._context.clear_deadline()
