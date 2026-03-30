"""Query rate limiting for resource protection in multi-tenant deployments.

Provides a thread-safe token-bucket rate limiter that can be applied
per-session or per-caller to prevent query abuse.  Integrates with the
existing audit logging and configuration infrastructure.

Configuration is via environment variables:

``PYCYPHER_RATE_LIMIT_QPS``
    Maximum sustained queries per second (token refill rate).
    ``0`` (default) disables rate limiting entirely.

``PYCYPHER_RATE_LIMIT_BURST``
    Maximum burst size (bucket capacity).  Allows short bursts above the
    sustained rate.  Defaults to ``10``.

Usage::

    from pycypher.rate_limiter import get_global_limiter

    limiter = get_global_limiter()
    limiter.acquire()  # raises RateLimitError if over limit

Programmatic usage with custom settings::

    limiter = QueryRateLimiter(qps=5.0, burst=20)
    limiter.acquire(caller_id="user-123")
"""

from __future__ import annotations

import threading
import time

from pycypher.exceptions import RateLimitError

__all__ = [
    "QueryRateLimiter",
    "get_global_limiter",
    "reset_global_limiter",
]


class _TokenBucket:
    """Thread-safe token bucket for rate limiting.

    Tokens refill at a constant rate up to a maximum capacity (burst).
    Each ``consume()`` call removes one token.  If no tokens are available,
    the call returns ``False`` without blocking.
    """

    __slots__ = ("_capacity", "_rate", "_tokens", "_last_refill", "_lock")

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def consume(self) -> bool:
        """Try to consume one token.  Returns ``True`` if successful."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._capacity,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    @property
    def available_tokens(self) -> float:
        """Current token count (approximate, for monitoring)."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            return min(
                self._capacity,
                self._tokens + elapsed * self._rate,
            )

    def reset(self) -> None:
        """Refill the bucket to full capacity."""
        with self._lock:
            self._tokens = float(self._capacity)
            self._last_refill = time.monotonic()


class QueryRateLimiter:
    """Configurable query rate limiter with optional per-caller tracking.

    When ``qps=0`` (the default from environment), the limiter is disabled
    and ``acquire()`` is a no-op.

    Args:
        qps: Sustained queries per second (token refill rate).
            ``0`` disables rate limiting.
        burst: Maximum burst size (token bucket capacity).
        per_caller: If ``True``, maintain separate buckets per ``caller_id``.
            If ``False`` (default), use a single shared bucket.

    """

    def __init__(
        self,
        qps: float = 0.0,
        burst: int = 10,
        *,
        per_caller: bool = False,
    ) -> None:
        if qps < 0:
            msg = f"qps must be non-negative, got {qps}"
            raise ValueError(msg)
        if burst < 1 and qps > 0:
            msg = f"burst must be >= 1 when rate limiting is enabled, got {burst}"
            raise ValueError(msg)

        self._qps = qps
        self._burst = burst
        self._per_caller = per_caller
        self._enabled = qps > 0

        # Shared bucket (used when per_caller=False)
        self._shared_bucket: _TokenBucket | None = (
            _TokenBucket(qps, burst)
            if self._enabled and not per_caller
            else None
        )

        # Per-caller buckets (used when per_caller=True)
        self._caller_buckets: dict[str, _TokenBucket] = {}
        self._caller_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        """Whether rate limiting is active."""
        return self._enabled

    @property
    def qps(self) -> float:
        """Configured queries-per-second rate."""
        return self._qps

    @property
    def burst(self) -> int:
        """Configured burst capacity."""
        return self._burst

    def acquire(self, *, caller_id: str | None = None) -> None:
        """Acquire permission to execute a query.

        Args:
            caller_id: Optional caller identifier for per-caller limiting.
                Ignored when ``per_caller=False``.

        Raises:
            RateLimitError: If the rate limit is exceeded.

        """
        if not self._enabled:
            return

        bucket = self._get_bucket(caller_id)
        if not bucket.consume():
            raise RateLimitError(
                qps=self._qps,
                burst=self._burst,
                caller_id=caller_id,
            )

    def try_acquire(self, *, caller_id: str | None = None) -> bool:
        """Non-raising variant of :meth:`acquire`.

        Returns:
            ``True`` if the query is allowed, ``False`` if rate-limited.

        """
        if not self._enabled:
            return True

        bucket = self._get_bucket(caller_id)
        return bucket.consume()

    def reset(self, *, caller_id: str | None = None) -> None:
        """Reset the bucket(s) to full capacity.

        Args:
            caller_id: If given and per_caller is enabled, reset only that
                caller's bucket.  Otherwise reset the shared bucket.

        """
        if not self._enabled:
            return
        if self._per_caller and caller_id is not None:
            with self._caller_lock:
                bucket = self._caller_buckets.get(caller_id)
                if bucket is not None:
                    bucket.reset()
        elif self._shared_bucket is not None:
            self._shared_bucket.reset()

    def _get_bucket(self, caller_id: str | None) -> _TokenBucket:
        """Return the appropriate bucket for this request."""
        if self._per_caller:
            key = caller_id or "__default__"
            with self._caller_lock:
                bucket = self._caller_buckets.get(key)
                if bucket is None:
                    bucket = _TokenBucket(self._qps, self._burst)
                    self._caller_buckets[key] = bucket
                return bucket
        assert self._shared_bucket is not None
        return self._shared_bucket


# ---------------------------------------------------------------------------
# Global singleton — configured from environment variables
# ---------------------------------------------------------------------------

_global_limiter: QueryRateLimiter | None = None
_global_lock = threading.Lock()


def get_global_limiter() -> QueryRateLimiter:
    """Return the process-wide rate limiter (lazily created from config).

    The limiter is configured from:
    - ``PYCYPHER_RATE_LIMIT_QPS`` (default ``0`` = disabled)
    - ``PYCYPHER_RATE_LIMIT_BURST`` (default ``10``)
    """
    global _global_limiter
    if _global_limiter is None:
        with _global_lock:
            if _global_limiter is None:
                from pycypher.config import RATE_LIMIT_BURST, RATE_LIMIT_QPS

                _global_limiter = QueryRateLimiter(
                    qps=RATE_LIMIT_QPS,
                    burst=RATE_LIMIT_BURST,
                )
    return _global_limiter


def reset_global_limiter() -> None:
    """Reset the global limiter singleton (primarily for testing)."""
    global _global_limiter
    with _global_lock:
        _global_limiter = None
