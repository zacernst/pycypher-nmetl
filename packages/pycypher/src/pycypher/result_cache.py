"""LRU query result cache with size-bounded eviction and TTL support.

Extracted from ``star.py`` to provide a focused, single-responsibility module
for query result caching.

The cache uses an ``OrderedDict`` for O(1) LRU operations, a readers-writer
lock for concurrent stats reads, adaptive timeouts based on operation
complexity, deadlock detection with owner-thread tracking, and generation-based
invalidation triggered by mutation commits.

Usage::

    cache = ResultCache(max_size_bytes=100 * 1024 * 1024, ttl_seconds=300)
    cache.put("MATCH (n) RETURN n", None, result_df)
    cached = cache.get("MATCH (n) RETURN n", None)  # returns copy or None

"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from collections import OrderedDict
from typing import Any

import pandas as pd
from shared.logger import LOGGER

from pycypher.config import RESULT_CACHE_MAX_MB as _DEFAULT_RESULT_CACHE_MAX_MB
from pycypher.config import RESULT_CACHE_TTL_S as _DEFAULT_RESULT_CACHE_TTL_S
from pycypher.exceptions import CacheLockTimeoutError

__all__ = ["ReadWriteLock", "ResultCache"]


# ---------------------------------------------------------------------------
# ReadWriteLock — allows concurrent readers, exclusive writers
# ---------------------------------------------------------------------------


class ReadWriteLock:
    """A readers-writer lock with timeout support and owner tracking.

    Multiple threads can hold the read lock concurrently. The write lock
    is exclusive — no readers or other writers may hold a lock while the
    write lock is held.

    All lock acquisitions use timeout-bounded waits to prevent deadlocks.
    The current write-lock owner thread is tracked for diagnostics.
    """

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._readers_ok = threading.Condition(self._state_lock)
        self._writers_ok = threading.Condition(self._state_lock)
        self._active_readers: int = 0
        self._active_writers: int = 0
        self._waiting_writers: int = 0
        self._write_owner: int | None = None

    @property
    def write_owner(self) -> int | None:
        """Thread ident of the current write-lock holder, or ``None``."""
        return self._write_owner

    def acquire_read(self, timeout: float) -> bool:
        """Acquire a shared read lock.

        Returns ``True`` if acquired within *timeout* seconds.
        """
        deadline = time.monotonic() + timeout
        with self._state_lock:
            while self._active_writers > 0 or self._waiting_writers > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._readers_ok.wait(timeout=remaining)
            self._active_readers += 1
            return True

    def release_read(self) -> None:
        """Release a shared read lock."""
        with self._state_lock:
            self._active_readers -= 1
            if self._active_readers == 0:
                self._writers_ok.notify()

    def acquire_write(self, timeout: float) -> bool:
        """Acquire an exclusive write lock.

        Returns ``True`` if acquired within *timeout* seconds.
        Returns ``False`` immediately if the current thread already holds
        the write lock (re-entrant deadlock prevention).
        """
        # Fast re-entrancy check — if this thread already holds the write
        # lock, waiting would deadlock.  Return False to let the caller's
        # timeout handler fire immediately.
        current_ident = threading.current_thread().ident
        if self._write_owner == current_ident:
            LOGGER.warning(
                "ReadWriteLock: re-entrant write acquisition attempted "
                "by thread %s — returning False to prevent deadlock",
                current_ident,
            )
            return False

        deadline = time.monotonic() + timeout
        with self._state_lock:
            self._waiting_writers += 1
            try:
                while self._active_readers > 0 or self._active_writers > 0:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._writers_ok.wait(timeout=remaining)
                self._active_writers += 1
                self._write_owner = current_ident
                return True
            finally:
                self._waiting_writers -= 1

    def release_write(self) -> None:
        """Release the exclusive write lock."""
        with self._state_lock:
            self._active_writers -= 1
            self._write_owner = None
            self._readers_ok.notify_all()
            self._writers_ok.notify()


# ---------------------------------------------------------------------------
# Adaptive timeout multipliers per operation type
# ---------------------------------------------------------------------------

_TIMEOUT_MULTIPLIERS: dict[str, float] = {
    "get": 1.0,         # Dict lookup + df.copy()
    "put": 2.0,         # May evict + df.copy() + size estimation
    "invalidate": 0.5,  # Single counter bump
    "clear": 1.0,       # dict.clear()
    "stats": 0.5,       # Read counters only
}


class ResultCache:
    """LRU cache for query results with size-bounded eviction and TTL support.

    Keys are derived from the normalised query string and parameters.
    Values are copied DataFrames so mutations to the returned frame do not
    corrupt the cached entry.

    The cache is automatically invalidated when the underlying ``Context``
    commits a mutation (SET / CREATE / DELETE / MERGE / REMOVE).

    Thread-safety:

    - Mutating operations (``get``, ``put``, ``invalidate``, ``clear``)
      acquire an exclusive write lock.
    - Read-only operations (``stats``) acquire a shared read lock, allowing
      concurrent stats collection without blocking cache operations.
    - All lock acquisitions use adaptive timeouts (scaled by operation
      complexity) to prevent deadlocks.
    - The write-lock owner thread is tracked for deadlock diagnostics.
    """

    _DEFAULT_LOCK_TIMEOUT: float = 5.0

    def __init__(
        self,
        max_size_bytes: int = _DEFAULT_RESULT_CACHE_MAX_MB * 1024 * 1024,
        ttl_seconds: float = _DEFAULT_RESULT_CACHE_TTL_S or 0.0,
        lock_timeout_seconds: float | None = None,
    ) -> None:
        """Initialize the result cache.

        Args:
            max_size_bytes: Maximum total memory for cached DataFrames.
                ``0`` disables caching entirely.
            ttl_seconds: Time-to-live per entry in seconds.
                ``0`` means entries never expire by time (only by LRU eviction
                or mutation-based invalidation).
            lock_timeout_seconds: Base timeout for lock acquisition in seconds.
                ``None`` uses the class default (5 s).  The actual timeout is
                scaled by a per-operation multiplier (e.g. ``put`` gets 2x
                the base timeout).  Set to ``0`` for non-blocking semantics.

        """
        self._max_size_bytes: int = max_size_bytes
        self._ttl_seconds: float = ttl_seconds
        self._base_lock_timeout: float = (
            lock_timeout_seconds
            if lock_timeout_seconds is not None
            else self._DEFAULT_LOCK_TIMEOUT
        )
        # OrderedDict for LRU: most-recently-used entries move to the end.
        self._entries: OrderedDict[str, tuple[pd.DataFrame, float, int]] = (
            OrderedDict()
        )
        self._current_size_bytes: int = 0
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0
        self._lock_timeouts: int = 0
        self._rwlock = ReadWriteLock()
        # Monotonically increasing counter — bumped on every mutation commit.
        # Cache entries store the generation at insertion time; a mismatch
        # means the underlying data has changed.
        self._generation: int = 0

    # -- Adaptive timeout helpers -------------------------------------------

    def _adaptive_timeout(self, operation: str) -> float:
        """Compute adaptive timeout for the given operation."""
        multiplier = _TIMEOUT_MULTIPLIERS.get(operation, 1.0)
        return self._base_lock_timeout * multiplier

    def _acquire_write(self, operation: str) -> None:
        """Acquire an exclusive write lock with adaptive timeout.

        Raises:
            CacheLockTimeoutError: If the lock cannot be acquired.
        """
        timeout = self._adaptive_timeout(operation)
        acquired = self._rwlock.acquire_write(timeout)
        if not acquired:
            self._lock_timeouts += 1
            owner = self._rwlock.write_owner
            LOGGER.warning(
                "Cache write-lock timeout after %.1fs during %r "
                "(timeouts=%d, write_owner_thread=%s)",
                timeout,
                operation,
                self._lock_timeouts,
                owner,
            )
            raise CacheLockTimeoutError(
                timeout_seconds=timeout,
                operation=operation,
            )

    def _acquire_read(self, operation: str) -> None:
        """Acquire a shared read lock with adaptive timeout.

        Raises:
            CacheLockTimeoutError: If the lock cannot be acquired.
        """
        timeout = self._adaptive_timeout(operation)
        acquired = self._rwlock.acquire_read(timeout)
        if not acquired:
            self._lock_timeouts += 1
            owner = self._rwlock.write_owner
            LOGGER.warning(
                "Cache read-lock timeout after %.1fs during %r "
                "(timeouts=%d, write_owner_thread=%s)",
                timeout,
                operation,
                self._lock_timeouts,
                owner,
            )
            raise CacheLockTimeoutError(
                timeout_seconds=timeout,
                operation=operation,
            )

    # -- Static helpers -----------------------------------------------------

    @property
    def enabled(self) -> bool:
        """Whether caching is active (max_size_bytes > 0)."""
        return self._max_size_bytes > 0

    @staticmethod
    def _make_key(query: str, parameters: dict[str, Any] | None) -> str:
        """Produce a deterministic cache key from query + parameters."""
        h = hashlib.blake2b(query.encode("utf-8"), digest_size=16)
        if parameters:
            h.update(
                json.dumps(parameters, sort_keys=True, default=str).encode(
                    "utf-8",
                ),
            )
        return h.hexdigest()

    @staticmethod
    def _estimate_df_bytes(df: pd.DataFrame) -> int:
        """Estimate the in-memory size of a DataFrame in bytes.

        Uses a fast heuristic (rows * cols * 8 bytes + index) instead of
        ``memory_usage(deep=True)`` which is expensive on object-dtype
        columns (must traverse every Python object).  The heuristic
        under-estimates for string-heavy frames but is sufficient for
        cache eviction decisions where approximate sizing is acceptable.
        """
        nrows, ncols = df.shape
        if nrows == 0 or ncols == 0:
            return 0
        # 8 bytes per cell covers numeric dtypes exactly; object columns
        # are typically larger but the cache only needs an order-of-magnitude
        # estimate to make eviction decisions.
        return nrows * ncols * 8 + nrows * 8  # data + index

    # -- Public API ---------------------------------------------------------

    def get(
        self,
        query: str,
        parameters: dict[str, Any] | None,
    ) -> pd.DataFrame | None:
        """Look up a cached result.

        Uses an exclusive write lock because cache hits mutate LRU order.

        Returns:
            A **copy** of the cached DataFrame, or ``None`` on miss.
            Returns ``None`` if the lock cannot be acquired (timeout).
        """
        if not self.enabled:
            self._misses += 1
            return None

        key = self._make_key(query, parameters)
        try:
            self._acquire_write("get")
        except CacheLockTimeoutError:
            self._misses += 1
            return None
        try:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None

            df, timestamp, generation = entry

            # Stale generation — data has been mutated since caching.
            if generation != self._generation:
                del self._entries[key]
                self._current_size_bytes -= self._estimate_df_bytes(df)
                self._misses += 1
                return None

            # TTL expiry.
            if self._ttl_seconds > 0:
                age = time.monotonic() - timestamp
                if age > self._ttl_seconds:
                    del self._entries[key]
                    self._current_size_bytes -= self._estimate_df_bytes(df)
                    self._misses += 1
                    return None

            # Move to end (most-recently-used).
            self._entries.move_to_end(key)
            self._hits += 1
            return df.copy()
        finally:
            self._rwlock.release_write()

    def put(
        self,
        query: str,
        parameters: dict[str, Any] | None,
        result: pd.DataFrame,
    ) -> None:
        """Store a query result in the cache.

        If the lock cannot be acquired within the adaptive timeout, the
        result is silently not cached (query execution is unaffected).
        """
        if not self.enabled:
            return

        # Estimate size on the original BEFORE copying.  For oversized
        # results this avoids an unnecessary O(n) copy + allocation.
        entry_bytes = self._estimate_df_bytes(result)
        if entry_bytes > self._max_size_bytes:
            return

        key = self._make_key(query, parameters)
        # Copy after size check passes.  O(1) with pandas 3.0+ CoW.
        df_copy = result.copy()

        try:
            self._acquire_write("put")
        except CacheLockTimeoutError:
            return
        try:
            # If key already exists, remove old entry first.
            if key in self._entries:
                old_df, _, _ = self._entries.pop(key)
                self._current_size_bytes -= self._estimate_df_bytes(old_df)

            # Evict LRU entries until there is room.
            while (
                self._entries
                and self._current_size_bytes + entry_bytes
                > self._max_size_bytes
            ):
                _, (evicted_df, _, _) = self._entries.popitem(last=False)
                self._current_size_bytes -= self._estimate_df_bytes(evicted_df)
                self._evictions += 1

            self._entries[key] = (
                df_copy,
                time.monotonic(),
                self._generation,
            )
            self._current_size_bytes += entry_bytes
        finally:
            self._rwlock.release_write()

    def invalidate(self) -> None:
        """Bump the generation counter, lazily invalidating all entries.

        Called when a mutation is committed to the Context.  Existing entries
        are not deleted immediately — they are evicted on the next ``get()``
        that detects the stale generation, or during LRU eviction.
        """
        try:
            self._acquire_write("invalidate")
        except CacheLockTimeoutError:
            return
        try:
            self._generation += 1
        finally:
            self._rwlock.release_write()

    def clear(self) -> None:
        """Remove all cached entries immediately."""
        try:
            self._acquire_write("clear")
        except CacheLockTimeoutError:
            return
        try:
            self._entries.clear()
            self._current_size_bytes = 0
        finally:
            self._rwlock.release_write()

    def stats(self) -> dict[str, Any]:
        """Return cache statistics.

        Uses a shared read lock so multiple threads can read stats
        concurrently without blocking cache operations.
        """
        try:
            self._acquire_read("stats")
        except CacheLockTimeoutError:
            # Return best-effort stats without the lock.
            return self._stats_snapshot()
        try:
            return self._stats_snapshot()
        finally:
            self._rwlock.release_read()

    def _stats_snapshot(self) -> dict[str, Any]:
        """Build stats dict from current counters."""
        total = self._hits + self._misses
        return {
            "result_cache_hits": self._hits,
            "result_cache_misses": self._misses,
            "result_cache_hit_rate": (
                self._hits / total if total > 0 else 0.0
            ),
            "result_cache_size_bytes": self._current_size_bytes,
            "result_cache_size_mb": round(
                self._current_size_bytes / (1024 * 1024),
                2,
            ),
            "result_cache_entries": len(self._entries),
            "result_cache_evictions": self._evictions,
            "result_cache_lock_timeouts": self._lock_timeouts,
            "result_cache_max_mb": round(
                self._max_size_bytes / (1024 * 1024),
                2,
            ),
        }
