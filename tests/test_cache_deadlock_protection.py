"""Tests for ResultCache deadlock protection with ReadWriteLock.

Verifies:
1. ReadWriteLock: concurrent reads, exclusive writes, timeout support, owner tracking.
2. Adaptive timeouts: per-operation multipliers scale the base timeout.
3. Graceful degradation: all cache ops return safely on lock timeout.
4. Deadlock detection: write_owner tracks the holding thread for diagnostics.
5. Atomic invalidation: generation bump is atomic under write lock.
6. Comprehensive concurrency: stress tests for mixed read/write workloads.

Run with:
    uv run pytest tests/test_cache_deadlock_protection.py -v
"""

from __future__ import annotations

import threading
import time

import pandas as pd
import pytest
from pycypher.exceptions import CacheLockTimeoutError
from pycypher.result_cache import ReadWriteLock, ResultCache


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"x": [1, 2, 3]})


# ---------------------------------------------------------------------------
# ReadWriteLock unit tests
# ---------------------------------------------------------------------------


class TestReadWriteLock:
    """Verify the readers-writer lock semantics."""

    def test_concurrent_reads_allowed(self) -> None:
        """Multiple threads can hold read locks simultaneously."""
        rwlock = ReadWriteLock()
        barrier = threading.Barrier(3)
        acquired = []

        def reader() -> None:
            ok = rwlock.acquire_read(timeout=2.0)
            acquired.append(ok)
            barrier.wait(timeout=2.0)
            rwlock.release_read()

        threads = [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert all(acquired), "All readers should acquire concurrently"

    def test_write_excludes_readers(self) -> None:
        """A write lock blocks new readers."""
        rwlock = ReadWriteLock()
        rwlock.acquire_write(timeout=1.0)

        # Try to acquire read lock — should timeout.
        ok = rwlock.acquire_read(timeout=0.1)
        assert not ok
        rwlock.release_write()

    def test_write_excludes_writers(self) -> None:
        """Only one writer at a time."""
        rwlock = ReadWriteLock()
        rwlock.acquire_write(timeout=1.0)

        ok = rwlock.acquire_write(timeout=0.1)
        assert not ok
        rwlock.release_write()

    def test_read_blocks_writers(self) -> None:
        """A held read lock blocks writers."""
        rwlock = ReadWriteLock()
        rwlock.acquire_read(timeout=1.0)

        ok = rwlock.acquire_write(timeout=0.1)
        assert not ok
        rwlock.release_read()

    def test_write_owner_tracked(self) -> None:
        """write_owner reports the holding thread's ident."""
        rwlock = ReadWriteLock()
        assert rwlock.write_owner is None

        rwlock.acquire_write(timeout=1.0)
        assert rwlock.write_owner == threading.current_thread().ident

        rwlock.release_write()
        assert rwlock.write_owner is None

    def test_write_owner_from_other_thread(self) -> None:
        """write_owner correctly identifies a different thread."""
        rwlock = ReadWriteLock()
        holder_ident = [None]

        def hold_write() -> None:
            rwlock.acquire_write(timeout=1.0)
            holder_ident[0] = threading.current_thread().ident
            time.sleep(0.3)
            rwlock.release_write()

        t = threading.Thread(target=hold_write)
        t.start()
        time.sleep(0.05)  # Let thread acquire lock.

        assert rwlock.write_owner is not None
        assert rwlock.write_owner != threading.current_thread().ident
        t.join(timeout=2.0)


# ---------------------------------------------------------------------------
# CacheLockTimeoutError
# ---------------------------------------------------------------------------


class TestCacheLockTimeoutError:
    """Verify the exception type and attributes."""

    def test_attributes(self) -> None:
        exc = CacheLockTimeoutError(timeout_seconds=5.0, operation="get")
        assert exc.timeout_seconds == 5.0
        assert exc.operation == "get"
        assert "5.0s" in str(exc)
        assert "'get'" in str(exc)

    def test_is_timeout_error(self) -> None:
        exc = CacheLockTimeoutError(timeout_seconds=1.0, operation="put")
        assert isinstance(exc, TimeoutError)


# ---------------------------------------------------------------------------
# Adaptive timeout
# ---------------------------------------------------------------------------


class TestAdaptiveTimeout:
    """Verify that timeouts scale by operation type."""

    def test_put_gets_double_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=2.0)
        assert cache._adaptive_timeout("put") == 4.0

    def test_get_uses_base_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=3.0)
        assert cache._adaptive_timeout("get") == 3.0

    def test_invalidate_gets_half_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=4.0)
        assert cache._adaptive_timeout("invalidate") == 2.0

    def test_stats_gets_half_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=6.0)
        assert cache._adaptive_timeout("stats") == 3.0

    def test_unknown_operation_uses_base(self) -> None:
        cache = ResultCache(lock_timeout_seconds=5.0)
        assert cache._adaptive_timeout("unknown") == 5.0


# ---------------------------------------------------------------------------
# Lock timeout behaviour — graceful degradation
# ---------------------------------------------------------------------------


class TestLockTimeoutBehaviour:
    """Verify that cache operations don't hang when the lock is held."""

    def _hold_write_lock(self, cache: ResultCache) -> None:
        """Hold the write lock externally to simulate contention."""
        cache._rwlock.acquire_write(timeout=1.0)

    def _release_write_lock(self, cache: ResultCache) -> None:
        cache._rwlock.release_write()

    def test_get_returns_none_on_lock_timeout(
        self, sample_df: pd.DataFrame,
    ) -> None:
        cache = ResultCache(lock_timeout_seconds=0.1)
        cache.put("RETURN 1", None, sample_df)

        self._hold_write_lock(cache)
        try:
            start = time.monotonic()
            result = cache.get("RETURN 1", None)
            elapsed = time.monotonic() - start

            assert result is None
            assert elapsed < 1.0
        finally:
            self._release_write_lock(cache)

    def test_put_skips_silently_on_lock_timeout(
        self, sample_df: pd.DataFrame,
    ) -> None:
        cache = ResultCache(lock_timeout_seconds=0.1)

        self._hold_write_lock(cache)
        try:
            start = time.monotonic()
            cache.put("RETURN 1", None, sample_df)
            elapsed = time.monotonic() - start

            assert elapsed < 1.0
        finally:
            self._release_write_lock(cache)

        assert cache.get("RETURN 1", None) is None

    def test_invalidate_skips_on_lock_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=0.1)
        gen_before = cache._generation

        self._hold_write_lock(cache)
        try:
            start = time.monotonic()
            cache.invalidate()
            elapsed = time.monotonic() - start

            assert elapsed < 1.0
        finally:
            self._release_write_lock(cache)

        assert cache._generation == gen_before

    def test_clear_skips_on_lock_timeout(
        self, sample_df: pd.DataFrame,
    ) -> None:
        cache = ResultCache(lock_timeout_seconds=0.1)
        cache.put("RETURN 1", None, sample_df)

        self._hold_write_lock(cache)
        try:
            start = time.monotonic()
            cache.clear()
            elapsed = time.monotonic() - start

            assert elapsed < 1.0
        finally:
            self._release_write_lock(cache)

        assert cache.get("RETURN 1", None) is not None

    def test_stats_returns_best_effort_on_lock_timeout(
        self, sample_df: pd.DataFrame,
    ) -> None:
        cache = ResultCache(lock_timeout_seconds=0.1)
        cache.put("RETURN 1", None, sample_df)
        cache.get("RETURN 1", None)

        self._hold_write_lock(cache)
        try:
            start = time.monotonic()
            s = cache.stats()
            elapsed = time.monotonic() - start

            assert elapsed < 1.0
            assert "result_cache_hits" in s
            assert "result_cache_lock_timeouts" in s
        finally:
            self._release_write_lock(cache)


# ---------------------------------------------------------------------------
# Lock timeout stats tracking
# ---------------------------------------------------------------------------


class TestLockTimeoutStats:
    """Verify lock timeout tracking in stats."""

    def test_lock_timeouts_counted(self, sample_df: pd.DataFrame) -> None:
        cache = ResultCache(lock_timeout_seconds=0.05)

        cache._rwlock.acquire_write(timeout=1.0)
        try:
            cache.get("q1", None)  # Should timeout.
            cache.put("q2", None, sample_df)  # Should timeout.
        finally:
            cache._rwlock.release_write()

        s = cache.stats()
        assert s["result_cache_lock_timeouts"] == 2

    def test_zero_timeouts_initially(self) -> None:
        cache = ResultCache()
        s = cache.stats()
        assert s["result_cache_lock_timeouts"] == 0


# ---------------------------------------------------------------------------
# Custom lock timeout parameter
# ---------------------------------------------------------------------------


class TestCustomLockTimeout:
    """Verify lock_timeout_seconds parameter."""

    def test_default_timeout(self) -> None:
        cache = ResultCache()
        assert cache._base_lock_timeout == ResultCache._DEFAULT_LOCK_TIMEOUT

    def test_custom_timeout(self) -> None:
        cache = ResultCache(lock_timeout_seconds=10.0)
        assert cache._base_lock_timeout == 10.0

    def test_zero_timeout_is_nonblocking(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """Zero timeout means try-lock: fail immediately if contended."""
        cache = ResultCache(lock_timeout_seconds=0)
        cache.put("RETURN 1", None, sample_df)

        cache._rwlock.acquire_write(timeout=1.0)
        try:
            start = time.monotonic()
            result = cache.get("RETURN 1", None)
            elapsed = time.monotonic() - start

            assert result is None
            assert elapsed < 0.5
        finally:
            cache._rwlock.release_write()


# ---------------------------------------------------------------------------
# Deadlock detection diagnostics
# ---------------------------------------------------------------------------


class TestDeadlockDetection:
    """Verify write_owner is available for deadlock diagnostics."""

    def test_no_owner_initially(self) -> None:
        cache = ResultCache()
        assert cache._rwlock.write_owner is None

    def test_owner_set_during_put(self, sample_df: pd.DataFrame) -> None:
        """The write_owner should be set while put() holds the lock."""
        cache = ResultCache()
        owner_during_put = [None]

        original_put = cache.put

        def instrumented_put(
            q: str, p: dict | None, r: pd.DataFrame,
        ) -> None:
            # We need to check owner while lock is held, so instrument
            # by running put in a thread and checking from outside.
            original_put(q, p, r)

        # Run put in a thread and observe owner from another.
        holder_ident = [None]
        started = threading.Event()
        proceed = threading.Event()

        def slow_writer() -> None:
            cache._acquire_write("put")
            holder_ident[0] = threading.current_thread().ident
            started.set()
            proceed.wait(timeout=2.0)
            cache._rwlock.release_write()

        t = threading.Thread(target=slow_writer)
        t.start()
        started.wait(timeout=2.0)

        # The write owner should be the writer thread.
        assert cache._rwlock.write_owner == holder_ident[0]
        assert cache._rwlock.write_owner != threading.current_thread().ident

        proceed.set()
        t.join(timeout=2.0)


# ---------------------------------------------------------------------------
# Atomic invalidation
# ---------------------------------------------------------------------------


class TestAtomicInvalidation:
    """Verify generation-based invalidation is atomic."""

    def test_invalidate_bumps_generation(self) -> None:
        cache = ResultCache()
        gen0 = cache._generation
        cache.invalidate()
        assert cache._generation == gen0 + 1

    def test_stale_entries_evicted_on_get(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """After invalidation, cached entries return None on get."""
        cache = ResultCache()
        cache.put("RETURN 1", None, sample_df)
        assert cache.get("RETURN 1", None) is not None

        cache.invalidate()
        assert cache.get("RETURN 1", None) is None

    def test_concurrent_invalidate_is_atomic(self) -> None:
        """Multiple concurrent invalidations each bump generation once."""
        cache = ResultCache()
        n_threads = 10
        n_ops = 100

        def invalidator() -> None:
            for _ in range(n_ops):
                cache.invalidate()

        threads = [
            threading.Thread(target=invalidator) for _ in range(n_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert cache._generation == n_threads * n_ops


# ---------------------------------------------------------------------------
# Comprehensive concurrency stress tests
# ---------------------------------------------------------------------------


class TestConcurrentAccess:
    """Verify no deadlock or data corruption under concurrent access."""

    def test_concurrent_put_get_no_deadlock(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """Multiple threads doing put/get don't deadlock."""
        cache = ResultCache(lock_timeout_seconds=2.0)
        errors: list[str] = []

        def writer(thread_id: int) -> None:
            try:
                for i in range(50):
                    cache.put(f"q{thread_id}_{i}", None, sample_df)
            except Exception as e:
                errors.append(f"writer {thread_id}: {e}")

        def reader(thread_id: int) -> None:
            try:
                for i in range(50):
                    cache.get(f"q{thread_id}_{i}", None)
            except Exception as e:
                errors.append(f"reader {thread_id}: {e}")

        threads = []
        for i in range(4):
            threads.append(threading.Thread(target=writer, args=(i,)))
            threads.append(threading.Thread(target=reader, args=(i,)))

        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)
        elapsed = time.monotonic() - start

        assert elapsed < 10.0, "Threads appear to have deadlocked"
        alive = [t for t in threads if t.is_alive()]
        assert not alive, f"{len(alive)} threads still alive (deadlock?)"
        assert not errors, f"Thread errors: {errors}"

    def test_concurrent_mixed_operations(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """Mixed put/get/invalidate/stats/clear under heavy contention."""
        cache = ResultCache(lock_timeout_seconds=2.0)
        errors: list[str] = []

        def mixed_worker(thread_id: int) -> None:
            try:
                for i in range(30):
                    op = i % 5
                    if op == 0:
                        cache.put(f"q{thread_id}_{i}", None, sample_df)
                    elif op == 1:
                        cache.get(f"q{thread_id}_{i}", None)
                    elif op == 2:
                        cache.stats()
                    elif op == 3:
                        cache.invalidate()
                    else:
                        cache.clear()
            except Exception as e:
                errors.append(f"worker {thread_id}: {e}")

        threads = [
            threading.Thread(target=mixed_worker, args=(i,))
            for i in range(8)
        ]

        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15.0)
        elapsed = time.monotonic() - start

        assert elapsed < 15.0
        alive = [t for t in threads if t.is_alive()]
        assert not alive, f"{len(alive)} threads still alive"
        assert not errors, f"Thread errors: {errors}"

    def test_concurrent_stats_dont_block_each_other(self) -> None:
        """Multiple stats() calls can proceed concurrently (read lock)."""
        cache = ResultCache(lock_timeout_seconds=2.0)
        barrier = threading.Barrier(4, timeout=5.0)
        results: list[dict] = []
        errors: list[str] = []

        def stats_reader() -> None:
            try:
                barrier.wait()
                s = cache.stats()
                results.append(s)
            except Exception as e:
                errors.append(str(e))

        threads = [
            threading.Thread(target=stats_reader) for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5.0)

        assert not errors, f"Errors: {errors}"
        assert len(results) == 4

    def test_size_tracking_consistent_under_contention(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """Size tracking remains consistent under concurrent writes."""
        cache = ResultCache(
            lock_timeout_seconds=2.0,
            max_size_bytes=10 * 1024 * 1024,
        )
        errors: list[str] = []

        def writer(thread_id: int) -> None:
            try:
                for i in range(20):
                    cache.put(f"q{thread_id}_{i}", None, sample_df)
            except Exception as e:
                errors.append(f"writer {thread_id}: {e}")

        threads = [
            threading.Thread(target=writer, args=(i,)) for i in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        assert not errors
        # Size tracking should be non-negative and consistent.
        assert cache._current_size_bytes >= 0
        s = cache.stats()
        assert s["result_cache_size_bytes"] >= 0
        assert s["result_cache_entries"] == len(cache._entries)

    def test_high_contention_100_threads(
        self, sample_df: pd.DataFrame,
    ) -> None:
        """100+ concurrent threads doing mixed ops must not deadlock."""
        cache = ResultCache(lock_timeout_seconds=5.0)
        errors: list[str] = []
        barrier = threading.Barrier(100, timeout=10.0)

        def worker(thread_id: int) -> None:
            try:
                barrier.wait()
                for i in range(20):
                    op = (thread_id + i) % 5
                    if op == 0:
                        cache.put(f"q{thread_id}_{i}", None, sample_df)
                    elif op == 1:
                        cache.get(f"q{thread_id}_{i}", None)
                    elif op == 2:
                        cache.stats()
                    elif op == 3:
                        cache.invalidate()
                    else:
                        cache.clear()
            except threading.BrokenBarrierError:
                pass  # Barrier timeout is acceptable under load
            except Exception as e:
                errors.append(f"worker {thread_id}: {e}")

        threads = [
            threading.Thread(target=worker, args=(i,)) for i in range(100)
        ]
        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30.0)
        elapsed = time.monotonic() - start

        assert elapsed < 30.0, "Threads appear to have deadlocked"
        alive = [t for t in threads if t.is_alive()]
        assert not alive, f"{len(alive)} threads still alive (deadlock?)"
        assert not errors, f"Thread errors: {errors}"

    def test_reentrant_write_prevented(self) -> None:
        """Same-thread re-entrant write acquisition returns False."""
        rwlock = ReadWriteLock()
        assert rwlock.acquire_write(timeout=1.0)
        # Same thread trying to acquire again should fail immediately
        assert not rwlock.acquire_write(timeout=0.1)
        rwlock.release_write()
