"""Tests for pycypher.rate_limiter — query rate limiting and throttling."""

from __future__ import annotations

import threading
import time

import pytest
from pycypher.exceptions import RateLimitError
from pycypher.rate_limiter import (
    QueryRateLimiter,
    _TokenBucket,
    get_global_limiter,
    reset_global_limiter,
)

# ---------------------------------------------------------------------------
# _TokenBucket unit tests
# ---------------------------------------------------------------------------


class TestTokenBucket:
    def test_consume_within_capacity(self):
        bucket = _TokenBucket(rate=10.0, capacity=5)
        for _ in range(5):
            assert bucket.consume() is True

    def test_consume_exceeds_capacity(self):
        bucket = _TokenBucket(rate=10.0, capacity=3)
        for _ in range(3):
            bucket.consume()
        assert bucket.consume() is False

    def test_tokens_refill_over_time(self):
        bucket = _TokenBucket(rate=100.0, capacity=5)
        # Drain all tokens
        for _ in range(5):
            bucket.consume()
        assert bucket.consume() is False
        # Wait for refill (100 tokens/sec → ~10ms per token)
        time.sleep(0.05)
        assert bucket.consume() is True

    def test_available_tokens_reflects_refill(self):
        bucket = _TokenBucket(rate=1000.0, capacity=10)
        for _ in range(10):
            bucket.consume()
        assert bucket.available_tokens < 1.0
        time.sleep(0.02)
        assert bucket.available_tokens >= 1.0

    def test_tokens_capped_at_capacity(self):
        bucket = _TokenBucket(rate=1000.0, capacity=5)
        time.sleep(0.1)  # Would generate 100 tokens, but capped at 5
        assert bucket.available_tokens <= 5.0

    def test_reset(self):
        bucket = _TokenBucket(rate=1.0, capacity=5)
        for _ in range(5):
            bucket.consume()
        assert bucket.consume() is False
        bucket.reset()
        assert bucket.consume() is True

    def test_thread_safety(self):
        """Multiple threads consuming from the same bucket."""
        bucket = _TokenBucket(rate=0.0, capacity=100)  # No refill
        consumed = []
        lock = threading.Lock()

        def worker():
            count = 0
            for _ in range(20):
                if bucket.consume():
                    count += 1
            with lock:
                consumed.append(count)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Total consumed should equal capacity (100 tokens, no refill)
        assert sum(consumed) == 100


# ---------------------------------------------------------------------------
# QueryRateLimiter tests
# ---------------------------------------------------------------------------


class TestQueryRateLimiter:
    def test_disabled_by_default(self):
        limiter = QueryRateLimiter(qps=0.0)
        assert limiter.enabled is False
        # Should not raise even after many calls
        for _ in range(1000):
            limiter.acquire()

    def test_enabled_with_positive_qps(self):
        limiter = QueryRateLimiter(qps=10.0, burst=5)
        assert limiter.enabled is True
        assert limiter.qps == 10.0
        assert limiter.burst == 5

    def test_acquire_within_burst(self):
        limiter = QueryRateLimiter(qps=1.0, burst=5)
        for _ in range(5):
            limiter.acquire()  # Should not raise

    def test_acquire_exceeds_burst_raises(self):
        limiter = QueryRateLimiter(qps=1.0, burst=3)
        for _ in range(3):
            limiter.acquire()
        with pytest.raises(RateLimitError, match="Rate limit exceeded"):
            limiter.acquire()

    def test_rate_limit_error_attributes(self):
        limiter = QueryRateLimiter(qps=5.0, burst=2)
        limiter.acquire()
        limiter.acquire()
        with pytest.raises(RateLimitError) as exc_info:
            limiter.acquire()
        assert exc_info.value.qps == 5.0
        assert exc_info.value.burst == 2
        assert exc_info.value.caller_id is None

    def test_try_acquire_returns_bool(self):
        limiter = QueryRateLimiter(qps=1.0, burst=2)
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False

    def test_try_acquire_disabled(self):
        limiter = QueryRateLimiter(qps=0.0)
        assert limiter.try_acquire() is True

    def test_reset_refills_bucket(self):
        limiter = QueryRateLimiter(qps=1.0, burst=3)
        for _ in range(3):
            limiter.acquire()
        with pytest.raises(RateLimitError):
            limiter.acquire()
        limiter.reset()
        limiter.acquire()  # Should succeed after reset

    def test_negative_qps_rejected(self):
        with pytest.raises(ValueError, match="qps must be non-negative"):
            QueryRateLimiter(qps=-1.0)

    def test_zero_burst_with_enabled_rejected(self):
        with pytest.raises(ValueError, match="burst must be >= 1"):
            QueryRateLimiter(qps=5.0, burst=0)


# ---------------------------------------------------------------------------
# Per-caller rate limiting
# ---------------------------------------------------------------------------


class TestPerCallerRateLimiting:
    def test_separate_buckets_per_caller(self):
        limiter = QueryRateLimiter(qps=1.0, burst=2, per_caller=True)
        # Each caller gets their own bucket
        limiter.acquire(caller_id="alice")
        limiter.acquire(caller_id="alice")
        with pytest.raises(RateLimitError):
            limiter.acquire(caller_id="alice")

        # Bob still has his full allocation
        limiter.acquire(caller_id="bob")
        limiter.acquire(caller_id="bob")

    def test_per_caller_error_includes_caller_id(self):
        limiter = QueryRateLimiter(qps=1.0, burst=1, per_caller=True)
        limiter.acquire(caller_id="user-42")
        with pytest.raises(RateLimitError) as exc_info:
            limiter.acquire(caller_id="user-42")
        assert exc_info.value.caller_id == "user-42"
        assert "user-42" in str(exc_info.value)

    def test_per_caller_reset_specific(self):
        limiter = QueryRateLimiter(qps=1.0, burst=1, per_caller=True)
        limiter.acquire(caller_id="alice")
        limiter.acquire(caller_id="bob")
        # Both exhausted
        with pytest.raises(RateLimitError):
            limiter.acquire(caller_id="alice")
        with pytest.raises(RateLimitError):
            limiter.acquire(caller_id="bob")
        # Reset only alice
        limiter.reset(caller_id="alice")
        limiter.acquire(caller_id="alice")  # Should work
        with pytest.raises(RateLimitError):
            limiter.acquire(caller_id="bob")  # Still exhausted

    def test_none_caller_id_uses_default_bucket(self):
        limiter = QueryRateLimiter(qps=1.0, burst=1, per_caller=True)
        limiter.acquire()  # caller_id=None → "__default__"
        with pytest.raises(RateLimitError):
            limiter.acquire()


# ---------------------------------------------------------------------------
# Global limiter singleton
# ---------------------------------------------------------------------------


class TestGlobalLimiter:
    def setup_method(self):
        reset_global_limiter()

    def teardown_method(self):
        reset_global_limiter()

    def test_global_limiter_disabled_by_default(self):
        limiter = get_global_limiter()
        assert limiter.enabled is False

    def test_global_limiter_singleton(self):
        a = get_global_limiter()
        b = get_global_limiter()
        assert a is b

    def test_reset_creates_new_instance(self):
        a = get_global_limiter()
        reset_global_limiter()
        b = get_global_limiter()
        assert a is not b

    def test_global_limiter_from_env(self, monkeypatch):
        reset_global_limiter()
        monkeypatch.setenv("PYCYPHER_RATE_LIMIT_QPS", "50")
        monkeypatch.setenv("PYCYPHER_RATE_LIMIT_BURST", "20")
        # Force config reload by reimporting
        import importlib

        import pycypher.config

        importlib.reload(pycypher.config)

        reset_global_limiter()
        limiter = get_global_limiter()
        assert limiter.enabled is True
        assert limiter.qps == 50.0
        assert limiter.burst == 20

        # Cleanup: restore defaults
        monkeypatch.delenv("PYCYPHER_RATE_LIMIT_QPS")
        monkeypatch.delenv("PYCYPHER_RATE_LIMIT_BURST")
        importlib.reload(pycypher.config)
        reset_global_limiter()


# ---------------------------------------------------------------------------
# RateLimitError exception
# ---------------------------------------------------------------------------


class TestRateLimitError:
    def test_message_format(self):
        err = RateLimitError(qps=10.0, burst=5, caller_id=None)
        assert "10.0 queries/sec" in str(err)
        assert "burst=5" in str(err)

    def test_message_with_caller(self):
        err = RateLimitError(qps=5.0, burst=3, caller_id="user-99")
        assert "user-99" in str(err)

    def test_is_exception(self):
        assert issubclass(RateLimitError, Exception)

    def test_docs_hint_included(self):
        err = RateLimitError(qps=1.0, burst=1)
        # Should include docs link if configured
        assert "Rate limit exceeded" in str(err)
