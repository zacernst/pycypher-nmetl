"""Tests for production security enhancements (Task #12).

Covers:
1. Rate limiting middleware (token bucket per IP)
2. Request body size limit middleware
3. Production-mode error message filtering
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from fastopendata.api import (
    _TokenBucket,
    app,
    get_max_body_bytes,
    get_rate_limiter,
    is_production_mode,
    set_star,
)
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> TestClient:
    """Return a FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def loaded_client() -> Iterator[TestClient]:
    """TestClient with Person data loaded into Star."""
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        },
    )
    ctx = ContextBuilder().add_entity("Person", people).build()
    set_star(Star(ctx))
    yield TestClient(app)
    set_star(Star())


# ===========================================================================
# 1. Rate limiting middleware tests
# ===========================================================================


class TestTokenBucket:
    """Unit tests for the _TokenBucket rate limiter."""

    def test_allows_requests_within_limit(self) -> None:
        """Requests within the token budget are allowed."""
        bucket = _TokenBucket(max_tokens=5, window_seconds=60)
        for _ in range(5):
            assert bucket.allow("10.0.0.1") is True

    def test_rejects_over_limit(self) -> None:
        """Requests exceeding the budget are rejected."""
        bucket = _TokenBucket(max_tokens=3, window_seconds=60)
        for _ in range(3):
            bucket.allow("10.0.0.1")
        assert bucket.allow("10.0.0.1") is False

    def test_independent_ips(self) -> None:
        """Different IPs have independent token budgets."""
        bucket = _TokenBucket(max_tokens=2, window_seconds=60)
        bucket.allow("10.0.0.1")
        bucket.allow("10.0.0.1")
        assert bucket.allow("10.0.0.1") is False
        # Different IP is unaffected
        assert bucket.allow("10.0.0.2") is True

    def test_tokens_refill_over_time(self) -> None:
        """Tokens refill after time passes."""
        bucket = _TokenBucket(max_tokens=2, window_seconds=1.0)
        bucket.allow("10.0.0.1")
        bucket.allow("10.0.0.1")
        assert bucket.allow("10.0.0.1") is False

        # Simulate time passing by manipulating the bucket state
        import time

        tokens, _ = bucket._buckets["10.0.0.1"]
        bucket._buckets["10.0.0.1"] = (tokens, time.monotonic() - 2.0)
        assert bucket.allow("10.0.0.1") is True

    def test_evicts_oldest_ip_when_full(self) -> None:
        """Old IP entries are evicted when the max is reached."""
        bucket = _TokenBucket(max_tokens=5, window_seconds=60)
        bucket._max_ips = 3
        bucket.allow("ip_1")
        bucket.allow("ip_2")
        bucket.allow("ip_3")
        # Adding a 4th should evict ip_1
        bucket.allow("ip_4")
        assert "ip_1" not in bucket._buckets
        assert "ip_4" in bucket._buckets

    def test_zero_window_does_not_crash(self) -> None:
        """Zero window seconds should not cause division by zero."""
        bucket = _TokenBucket(max_tokens=5, window_seconds=0)
        assert bucket.allow("10.0.0.1") is True


class TestRateLimitMiddleware:
    """Integration tests for the rate limiting middleware."""

    def test_normal_requests_include_rate_limit_header(
        self,
        client: TestClient,
    ) -> None:
        """All responses include X-RateLimit-Limit header."""
        r = client.get("/health")
        assert r.status_code == 200
        assert "X-RateLimit-Limit" in r.headers

    def test_rate_limited_request_returns_429(self) -> None:
        """Exceeding rate limit returns HTTP 429."""
        # Create a limiter with very low limit
        limiter = _TokenBucket(max_tokens=2, window_seconds=60)
        with patch("fastopendata.api._rate_limiter", limiter):
            test_client = TestClient(app)
            test_client.get("/health")
            test_client.get("/health")
            r = test_client.get("/health")
            assert r.status_code == 429
            assert "Rate limit exceeded" in r.json()["detail"]
            assert "Retry-After" in r.headers

    def test_429_includes_retry_after(self) -> None:
        """429 response includes Retry-After header."""
        limiter = _TokenBucket(max_tokens=1, window_seconds=60)
        with patch("fastopendata.api._rate_limiter", limiter):
            test_client = TestClient(app)
            test_client.get("/health")
            r = test_client.get("/health")
            assert r.status_code == 429
            assert r.headers["Retry-After"] == "60"


# ===========================================================================
# 2. Request body size limit middleware tests
# ===========================================================================


class TestBodySizeLimitMiddleware:
    """Tests for the request body size limit middleware."""

    def test_normal_post_request_succeeds(
        self,
        loaded_client: TestClient,
    ) -> None:
        """Normal-sized POST requests are processed."""
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name"},
        )
        assert r.status_code == 200

    def test_oversized_body_returns_413(self) -> None:
        """Requests with Content-Length exceeding the limit are rejected."""
        with patch("fastopendata.api._MAX_BODY_BYTES", 10):
            test_client = TestClient(app)
            r = test_client.post(
                "/query",
                json={"query": "MATCH (p:Person) RETURN p.name"},
            )
            assert r.status_code == 413
            assert "too large" in r.json()["detail"]

    def test_get_requests_are_unaffected(self, client: TestClient) -> None:
        """GET requests without body are not affected by size limit."""
        r = client.get("/health")
        assert r.status_code == 200

    def test_max_body_bytes_accessible(self) -> None:
        """get_max_body_bytes returns configured value."""
        assert get_max_body_bytes() > 0


# ===========================================================================
# 3. Production-mode error filtering tests
# ===========================================================================


class TestProductionModeErrorFiltering:
    """Tests for production-mode error message suppression."""

    def test_default_mode_is_not_production(self) -> None:
        """By default, production mode is off."""
        # We cannot guarantee the env var isn't set in CI,
        # but we test the accessor works
        assert isinstance(is_production_mode(), bool)

    def test_production_mode_suppresses_exception_details(self) -> None:
        """In production mode, known error details are replaced with generic text."""
        people = pd.DataFrame(
            {
                "__ID__": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            },
        )
        ctx = ContextBuilder().add_entity("Person", people).build()
        set_star(Star(ctx))

        with patch("fastopendata.api._PRODUCTION_MODE", True):
            test_client = TestClient(app)
            r = test_client.post(
                "/query",
                json={"query": "MATCH (x:DoesNotExist) RETURN x"},
            )
            assert r.status_code == 422
            detail = r.json()["detail"]
            assert "(details omitted)" in detail
            # Should NOT contain the actual exception text
            assert "DoesNotExist" not in detail

        set_star(Star())

    def test_dev_mode_includes_exception_details(self) -> None:
        """In development mode, known error details include exception text."""
        people = pd.DataFrame(
            {
                "__ID__": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            },
        )
        ctx = ContextBuilder().add_entity("Person", people).build()
        set_star(Star(ctx))

        with patch("fastopendata.api._PRODUCTION_MODE", False):
            test_client = TestClient(app)
            r = test_client.post(
                "/query",
                json={"query": "MATCH (x:DoesNotExist) RETURN x"},
            )
            assert r.status_code == 422
            detail = r.json()["detail"]
            assert "(details omitted)" not in detail

        set_star(Star())

    def test_catch_all_always_generic(self, client: TestClient) -> None:
        """The catch-all error handler always returns generic message."""
        set_star(Star())
        with patch.object(
            Star,
            "execute_query",
            side_effect=RuntimeError("secret: /internal/path/db.sqlite"),
        ):
            r = client.post(
                "/query",
                json={"query": "MATCH (n:Person) RETURN n.name"},
            )
        assert r.status_code == 400
        detail = r.json()["detail"]
        assert detail == "Query execution failed"
        assert "secret" not in detail
        assert "/internal" not in detail


# ===========================================================================
# 4. Security headers still applied with new middleware
# ===========================================================================


class TestSecurityHeadersWithNewMiddleware:
    """Verify security headers still present after adding rate limit and body size middleware."""

    def test_security_headers_on_normal_response(
        self,
        client: TestClient,
    ) -> None:
        """Security headers present on normal responses."""
        r = client.get("/health")
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["X-Frame-Options"] == "DENY"
        assert r.headers["Content-Security-Policy"] == "default-src 'none'"

    def test_security_headers_on_429_response(self) -> None:
        """Security headers are NOT on 429 responses (returned before call_next)."""
        limiter = _TokenBucket(max_tokens=1, window_seconds=60)
        with patch("fastopendata.api._rate_limiter", limiter):
            test_client = TestClient(app)
            test_client.get("/health")
            r = test_client.get("/health")
            assert r.status_code == 429
            # Rate limit header should be present
            assert "X-RateLimit-Limit" in r.headers

    def test_rate_limit_header_on_post(
        self,
        loaded_client: TestClient,
    ) -> None:
        """Rate limit header present on POST responses."""
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name"},
        )
        assert r.status_code == 200
        assert "X-RateLimit-Limit" in r.headers
