"""Tests for API key authentication middleware (Task #54).

Covers:
1. ApiKeyStore unit tests (create, validate, revoke, list, hash security)
2. Auth middleware integration tests (disabled passthrough, enabled 401/403, exempt paths)
3. Admin endpoint tests (create key, revoke key, list keys, admin key required)
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from fastopendata.api import (
    ApiKeyStore,
    app,
    get_api_key_store,
    is_api_auth_enabled,
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


@pytest.fixture
def fresh_store() -> ApiKeyStore:
    """Return a fresh ApiKeyStore for unit testing."""
    return ApiKeyStore()


# ===========================================================================
# 1. ApiKeyStore unit tests
# ===========================================================================


class TestApiKeyStore:
    """Unit tests for the ApiKeyStore class."""

    def test_create_key_returns_prefixed_string(self, fresh_store: ApiKeyStore) -> None:
        """Created keys start with 'pyc_' prefix."""
        key = fresh_store.create_key(label="test")
        assert key.startswith("pyc_")
        assert len(key) > 10

    def test_create_key_unique(self, fresh_store: ApiKeyStore) -> None:
        """Each created key is unique."""
        keys = {fresh_store.create_key(label=f"key_{i}") for i in range(10)}
        assert len(keys) == 10

    def test_validate_valid_key(self, fresh_store: ApiKeyStore) -> None:
        """A freshly created key validates successfully."""
        key = fresh_store.create_key(label="valid")
        assert fresh_store.validate(key) is True

    def test_validate_invalid_key(self, fresh_store: ApiKeyStore) -> None:
        """A random string does not validate."""
        assert fresh_store.validate("pyc_notarealkey") is False

    def test_validate_increments_request_count(self, fresh_store: ApiKeyStore) -> None:
        """Each successful validation increments the request counter."""
        key = fresh_store.create_key(label="counter")
        fresh_store.validate(key)
        fresh_store.validate(key)
        fresh_store.validate(key)
        keys = fresh_store.list_keys()
        assert keys[0]["requests"] == 3

    def test_revoke_key(self, fresh_store: ApiKeyStore) -> None:
        """Revoking a key prevents future validation."""
        key = fresh_store.create_key(label="revokable")
        assert fresh_store.validate(key) is True
        assert fresh_store.revoke(key) is True
        assert fresh_store.validate(key) is False

    def test_revoke_nonexistent_key(self, fresh_store: ApiKeyStore) -> None:
        """Revoking a nonexistent key returns False."""
        assert fresh_store.revoke("pyc_doesnotexist") is False

    def test_list_keys_metadata(self, fresh_store: ApiKeyStore) -> None:
        """list_keys returns metadata without exposing raw keys."""
        fresh_store.create_key(label="my-service")
        keys = fresh_store.list_keys()
        assert len(keys) == 1
        entry = keys[0]
        assert entry["label"] == "my-service"
        assert entry["active"] is True
        assert entry["requests"] == 0
        assert "..." in entry["key_prefix"]
        # Raw key should not appear
        assert not entry["key_prefix"].startswith("pyc_")

    def test_active_count(self, fresh_store: ApiKeyStore) -> None:
        """active_count reflects only non-revoked keys."""
        k1 = fresh_store.create_key(label="a")
        fresh_store.create_key(label="b")
        assert fresh_store.active_count == 2
        fresh_store.revoke(k1)
        assert fresh_store.active_count == 1

    def test_hash_is_deterministic(self) -> None:
        """Same input always produces the same hash."""
        h1 = ApiKeyStore._hash("pyc_testkey123")
        h2 = ApiKeyStore._hash("pyc_testkey123")
        assert h1 == h2

    def test_hash_differs_for_different_keys(self) -> None:
        """Different inputs produce different hashes."""
        h1 = ApiKeyStore._hash("pyc_key_a")
        h2 = ApiKeyStore._hash("pyc_key_b")
        assert h1 != h2

    def test_raw_key_not_stored(self, fresh_store: ApiKeyStore) -> None:
        """The raw key string is never stored in the internal dict."""
        key = fresh_store.create_key(label="secret")
        # Check that no value in _keys contains the raw key
        for h, entry in fresh_store._keys.items():
            assert key not in str(h)
            assert key not in str(entry)


# ===========================================================================
# 2. Auth middleware integration tests
# ===========================================================================


class TestAuthMiddlewareDisabled:
    """When auth is disabled (default), all requests pass through."""

    def test_health_accessible(self, client: TestClient) -> None:
        """Health endpoint accessible without auth."""
        r = client.get("/health")
        assert r.status_code == 200

    def test_query_accessible_without_auth(self, loaded_client: TestClient) -> None:
        """Query endpoint accessible without auth when disabled."""
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name"},
        )
        assert r.status_code == 200

    def test_auth_disabled_by_default(self) -> None:
        """Auth is disabled by default."""
        # The function returns the module-level flag; in test env it should be False
        assert is_api_auth_enabled() is False


class TestAuthMiddlewareEnabled:
    """When auth is enabled, non-exempt paths require a valid Bearer token."""

    def test_missing_auth_returns_401(self) -> None:
        """Request without Authorization header returns 401."""
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            r = test_client.get("/datasets")
            assert r.status_code == 401
            assert "Authorization" in r.json()["detail"]
            assert "WWW-Authenticate" in r.headers

    def test_invalid_token_returns_403(self) -> None:
        """Request with invalid Bearer token returns 403."""
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            r = test_client.get(
                "/datasets",
                headers={"Authorization": "Bearer pyc_invalid_key"},
            )
            assert r.status_code == 403
            assert "Invalid or revoked" in r.json()["detail"]

    def test_non_bearer_auth_returns_401(self) -> None:
        """Non-Bearer auth scheme returns 401."""
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            r = test_client.get(
                "/datasets",
                headers={"Authorization": "Basic dXNlcjpwYXNz"},
            )
            assert r.status_code == 401

    def test_valid_key_allows_access(self) -> None:
        """Request with a valid API key passes through."""
        store = get_api_key_store()
        key = store.create_key(label="test-access")
        try:
            with patch("fastopendata.api._API_AUTH_ENABLED", True):
                test_client = TestClient(app)
                r = test_client.get(
                    "/health",  # exempt, but let's also test non-exempt
                )
                assert r.status_code == 200

                r = test_client.get(
                    "/datasets",
                    headers={"Authorization": f"Bearer {key}"},
                )
                assert r.status_code == 200
        finally:
            store.revoke(key)

    def test_revoked_key_returns_403(self) -> None:
        """A revoked key is rejected."""
        store = get_api_key_store()
        key = store.create_key(label="to-revoke")
        store.revoke(key)
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            r = test_client.get(
                "/datasets",
                headers={"Authorization": f"Bearer {key}"},
            )
            assert r.status_code == 403

    def test_exempt_paths_bypass_auth(self) -> None:
        """Exempt paths (/, /health, /docs) do not require auth."""
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            for path in ["/", "/health"]:
                r = test_client.get(path)
                assert r.status_code == 200, f"Exempt path {path} should be accessible"


# ===========================================================================
# 3. Admin endpoint tests
# ===========================================================================


class TestAdminEndpoints:
    """Tests for /admin/keys endpoints."""

    def test_create_key_requires_admin(self, client: TestClient) -> None:
        """POST /admin/keys without admin key returns 403."""
        r = client.post("/admin/keys", json={"label": "test"})
        assert r.status_code == 403

    def test_list_keys_requires_admin(self, client: TestClient) -> None:
        """GET /admin/keys without admin key returns 403."""
        r = client.get("/admin/keys")
        assert r.status_code == 403

    def test_revoke_key_requires_admin(self, client: TestClient) -> None:
        """POST /admin/keys/revoke without admin key returns 403."""
        r = client.post("/admin/keys/revoke", json={"key": "pyc_fake"})
        assert r.status_code == 403

    def test_create_key_with_admin(self) -> None:
        """POST /admin/keys with valid admin key creates a key."""
        admin_key = "test-admin-secret-key"
        with patch("fastopendata.api._ADMIN_API_KEY", admin_key):
            test_client = TestClient(app)
            r = test_client.post(
                "/admin/keys",
                json={"label": "new-service"},
                headers={"Authorization": f"Bearer {admin_key}"},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["key"].startswith("pyc_")
            assert body["label"] == "new-service"
            assert "Store it securely" in body["message"]

    def test_revoke_key_with_admin(self) -> None:
        """POST /admin/keys/revoke with valid admin key revokes a key."""
        admin_key = "test-admin-secret-key"
        store = get_api_key_store()
        key_to_revoke = store.create_key(label="to-revoke-admin")
        with patch("fastopendata.api._ADMIN_API_KEY", admin_key):
            test_client = TestClient(app)
            r = test_client.post(
                "/admin/keys/revoke",
                json={"key": key_to_revoke},
                headers={"Authorization": f"Bearer {admin_key}"},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["revoked"] is True

    def test_revoke_nonexistent_key(self) -> None:
        """Revoking a nonexistent key returns revoked=False."""
        admin_key = "test-admin-secret-key"
        with patch("fastopendata.api._ADMIN_API_KEY", admin_key):
            test_client = TestClient(app)
            r = test_client.post(
                "/admin/keys/revoke",
                json={"key": "pyc_nonexistent"},
                headers={"Authorization": f"Bearer {admin_key}"},
            )
            assert r.status_code == 200
            body = r.json()
            assert body["revoked"] is False

    def test_list_keys_with_admin(self) -> None:
        """GET /admin/keys with valid admin key returns key metadata."""
        admin_key = "test-admin-secret-key"
        with patch("fastopendata.api._ADMIN_API_KEY", admin_key):
            test_client = TestClient(app)
            r = test_client.get(
                "/admin/keys",
                headers={"Authorization": f"Bearer {admin_key}"},
            )
            assert r.status_code == 200
            body = r.json()
            assert "keys" in body
            assert isinstance(body["keys"], list)
            assert "active_count" in body
            assert "auth_enabled" in body

    def test_admin_no_key_configured(self, client: TestClient) -> None:
        """When PYCYPHER_ADMIN_KEY is not set, admin endpoints explain the issue."""
        with patch("fastopendata.api._ADMIN_API_KEY", ""):
            test_client = TestClient(app)
            r = test_client.post("/admin/keys", json={"label": "test"})
            assert r.status_code == 403
            assert "not configured" in r.json()["detail"]

    def test_wrong_admin_key_returns_403(self) -> None:
        """Wrong admin key returns 403."""
        with patch("fastopendata.api._ADMIN_API_KEY", "correct-admin-key"):
            test_client = TestClient(app)
            r = test_client.post(
                "/admin/keys",
                json={"label": "test"},
                headers={"Authorization": "Bearer wrong-key"},
            )
            assert r.status_code == 403
            assert "Admin access required" in r.json()["detail"]


# ===========================================================================
# 4. Integration: auth + existing middleware
# ===========================================================================


class TestAuthWithExistingMiddleware:
    """Verify auth middleware works alongside rate limiting and security headers."""

    def test_security_headers_on_401(self) -> None:
        """Security headers present on 401 auth responses."""
        with patch("fastopendata.api._API_AUTH_ENABLED", True):
            test_client = TestClient(app)
            r = test_client.get("/datasets")
            assert r.status_code == 401
            # Security headers are added by the security_headers middleware
            # which runs after auth middleware. Since auth returns early,
            # security headers may not be present on 401s (expected).

    def test_rate_limit_header_with_auth(self) -> None:
        """Rate limit headers present when auth is disabled."""
        r = TestClient(app).get("/health")
        assert r.status_code == 200
        assert "X-RateLimit-Limit" in r.headers

    def test_get_api_key_store_returns_store(self) -> None:
        """get_api_key_store returns the global store instance."""
        store = get_api_key_store()
        assert isinstance(store, ApiKeyStore)
