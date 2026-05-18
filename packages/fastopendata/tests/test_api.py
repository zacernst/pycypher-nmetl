"""Tests for fastopendata.api endpoints."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from fastopendata.api import app, get_audit_log, set_star
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.star import Star


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
    # Reset to default empty Star after test
    set_star(Star())


# ── Health / root endpoints ───────────────────────────────────────────


class TestHealthEndpoints:
    def test_root_returns_ok(self, client: TestClient) -> None:
        r = client.get("/")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "api_version" in body
        assert "pycypher_version" in body

    def test_health_returns_ok(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"

    def test_root_and_health_agree(self, client: TestClient) -> None:
        root = client.get("/").json()
        health = client.get("/health").json()
        assert root["api_version"] == health["api_version"]
        assert root["pycypher_version"] == health["pycypher_version"]


# ── Dataset listing ───────────────────────────────────────────────────


class TestDatasetListing:
    def test_list_datasets_returns_list(self, client: TestClient) -> None:
        r = client.get("/datasets")
        assert r.status_code == 200
        body = r.json()
        assert "datasets" in body
        assert isinstance(body["datasets"], list)

    def test_dataset_entries_have_required_fields(
        self,
        client: TestClient,
    ) -> None:
        r = client.get("/datasets")
        for ds in r.json()["datasets"]:
            assert "name" in ds
            assert "description" in ds
            assert "format" in ds
            assert "source" in ds
            assert "approx_size" in ds


# ── Single dataset lookup ────────────────────────────────────────────


class TestDatasetLookup:
    def test_get_known_dataset(self, client: TestClient) -> None:
        listing = client.get("/datasets").json()["datasets"]
        if not listing:
            pytest.skip("No datasets configured")
        name = listing[0]["name"]

        r = client.get(f"/datasets/{name}")
        assert r.status_code == 200
        assert r.json()["name"] == name

    def test_get_unknown_dataset_returns_404(
        self,
        client: TestClient,
    ) -> None:
        r = client.get("/datasets/this_dataset_does_not_exist")
        assert r.status_code == 404

    def test_404_detail_contains_name(self, client: TestClient) -> None:
        r = client.get("/datasets/bogus")
        assert r.status_code == 404
        assert "bogus" in r.json()["detail"]


# ── Cypher query — validation (no data needed) ──────────────────────


class TestCypherQueryValidation:
    def test_invalid_cypher_returns_422(self, client: TestClient) -> None:
        r = client.post("/query", json={"query": "INVALID SYNTAX"})
        assert r.status_code == 422

    def test_invalid_cypher_has_error_details(
        self,
        client: TestClient,
    ) -> None:
        r = client.post("/query", json={"query": "INVALID"})
        assert r.status_code == 422
        detail = r.json()["detail"]
        assert isinstance(detail, list)
        assert len(detail) > 0
        assert "severity" in detail[0]
        assert "message" in detail[0]

    def test_missing_query_field_returns_422(
        self,
        client: TestClient,
    ) -> None:
        r = client.post("/query", json={})
        assert r.status_code == 422

    def test_empty_body_returns_422(self, client: TestClient) -> None:
        r = client.post(
            "/query",
            content=b"",
            headers={"content-type": "application/json"},
        )
        assert r.status_code == 422


# ── Cypher query — execution with loaded data ───────────────────────


class TestCypherQueryExecution:
    def test_match_all_returns_rows(
        self,
        loaded_client: TestClient,
    ) -> None:
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name, p.age"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 3
        names = {row["name"] for row in body["rows"]}
        assert names == {"Alice", "Bob", "Carol"}

    def test_where_filter(self, loaded_client: TestClient) -> None:
        r = loaded_client.post(
            "/query",
            json={
                "query": "MATCH (p:Person) WHERE p.age > 28 RETURN p.name",
            },
        )
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 2
        names = {row["name"] for row in body["rows"]}
        assert names == {"Alice", "Carol"}

    def test_count_aggregation(self, loaded_client: TestClient) -> None:
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN count(p)"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == 1

    def test_unknown_label_returns_422(
        self,
        loaded_client: TestClient,
    ) -> None:
        """Querying an unregistered entity type returns a 422 error."""
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (x:DoesNotExist) RETURN x"},
        )
        assert r.status_code == 422
        assert "unknown type or variable" in r.json()["detail"]

    def test_response_preserves_query_text(
        self,
        loaded_client: TestClient,
    ) -> None:
        query = "MATCH (p:Person) RETURN p.name"
        r = loaded_client.post("/query", json={"query": query})
        assert r.json()["query"] == query

    def test_row_values_are_correct_types(
        self,
        loaded_client: TestClient,
    ) -> None:
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name, p.age"},
        )
        for row in r.json()["rows"]:
            assert isinstance(row["name"], str)
            assert isinstance(row["age"], int | float)

    def test_query_with_no_data_returns_422(
        self,
        client: TestClient,
    ) -> None:
        """Without loaded data, querying an entity type returns a 422 error.

        The query is syntactically valid so validate_query passes it, but
        execution fails with GraphTypeNotFoundError because no entity types
        are registered in the empty Star context.
        """
        set_star(Star())
        r = client.post(
            "/query",
            json={"query": "MATCH (n:Person) RETURN n.name"},
        )
        assert r.status_code == 422
        assert "unknown type or variable" in r.json()["detail"]

    def test_default_parameters_is_empty_dict(
        self,
        loaded_client: TestClient,
    ) -> None:
        """Omitting parameters should default to empty dict."""
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name"},
        )
        assert r.status_code == 200

    def test_multiple_queries_sequentially(
        self,
        loaded_client: TestClient,
    ) -> None:
        queries = [
            "MATCH (p:Person) RETURN p.name",
            "MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            "MATCH (p:Person) RETURN count(p)",
        ]
        for q in queries:
            r = loaded_client.post("/query", json={"query": q})
            assert r.status_code == 200, f"Failed for query: {q}"
            assert r.json()["query"] == q


# ── Security headers ─────────────────────────────────────────────────


class TestSecurityHeaders:
    """Verify that security headers middleware is applied to all responses."""

    def test_x_content_type_options(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.headers["X-Content-Type-Options"] == "nosniff"

    def test_x_frame_options(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.headers["X-Frame-Options"] == "DENY"

    def test_content_security_policy(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.headers["Content-Security-Policy"] == "default-src 'none'"

    def test_x_xss_protection(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.headers["X-XSS-Protection"] == "1; mode=block"

    def test_referrer_policy(self, client: TestClient) -> None:
        r = client.get("/health")
        assert (
            r.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
        )

    def test_permissions_policy(self, client: TestClient) -> None:
        r = client.get("/health")
        assert "camera=()" in r.headers["Permissions-Policy"]

    def test_headers_on_error_responses(self, client: TestClient) -> None:
        """Security headers are present even on 404 responses."""
        r = client.get("/datasets/nonexistent_dataset_xyz")
        assert r.status_code == 404
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["X-Frame-Options"] == "DENY"

    def test_headers_on_post_endpoints(
        self,
        loaded_client: TestClient,
    ) -> None:
        r = loaded_client.post(
            "/query",
            json={"query": "MATCH (p:Person) RETURN p.name"},
        )
        assert r.status_code == 200
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["X-Frame-Options"] == "DENY"


# ── Exception sanitization ───────────────────────────────────────────


class TestExceptionSanitization:
    """Verify that catch-all errors do not leak internal details."""

    def test_catch_all_error_is_generic(self, client: TestClient) -> None:
        """The catch-all 400 response should not contain internal details."""
        # Force a catch-all by querying with no Star data loaded and a
        # query that passes validation but fails during execution in an
        # unexpected way.  We use set_star with a Star that will raise
        # a non-specific exception.
        from unittest.mock import patch

        set_star(Star())
        with patch.object(
            Star,
            "execute_query",
            side_effect=RuntimeError("secret internal path /app/data/x.db"),
        ):
            r = client.post(
                "/query",
                json={"query": "MATCH (n:Person) RETURN n.name"},
            )
        assert r.status_code == 400
        detail = r.json()["detail"]
        assert detail == "Query execution failed"
        assert "secret" not in detail
        assert "/app/data" not in detail


# ── Audit chain endpoints ───────────────────────────────────────────


class TestAuditEndpoints:
    """Verify /audit/verify and /audit/tail endpoints."""

    def test_verify_empty_chain(self, client: TestClient) -> None:
        r = client.get("/audit/verify")
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is True
        assert body["violations"] == []

    def test_verify_after_append(self, client: TestClient) -> None:
        log = get_audit_log()
        log.append(event="test_verify", severity="info", source="test")
        r = client.get("/audit/verify")
        assert r.status_code == 200
        body = r.json()
        assert body["valid"] is True
        assert body["record_count"] >= 1

    def test_tail_returns_records(self, client: TestClient) -> None:
        log = get_audit_log()
        log.append(event="tail_test", severity="info")
        r = client.get("/audit/tail?n=5")
        assert r.status_code == 200
        body = r.json()
        assert isinstance(body["records"], list)
        assert len(body["records"]) >= 1
        assert "event" in body["records"][-1]
        assert "hmac_signature" in body["records"][-1]

    def test_tail_default_n(self, client: TestClient) -> None:
        r = client.get("/audit/tail")
        assert r.status_code == 200

    def test_verify_has_security_headers(self, client: TestClient) -> None:
        r = client.get("/audit/verify")
        assert r.headers["X-Content-Type-Options"] == "nosniff"


# ── Data loading integration ─────────────────────────────────────────


class TestDataLoadingIntegration:
    """Tests for _load_datasets_into_star() and /admin/reload endpoint."""

    def test_load_datasets_into_star_with_data(self, tmp_path: Path) -> None:
        """_load_datasets_into_star loads CSV datasets and sets Star."""
        import csv
        from unittest.mock import patch

        from fastopendata.api import _load_datasets_into_star, get_star

        csv_path = tmp_path / "items.csv"
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["__ID__", "name"])
            w.writeheader()
            w.writerows([{"__ID__": 1, "name": "A"}, {"__ID__": 2, "name": "B"}])

        class FakeDataset:
            output_file = "items.csv"
            format = "CSV"

        class FakeConfig:
            data_path = tmp_path
            datasets = {"test_items": FakeDataset()}

        # Two patches are required:
        # 1. ``fastopendata.api.config`` — api.py uses ``from .config import
        #    config`` (line 49), so it has its own module-local binding that
        #    must be replaced for ``data_dir = config.data_path`` to see the
        #    fake.
        # 2. ``fastopendata.config.config`` — ``load_available_datasets``
        #    re-imports config from the module on each call, so the module
        #    attribute must also be replaced for ``config.datasets`` to see
        #    the fake.
        fake = FakeConfig()
        with (
            patch("fastopendata.api.config", fake),
            patch("fastopendata.config.config", fake),
        ):
            count = _load_datasets_into_star()

        assert count == 1
        star = get_star()
        result = star.execute_query("MATCH (i:TestItems) RETURN i.name")
        assert len(result) == 2
        # Clean up
        set_star(Star())

    def test_load_datasets_into_star_empty(self, tmp_path: Path) -> None:
        """_load_datasets_into_star returns 0 when no datasets found."""
        from unittest.mock import patch

        from fastopendata.api import _load_datasets_into_star

        class FakeConfig:
            data_path = tmp_path
            datasets: dict = {}

        fake = FakeConfig()
        # See test_load_datasets_into_star_with_data for why we patch both.
        with (
            patch("fastopendata.api.config", fake),
            patch("fastopendata.config.config", fake),
        ):
            count = _load_datasets_into_star()

        assert count == 0

    def test_reload_endpoint_requires_admin(self, client: TestClient) -> None:
        """POST /admin/reload returns 403 without admin key."""
        r = client.post("/admin/reload")
        assert r.status_code == 403

    def test_reload_endpoint_with_admin_key(self, tmp_path: Path) -> None:
        """POST /admin/reload with valid admin key reloads datasets."""
        import csv
        from unittest.mock import patch

        csv_path = tmp_path / "things.csv"
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["__ID__", "val"])
            w.writeheader()
            w.writerows([{"__ID__": 1, "val": 42}])

        class FakeDataset:
            output_file = "things.csv"
            format = "CSV"

        class FakeConfig:
            data_path = tmp_path
            datasets = {"test_things": FakeDataset()}

        # See test_load_datasets_into_star_with_data for why we patch both
        # ``fastopendata.api.config`` and ``fastopendata.config.config``.
        fake = FakeConfig()
        with (
            patch("fastopendata.api._ADMIN_API_KEY", "test-admin-key"),
            patch("fastopendata.api.config", fake),
            patch("fastopendata.config.config", fake),
        ):
            client = TestClient(app)
            r = client.post(
                "/admin/reload",
                headers={"authorization": "Bearer test-admin-key"},
            )

        assert r.status_code == 200
        body = r.json()
        assert body["loaded_entity_types"] == 1
        assert "Reloaded" in body["message"]
        # Clean up
        set_star(Star())

    def test_reload_endpoint_wrong_admin_key(self) -> None:
        """POST /admin/reload with wrong admin key returns 403."""
        from unittest.mock import patch

        with patch("fastopendata.api._ADMIN_API_KEY", "real-key"):
            client = TestClient(app)
            r = client.post(
                "/admin/reload",
                headers={"authorization": "Bearer wrong-key"},
            )

        assert r.status_code == 403


# ── STATE_FIPS environment variable dispatch ─────────────────────────


class TestStateFipsEnvVar:
    """``STATE_FIPS`` env var selects which state ``_load_datasets_into_star`` builds.

    These tests don't exercise the full pipeline (that's covered by
    ``test_etl_pipeline.py``); they verify only that the env-var → state_fips
    dispatch works correctly. We mock ``build_state_pipeline`` to capture the
    arguments it's called with.
    """

    @staticmethod
    def _seed_state_data(data_dir: Path, state_fips: str) -> None:
        """Create empty-but-present contracts + crosswalk so the
        ``state_contracts.exists() and crosswalk.exists()`` branch is taken.
        """
        (data_dir / f"contracts_state_{state_fips}.csv").write_text("")
        (data_dir / "state_county_tract_puma.csv").write_text("")

    def test_default_state_is_georgia(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without ``STATE_FIPS``, the loader defaults to FIPS ``13`` (Georgia)."""
        from unittest.mock import MagicMock

        from fastopendata.api import _load_datasets_into_star

        self._seed_state_data(tmp_path, "13")

        # data_path is read from api.config — patch the api-local binding.
        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        captured: dict[str, str] = {}

        def _fake_build(data_dir, *, state_fips, **_kwargs):
            captured["state_fips"] = state_fips
            from fastopendata.pipeline import GraphPipeline
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", _fake_build,
        )
        monkeypatch.delenv("STATE_FIPS", raising=False)

        _load_datasets_into_star()
        assert captured["state_fips"] == "13"

    def test_state_fips_env_var_selects_california(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``STATE_FIPS=06`` routes to the California pipeline."""
        from unittest.mock import MagicMock

        from fastopendata.api import _load_datasets_into_star

        self._seed_state_data(tmp_path, "06")

        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        captured: dict[str, str] = {}

        def _fake_build(data_dir, *, state_fips, **_kwargs):
            captured["state_fips"] = state_fips
            from fastopendata.pipeline import GraphPipeline
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", _fake_build,
        )
        monkeypatch.setenv("STATE_FIPS", "06")

        _load_datasets_into_star()
        assert captured["state_fips"] == "06"

    def test_state_fips_is_normalized(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Whitespace and missing leading zeros are normalized.

        ``STATE_FIPS=6`` and ``STATE_FIPS=" 06\n"`` both land on ``"06"``
        so all downstream lookups (file paths, _STATE_INFO) work.
        """
        from unittest.mock import MagicMock

        from fastopendata.api import _load_datasets_into_star

        # Seed file under the canonical zero-padded name; if normalization
        # works, the loader will find it.
        self._seed_state_data(tmp_path, "06")

        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        captured: list[str] = []

        def _fake_build(data_dir, *, state_fips, **_kwargs):
            captured.append(state_fips)
            from fastopendata.pipeline import GraphPipeline
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", _fake_build,
        )

        for raw in ("6", " 06 ", "06\n", "6\t"):
            monkeypatch.setenv("STATE_FIPS", raw)
            _load_datasets_into_star()

        assert captured == ["06", "06", "06", "06"], (
            f"normalization failed: got {captured!r}"
        )

    def test_state_fips_logs_state_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The loader logs the resolved state name (not just the FIPS)."""
        import logging
        from unittest.mock import MagicMock

        from fastopendata.api import _load_datasets_into_star

        self._seed_state_data(tmp_path, "48")  # Texas

        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)

        def _fake_build(*_args, **_kwargs):
            from fastopendata.pipeline import GraphPipeline
            return GraphPipeline()

        monkeypatch.setattr(
            "fastopendata.etl.state_pipeline.build_state_pipeline", _fake_build,
        )
        monkeypatch.setenv("STATE_FIPS", "48")

        with caplog.at_level(logging.INFO, logger="fastopendata.api"):
            _load_datasets_into_star()

        # The configured-state line should mention Texas + the FIPS.
        config_lines = [
            r.message for r in caplog.records
            if "Configured to load state" in r.message
        ]
        assert any("Texas" in line and "48" in line for line in config_lines), (
            f"expected Texas+48 in log, got: {config_lines}"
        )

    def test_unknown_state_fips_uses_fallback_label(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An unknown FIPS still works — log shows the synthetic label."""
        import logging
        import sys
        from unittest.mock import MagicMock

        from fastopendata.api import _load_datasets_into_star

        # No data files for FIPS 99 → falls through to load_available_datasets.
        # api.py uses ``from .config import config`` (the local binding), so
        # patching fastopendata.api.config covers the data_path read.
        fake_config = MagicMock()
        fake_config.data_path = tmp_path
        monkeypatch.setattr("fastopendata.api.config", fake_config)
        # ``load_available_datasets`` does its own ``from fastopendata.config
        # import config`` inside the function body, so we also need to swap
        # the attribute on the underlying module. NB: ``import
        # fastopendata.config`` gives us the *Config instance* (the package
        # __init__ re-exports it under that name), so to reach the actual
        # module we go through ``sys.modules``.
        config_module = sys.modules["fastopendata.config"]
        fake_pkg_config = MagicMock()
        fake_pkg_config.data_path = tmp_path
        fake_pkg_config.datasets = {}
        monkeypatch.setattr(config_module, "config", fake_pkg_config)
        monkeypatch.setenv("STATE_FIPS", "99")

        with caplog.at_level(logging.INFO, logger="fastopendata.api"):
            count = _load_datasets_into_star()

        assert count == 0  # no data anywhere
        config_lines = [
            r.message for r in caplog.records
            if "Configured to load state" in r.message
        ]
        # Synthetic fallback label is "FIPS 99".
        assert any("FIPS 99" in line for line in config_lines), (
            f"expected FIPS 99 fallback label in log, got: {config_lines}"
        )
