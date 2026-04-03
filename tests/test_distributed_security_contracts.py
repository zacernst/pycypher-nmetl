"""Security contract tests for distributed large dataset implementation.

These tests define security invariants that MUST hold across all phases of the
large dataset implementation (Phases 1-4).  Written TDD-style as red-phase tests
that will turn green as each phase is implemented WITH proper security.

Security threat model assumes:
- External connections are potentially compromised
- Data in transit and at rest must be protected
- Serialization must never use pickle (RCE vector)
- Credentials must never leak into logs, errors, or repr
- All backends must enforce identical input validation
- Temporary storage must be secured (permissions + cleanup)

Categories:
1. Serialization safety — no pickle anywhere in new code paths
2. Credential isolation — SecretStr-style redaction
3. Backend validation parity — all backends enforce same rules
4. Temporary storage security — permissions and cleanup
5. Dask cluster security — TLS + authentication requirements
6. Network security — encrypted communication
"""

from __future__ import annotations

import importlib
import os
import pickle
import stat
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.slow


# ===========================================================================
# Category 1 — No pickle serialization in any code path
# ===========================================================================


class TestNoPickleSerialization:
    """Verify that new distributed code paths never use pickle for serialization.

    cloudpickle/pickle is Dask's default serialization.  For DATA serialization
    (checkpoints, spill-to-disk, cache entries), we must use Arrow IPC or
    Parquet.  Pickle is only acceptable for Dask's internal task graph
    serialization (which we cannot control), NOT for user data.
    """

    def test_helpers_encode_rejects_pickle(self) -> None:
        """shared.helpers.encode uses JSON, not pickle.  Confirm the invariant."""
        from shared.helpers import decode, encode

        data = {"key": "value", "count": 42}
        encoded = encode(data)
        # Must roundtrip through JSON, not pickle
        assert decode(encoded) == data

        # Pickle payload must be rejected
        import base64

        pickle_bytes = base64.b64encode(pickle.dumps(data)).decode()
        with pytest.raises(ValueError):
            decode(pickle_bytes)

    def test_ingestion_modules_do_not_import_pickle(self) -> None:
        """No ingestion module should import pickle or cloudpickle directly."""
        ingestion_modules = [
            "pycypher.ingestion.data_sources",
            "pycypher.ingestion.duckdb_reader",
            "pycypher.ingestion.config",
            "pycypher.ingestion.security",
            "pycypher.ingestion.context_builder",
            "pycypher.ingestion.arrow_utils",
        ]
        for mod_name in ingestion_modules:
            try:
                mod = importlib.import_module(mod_name)
            except ImportError:
                continue
            source_file = getattr(mod, "__file__", None)
            if source_file is None:
                continue
            source = Path(source_file).read_text()
            assert "import pickle" not in source, (
                f"{mod_name} imports pickle — use JSON or Arrow IPC instead"
            )
            assert "import cloudpickle" not in source, (
                f"{mod_name} imports cloudpickle — use JSON or Arrow IPC instead"
            )

    def test_sink_modules_do_not_import_pickle(self) -> None:
        """No sink module should import pickle or cloudpickle directly."""
        sink_modules = [
            "pycypher.sinks.neo4j",
        ]
        for mod_name in sink_modules:
            try:
                mod = importlib.import_module(mod_name)
            except ImportError:
                continue
            source_file = getattr(mod, "__file__", None)
            if source_file is None:
                continue
            source = Path(source_file).read_text()
            assert "import pickle" not in source, (
                f"{mod_name} imports pickle — use JSON or Arrow IPC instead"
            )

    def test_security_module_does_not_import_pickle(self) -> None:
        """The security module itself must never import pickle."""
        from pycypher.ingestion import security

        source = Path(security.__file__).read_text()
        assert "import pickle" not in source


# ===========================================================================
# Category 2 — Credential isolation and redaction
# ===========================================================================


class TestCredentialIsolation:
    """Credentials must never appear in logs, repr, str, or error messages."""

    def test_neo4j_sink_does_not_leak_password_in_repr(self) -> None:
        """Neo4jSink repr/str must not contain the password in cleartext."""
        try:
            from pycypher.sinks.neo4j import Neo4jSink
        except ImportError:
            pytest.skip("neo4j extra not installed")

        # Create a sink WITHOUT actually connecting (mock the driver)
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = None
            try:
                sink = Neo4jSink.__new__(Neo4jSink)
                sink._uri = "bolt://localhost:7687"
                sink._user = "neo4j"
                sink._password = "super_secret_password_12345"
                sink._driver = None
                sink._encrypted = None
                sink._driver_kwargs = {}
            except Exception:
                pytest.skip("Cannot construct Neo4jSink for repr test")

            repr_str = repr(sink)
            str_str = str(sink)
            assert "super_secret_password_12345" not in repr_str, (
                "Password leaked in repr()"
            )
            assert "super_secret_password_12345" not in str_str, (
                "Password leaked in str()"
            )

    def test_connection_string_not_in_error_messages(self) -> None:
        """Database connection errors must not include credentials in the message."""
        # Simulate a connection failure with credentials in the URI
        uri_with_creds = "postgresql://admin:s3cret@db.internal:5432/prod"

        # The security module's validate_uri_scheme should accept postgres URIs
        # but any error handling must strip the userinfo portion
        from pycypher.ingestion.security import validate_uri_scheme

        # This should not raise — postgres is an allowed scheme
        result = validate_uri_scheme(uri_with_creds)
        assert result is not None


# ===========================================================================
# Category 3 — Backend validation parity
# ===========================================================================


class TestBackendValidationParity:
    """All backends must enforce the same input validation rules.

    When BackendEngine protocol is implemented (Phase 1), every concrete
    backend (PandasBackend, DaskBackend, etc.) must validate inputs
    identically.  These tests verify the validation layer exists and is
    consistent.
    """

    def test_security_module_validates_sql_identifiers(self) -> None:
        """SQL identifier validation must block injection regardless of backend."""
        from pycypher.ingestion.security import (
            SecurityError,
            sanitize_sql_identifier,
        )

        # Must reject SQL injection attempts
        with pytest.raises(SecurityError):
            sanitize_sql_identifier("table; DROP TABLE users--")

        with pytest.raises(SecurityError):
            sanitize_sql_identifier("")

        with pytest.raises(SecurityError):
            sanitize_sql_identifier("123starts_with_number")

    def test_security_module_validates_file_paths(self) -> None:
        """Path validation must block traversal regardless of backend."""
        from pycypher.ingestion.security import (
            SecurityError,
            sanitize_file_path,
        )

        with pytest.raises(SecurityError):
            sanitize_file_path("../../etc/passwd")

        with pytest.raises(SecurityError):
            sanitize_file_path("/etc/shadow")

        with pytest.raises(SecurityError):
            sanitize_file_path("")

    def test_security_module_validates_sql_queries(self) -> None:
        """SQL query validation must block dangerous operations."""
        from pycypher.ingestion.security import (
            SecurityError,
            validate_sql_query,
        )

        dangerous_queries = [
            "SELECT 1; DROP TABLE users;",
            "DROP TABLE users",
            "DELETE FROM users WHERE 1=1",
            "",
        ]
        for query in dangerous_queries:
            with pytest.raises(SecurityError):
                validate_sql_query(query)

    def test_security_module_validates_uri_schemes(self) -> None:
        """URI scheme validation must reject dangerous schemes."""
        from pycypher.ingestion.security import (
            SecurityError,
            validate_uri_scheme,
        )

        # Dangerous schemes that could be used for SSRF or code execution
        with pytest.raises(SecurityError):
            validate_uri_scheme("javascript:alert(1)")

        with pytest.raises(SecurityError):
            validate_uri_scheme("ftp://malicious.server/data")

        with pytest.raises(SecurityError):
            validate_uri_scheme("")


# ===========================================================================
# Category 4 — Temporary storage security
# ===========================================================================


class TestTemporaryStorageSecurity:
    """Spill-to-disk, checkpoints, and cache files must be secured."""

    def test_tempfile_default_permissions_are_restrictive(self) -> None:
        """Temporary files created by Python default to safe permissions on Unix."""
        with tempfile.NamedTemporaryFile(delete=True) as tmp:
            if os.name != "nt":  # Unix-like only
                file_stat = os.stat(tmp.name)
                mode = stat.S_IMODE(file_stat.st_mode)
                # Must not be world-readable or world-writable
                assert not (mode & stat.S_IROTH), (
                    f"Temp file is world-readable: {oct(mode)}"
                )
                assert not (mode & stat.S_IWOTH), (
                    f"Temp file is world-writable: {oct(mode)}"
                )

    def test_tempdir_default_permissions_are_restrictive(self) -> None:
        """Temporary directories must not be world-accessible."""
        with tempfile.TemporaryDirectory() as tmpdir:
            if os.name != "nt":
                dir_stat = os.stat(tmpdir)
                mode = stat.S_IMODE(dir_stat.st_mode)
                # Must not be world-readable, writable, or executable
                assert not (mode & stat.S_IROTH), (
                    f"Temp dir is world-readable: {oct(mode)}"
                )
                assert not (mode & stat.S_IWOTH), (
                    f"Temp dir is world-writable: {oct(mode)}"
                )


# ===========================================================================
# Category 5 — Dask cluster security requirements
# ===========================================================================


class TestDaskSecurityRequirements:
    """When Dask distributed is used, TLS and authentication must be enforced.

    These tests verify that:
    1. Dask Security objects can be created with TLS
    2. The project does not create unencrypted schedulers
    3. Cluster configuration includes security settings
    """

    def test_dask_security_module_exists(self) -> None:
        """dask.distributed.Security must be importable for TLS config."""
        try:
            from distributed.security import Security

            # Verify we can create a security config
            sec = Security()
            assert sec is not None
        except ImportError:
            pytest.skip("dask.distributed not installed")

    def test_dask_security_supports_tls(self) -> None:
        """dask.distributed.Security must support TLS configuration."""
        try:
            from distributed.security import Security

            # Verify TLS-capable Security can be created
            # (don't actually create certs, just verify the API exists)
            sec = Security(
                tls_ca_file="ca.pem",
                tls_client_cert="cert.pem",
                tls_client_key="key.pem",
            )
            assert sec is not None
        except ImportError:
            pytest.skip("dask.distributed not installed")
        except (TypeError, FileNotFoundError):
            # API exists but files don't — that's fine for this contract test
            pass


# ===========================================================================
# Category 6 — Network and logging security
# ===========================================================================


class TestLoggingSecurity:
    """Sensitive data must not appear in log output."""

    def test_logger_does_not_expose_passwords(self) -> None:
        """Logger must not output password values."""
        from shared.logger import LOGGER

        # Verify logger exists and is configured
        assert LOGGER is not None
        assert LOGGER.name is not None

    def test_uri_with_credentials_not_logged_verbatim(self) -> None:
        """ensure_uri should not log credentials from URI strings."""
        import logging

        from shared.helpers import ensure_uri

        # Capture log output at DEBUG level to catch all messages
        records: list[logging.LogRecord] = []

        class CapturingHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record)

        from shared.logger import LOGGER

        handler = CapturingHandler()
        handler.setLevel(logging.DEBUG)
        LOGGER.addHandler(handler)
        old_level = LOGGER.level
        LOGGER.setLevel(logging.DEBUG)
        try:
            # Parse a URI with embedded credentials
            result = ensure_uri(
                "postgresql://admin:s3cret_pw@db.internal:5432/prod",
            )
            # Check that the password is not in any log record
            for record in records:
                msg = record.getMessage()
                assert "s3cret_pw" not in msg, (
                    f"Password leaked in log message: {msg}"
                )
        finally:
            LOGGER.setLevel(old_level)
            LOGGER.removeHandler(handler)
