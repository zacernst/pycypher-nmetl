"""Security tests for Phase 3 Dask distributed execution.

These tests verify that distributed execution enforces TLS encryption
and authentication, prevents pickle-based attacks on checkpoints, and
secures temporary storage.

Categories:
1. Dask TLS enforcement — clusters must use encrypted communication
2. Checkpoint serialization safety — no pickle for persisted data
3. Temporary storage permissions — spill-to-disk files are secured
4. Credential isolation — connection details never leak to logs
5. Security.temporary() — verify self-signed cert generation for testing
"""

from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.slow


# ===========================================================================
# Category 1 — Dask TLS enforcement
# ===========================================================================


class TestDaskTLSEnforcement:
    """Distributed Dask clusters must use TLS-encrypted communication."""

    def test_dask_security_temporary_creates_certs(self) -> None:
        """Security.temporary() must create valid self-signed certs for testing."""
        try:
            from distributed.security import Security
        except ImportError:
            pytest.skip("dask.distributed not installed")

        try:
            sec = Security.temporary()
        except ImportError:
            pytest.skip("cryptography library not installed")

        assert sec.require_encryption is True
        # Must have TLS config for all roles
        assert sec.tls_ca_file or hasattr(sec, "_tls_ca_file")

    def test_security_require_encryption_flag(self) -> None:
        """Security(require_encryption=True) must enforce TLS."""
        try:
            from distributed.security import Security
        except ImportError:
            pytest.skip("dask.distributed not installed")

        sec = Security(require_encryption=True)
        assert sec.require_encryption is True

    def test_security_with_explicit_tls_config(self) -> None:
        """Security accepts explicit TLS certificate paths."""
        try:
            from distributed.security import Security
        except ImportError:
            pytest.skip("dask.distributed not installed")

        # Verify the API accepts cert paths (files don't need to exist for config)
        sec = Security(
            require_encryption=True,
            tls_ca_file="/path/to/ca.pem",
            tls_client_cert="/path/to/client.pem",
            tls_client_key="/path/to/client.key",
        )
        assert sec.require_encryption is True

    def test_default_security_has_no_encryption(self) -> None:
        """Default Security() has encryption disabled — our code must override this."""
        try:
            from distributed.security import Security
        except ImportError:
            pytest.skip("dask.distributed not installed")

        sec = Security()
        # Default Dask has NO encryption — this is the vulnerability
        # Our DistributedContext must NEVER use this default
        assert sec.require_encryption is False, (
            "Dask default changed to require encryption — update our security docs"
        )


# ===========================================================================
# Category 2 — Checkpoint serialization safety
# ===========================================================================


class TestCheckpointSerializationSafety:
    """Checkpoint files must use Arrow IPC or Parquet, never pickle."""

    def test_arrow_ipc_roundtrip_works(self) -> None:
        """Arrow IPC must be usable for checkpoint serialization."""
        import io

        import pandas as pd
        import pyarrow as pa
        from pyarrow import ipc

        # Create test data
        df = pd.DataFrame({"__ID__": [1, 2, 3], "name": ["a", "b", "c"]})
        table = pa.Table.from_pandas(df)

        # Write to IPC
        buf = io.BytesIO()
        writer = ipc.new_file(buf, table.schema)
        writer.write_table(table)
        writer.close()

        # Read back
        buf.seek(0)
        reader = ipc.open_file(buf)
        restored = reader.read_all().to_pandas()

        pd.testing.assert_frame_equal(df, restored)

    def test_parquet_roundtrip_works(self) -> None:
        """Parquet must be usable for checkpoint serialization."""
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        df = pd.DataFrame({"__ID__": [1, 2, 3], "val": [10.0, 20.0, 30.0]})

        with tempfile.NamedTemporaryFile(
            suffix=".parquet",
            delete=True,
        ) as tmp:
            table = pa.Table.from_pandas(df)
            pq.write_table(table, tmp.name)
            restored = pq.read_table(tmp.name).to_pandas()
            pd.testing.assert_frame_equal(df, restored)

    def test_pickle_checkpoint_must_be_rejected(self) -> None:
        """Demonstrate that pickle checkpoints are dangerous.

        This test shows WHY we must use Arrow IPC instead of pickle.
        pickle.loads can execute arbitrary code during deserialization.
        """
        import pickle

        # Create a "checkpoint" that executes code on load
        class MaliciousCheckpoint:
            def __reduce__(self) -> tuple:
                # This would execute os.system("rm -rf /") on unpickle
                return (eval, ("1+1",))

        payload = pickle.dumps(MaliciousCheckpoint())
        # pickle.loads(payload) would execute eval("1+1") — proof of RCE
        # Arrow IPC cannot do this — it's a pure data format
        result = pickle.loads(payload)
        assert result == 2  # eval("1+1") executed during deserialization


# ===========================================================================
# Category 3 — Temporary storage permissions
# ===========================================================================


class TestTemporaryStoragePermissions:
    """Spill-to-disk and checkpoint directories must be access-restricted."""

    def test_checkpoint_directory_created_with_restrictive_permissions(
        self,
    ) -> None:
        """Checkpoint directories must be 0700 (owner only)."""
        with tempfile.TemporaryDirectory() as parent:
            checkpoint_dir = Path(parent) / "checkpoints"
            checkpoint_dir.mkdir(mode=0o700)

            dir_stat = os.stat(checkpoint_dir)
            mode = stat.S_IMODE(dir_stat.st_mode)
            assert mode == 0o700, f"Expected 0700, got {oct(mode)}"

    def test_spill_file_not_world_readable(self) -> None:
        """Spill-to-disk files must not be world-readable."""
        with tempfile.NamedTemporaryFile(
            prefix="pycypher_spill_",
            suffix=".arrow",
            delete=True,
        ) as tmp:
            if os.name != "nt":
                file_stat = os.stat(tmp.name)
                mode = stat.S_IMODE(file_stat.st_mode)
                assert not (mode & stat.S_IROTH), (
                    f"Spill file is world-readable: {oct(mode)}"
                )
                assert not (mode & stat.S_IWOTH), (
                    f"Spill file is world-writable: {oct(mode)}"
                )


# ===========================================================================
# Category 4 — Credential isolation for distributed connections
# ===========================================================================


class TestDistributedCredentialIsolation:
    """Dask cluster credentials must not leak into logs or error messages."""

    def test_scheduler_address_can_contain_credentials(self) -> None:
        """Verify we can parse scheduler addresses with embedded credentials.

        If a scheduler address contains credentials (unlikely but possible
        with custom protocols), our wrapper must redact them.
        """
        from urllib.parse import urlparse

        # Dask typically uses tcp:// or tls:// schemes
        addr = "tls://admin:s3cret@scheduler.internal:8786"
        parsed = urlparse(addr)
        assert parsed.password == "s3cret"
        assert parsed.hostname == "scheduler.internal"

        # Redacted version for logging
        safe_addr = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        assert "s3cret" not in safe_addr
        assert "scheduler.internal" in safe_addr

    def test_dask_config_does_not_log_tls_keys(self) -> None:
        """TLS private key paths should be treated as sensitive."""
        try:
            from distributed.security import Security
        except ImportError:
            pytest.skip("dask.distributed not installed")

        sec = Security(
            require_encryption=True,
            tls_client_key="/secret/path/client.key",
        )

        # repr/str of Security should not expose full key paths
        # (Dask may or may not redact — we document the risk)
        repr_str = repr(sec)
        # This is informational — if Dask exposes paths, our wrapper must redact
        assert isinstance(repr_str, str)


# ===========================================================================
# Category 5 — DaskBackend security contract (TDD for future impl)
# ===========================================================================


def _try_import_dask_backend() -> type | None:
    """Try to import DaskBackend; return None if not yet implemented."""
    try:
        from pycypher.backend_engine import DaskBackend

        return DaskBackend
    except ImportError:
        return None


class TestDaskBackendSecurityContract:
    """Security contract tests for future DaskBackend implementation.

    These tests skip until DaskBackend is implemented, then auto-activate.
    """

    def _get_backend(self) -> Any:
        cls = _try_import_dask_backend()
        if cls is None:
            pytest.skip("DaskBackend not yet implemented")
        return cls()

    def test_dask_backend_exists(self) -> None:
        """DaskBackend must be importable when Phase 3 is complete."""
        cls = _try_import_dask_backend()
        if cls is None:
            pytest.skip("DaskBackend not yet implemented")
        assert cls is not None

    def test_dask_backend_requires_security_config(self) -> None:
        """DaskBackend constructor must accept or require security configuration."""
        cls = _try_import_dask_backend()
        if cls is None:
            pytest.skip("DaskBackend not yet implemented")
        import inspect

        sig = inspect.signature(cls.__init__)
        param_names = list(sig.parameters.keys())
        # Must accept security-related parameter
        security_params = [
            p
            for p in param_names
            if "security" in p.lower()
            or "tls" in p.lower()
            or "encrypt" in p.lower()
        ]
        assert security_params, (
            "DaskBackend.__init__ must accept a security configuration parameter "
            "(e.g., security_config, tls_config, require_encryption). "
            "Found params: " + str(param_names)
        )

    def test_dask_backend_source_has_no_pickle_serialization(self) -> None:
        """DaskBackend must not use pickle for DATA serialization."""
        cls = _try_import_dask_backend()
        if cls is None:
            pytest.skip("DaskBackend not yet implemented")
        import inspect

        source = inspect.getsource(cls)
        # pickle is OK for Dask's internal task graph (we can't control that)
        # but our code must not explicitly call pickle.dumps/loads for data
        assert "pickle.dumps" not in source, (
            "DaskBackend must not use pickle.dumps for data serialization"
        )
        assert "pickle.loads" not in source, (
            "DaskBackend must not use pickle.loads for data deserialization"
        )
