"""TDD tests validating that all documentation examples are executable.

These tests ensure documentation code examples remain accurate and functional.
Each test corresponds to a specific documentation section and validates that
the example code produces the documented behavior.
"""

import pytest

# ---------------------------------------------------------------------------
# Backend Delegation Guide Examples
# ---------------------------------------------------------------------------


class TestBackendDelegationExamples:
    """Validate examples from the backend delegation guide."""

    def test_backend_engine_protocol_exists(self):
        """Backend guide: BackendEngine protocol is importable."""
        from pycypher.backend_engine import BackendEngine

        assert BackendEngine is not None

    def test_pandas_backend_importable(self):
        """Backend guide: default Pandas backend."""
        from pycypher.backend_engine import PandasBackend

        backend = PandasBackend()
        assert backend is not None

    def test_duckdb_backend_importable(self):
        """Backend guide: DuckDB backend for analytical workloads."""
        from pycypher.backends.duckdb_backend import DuckDBBackend

        backend = DuckDBBackend()
        assert backend is not None

    def test_backend_protocol_methods(self):
        """Backend guide: protocol defines expected operations."""
        from pycypher.backend_engine import BackendEngine

        # Verify key protocol methods exist
        expected_methods = [
            "scan_entity",
            "join",
            "filter",
            "rename",
            "concat",
            "distinct",
            "assign_column",
            "drop_columns",
            "aggregate",
            "sort",
            "limit",
            "skip",
            "to_pandas",
            "row_count",
            "is_empty",
            "memory_estimate_bytes",
        ]

        for method_name in expected_methods:
            assert hasattr(BackendEngine, method_name), (
                f"BackendEngine missing {method_name}"
            )
