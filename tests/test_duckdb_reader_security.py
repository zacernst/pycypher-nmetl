"""TDD security tests for DuckDBReader SQL injection prevention (Loop 173).

Problem: ``DuckDBReader.from_csv``, ``from_parquet``, and ``from_json`` each
interpolate a user-supplied ``path`` directly into a DuckDB SQL string literal
without any validation:

    con.execute(f"CREATE VIEW source AS SELECT * FROM read_csv_auto('{path}')")

A path containing a single quote closes the SQL string early and allows the
caller to append arbitrary DuckDB SQL.  A NUL byte terminates C-level string
processing in some SQL engines.

The ``_validate_sql_string_literal`` helper was added in Loop 167 and is
already applied consistently in ``data_sources.py`` (``CsvFormat.view_sql``,
``ParquetFormat.view_sql``, ``JsonFormat.view_sql``).  ``duckdb_reader.py``
was not updated in the same pass — it is the only remaining call site that
embeds user-supplied paths into SQL without the guard.

Fix: call ``_validate_sql_string_literal(path, "path")`` at the top of each
affected method, before the ``import duckdb`` line, so the validation fires
immediately and DuckDB is never invoked with malicious input.

All tests are written before the fix (TDD red phase).
"""

from __future__ import annotations

import pytest
from pycypher.ingestion.duckdb_reader import DuckDBReader

# ---------------------------------------------------------------------------
# Shared injection payload constants
# ---------------------------------------------------------------------------

# Payloads that contain a single quote — the primary injection vector.
_QUOTE_PAYLOADS: list[str] = [
    "my'file.csv",
    "data/has'quote.parquet",
    "data.csv'); SELECT * FROM duckdb_tables(); --",
    "data.csv'); COPY (SELECT 42) TO '/tmp/leaked.txt'; --",
    "it's_a_trap.json",
]

# Payloads that contain a NUL byte — terminates C-level string processing.
_NUL_PAYLOADS: list[str] = [
    "file\x00.csv",
    "path/to\x00/data.parquet",
    "\x00.json",
]


# ---------------------------------------------------------------------------
# Category 1 — from_csv path validation
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.slow


class TestCsvPathValidation:
    """from_csv must reject paths that would break the SQL string literal."""

    @pytest.mark.parametrize("path", _QUOTE_PAYLOADS)
    def test_single_quote_in_path_raises_valueerror(self, path: str) -> None:
        """A path containing ' must raise ValueError before touching DuckDB."""
        with pytest.raises(ValueError, match="single quote"):
            DuckDBReader.from_csv(path)

    @pytest.mark.parametrize("path", _NUL_PAYLOADS)
    def test_nul_byte_in_path_raises_valueerror(self, path: str) -> None:
        """A path containing \\x00 must raise ValueError before touching DuckDB."""
        with pytest.raises(ValueError, match="NUL"):
            DuckDBReader.from_csv(path)

    def test_valid_path_raises_non_validation_error(self) -> None:
        """A path without injection characters passes validation; DuckDB raises
        its own error (missing file), not a ValueError from our guard.
        """
        with pytest.raises(Exception) as exc_info:
            DuckDBReader.from_csv("/nonexistent/definitely_missing.csv")
        assert not isinstance(exc_info.value, ValueError), (
            "Expected a non-ValueError (DuckDB file-not-found), "
            f"but got ValueError: {exc_info.value}"
        )

    def test_validation_fires_before_duckdb_import(self) -> None:
        """Validation must raise before any DuckDB connection is opened.

        We verify this by passing an injected path and confirming the error
        message comes from _validate_sql_string_literal (contains 'single quote'
        or 'NUL'), not from DuckDB itself.
        """
        with pytest.raises(ValueError) as exc_info:
            DuckDBReader.from_csv("inject'); DROP TABLE nodes; --")
        msg = str(exc_info.value)
        assert "single quote" in msg, (
            f"Expected validation error mentioning 'single quote', got: {msg!r}"
        )


# ---------------------------------------------------------------------------
# Category 2 — from_parquet path validation
# ---------------------------------------------------------------------------


class TestParquetPathValidation:
    """from_parquet must reject paths that would break the SQL string literal."""

    @pytest.mark.parametrize("path", _QUOTE_PAYLOADS)
    def test_single_quote_in_path_raises_valueerror(self, path: str) -> None:
        with pytest.raises(ValueError, match="single quote"):
            DuckDBReader.from_parquet(path)

    @pytest.mark.parametrize("path", _NUL_PAYLOADS)
    def test_nul_byte_in_path_raises_valueerror(self, path: str) -> None:
        with pytest.raises(ValueError, match="NUL"):
            DuckDBReader.from_parquet(path)

    def test_valid_path_raises_non_validation_error(self) -> None:
        with pytest.raises(Exception) as exc_info:
            DuckDBReader.from_parquet(
                "/nonexistent/definitely_missing.parquet",
            )
        assert not isinstance(exc_info.value, ValueError), (
            f"Expected a non-ValueError (file-not-found), got ValueError: {exc_info.value}"
        )


# ---------------------------------------------------------------------------
# Category 3 — from_json path validation
# ---------------------------------------------------------------------------


class TestJsonPathValidation:
    """from_json must reject paths that would break the SQL string literal."""

    @pytest.mark.parametrize("path", _QUOTE_PAYLOADS)
    def test_single_quote_in_path_raises_valueerror(self, path: str) -> None:
        with pytest.raises(ValueError, match="single quote"):
            DuckDBReader.from_json(path)

    @pytest.mark.parametrize("path", _NUL_PAYLOADS)
    def test_nul_byte_in_path_raises_valueerror(self, path: str) -> None:
        with pytest.raises(ValueError, match="NUL"):
            DuckDBReader.from_json(path)

    def test_valid_path_raises_non_validation_error(self) -> None:
        with pytest.raises(Exception) as exc_info:
            DuckDBReader.from_json("/nonexistent/definitely_missing.json")
        assert not isinstance(exc_info.value, ValueError), (
            f"Expected a non-ValueError (file-not-found), got ValueError: {exc_info.value}"
        )


# ---------------------------------------------------------------------------
# Category 4 — Methods that do NOT use string interpolation are unaffected
# ---------------------------------------------------------------------------


class TestUnaffectedMethods:
    """from_dataframe, from_arrow, and from_sql do not interpolate path into
    SQL string literals, so they have no injection surface to guard.
    """

    def test_from_sql_accepts_connection_string_with_quote(self) -> None:
        """from_sql passes connection_string to duckdb.connect() — not embedded
        in a SQL literal — so it should not be rejected by our path guard.
        We expect a DuckDB connection error, not a ValueError.
        """
        with pytest.raises(Exception) as exc_info:
            # This will fail at DuckDB level (bad connection), not at our guard
            DuckDBReader.from_sql(
                "invalid://connection'string",
                query="SELECT 1",
            )
        # Should NOT be our validation ValueError
        assert not isinstance(exc_info.value, ValueError), (
            f"from_sql should not apply path validation to connection_string, "
            f"but got ValueError: {exc_info.value}"
        )
