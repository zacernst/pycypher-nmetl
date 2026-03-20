"""DuckDB-backed readers that return Arrow tables.

Every method opens a fresh in-process DuckDB connection, loads the source,
and returns the result as a ``pa.Table`` via ``duckdb_relation.to_arrow_table()``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pycypher.ingestion.data_sources import _validate_sql_string_literal
from pycypher.ingestion.security import (
    SecurityError,
    escape_sql_string_literal,
    sanitize_file_path,
    validate_sql_query,
    validate_uri_scheme,
)

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa


class DuckDBReader:
    """Universal ingestion adapter that normalises any data source to Arrow.

    All methods are static.  Each method opens a fresh in-process DuckDB
    connection for the duration of the read and closes it via the context
    manager protocol on return or exception.
    """

    @staticmethod
    def from_csv(
        path: str,
        id_col: str | None = None,
        query: str | None = None,
    ) -> pa.Table:
        """Read a CSV file into an Arrow table.

        Args:
            path: Path to the CSV file.
            id_col: Unused by this method — pass to
                :func:`~pycypher.ingestion.arrow_utils.normalize_entity_table`
                after reading.
            query: Optional SQL query to execute against the loaded CSV.
                When *None*, ``SELECT * FROM source`` is used.

        Returns:
            Arrow table with the CSV contents.

        Raises:
            SecurityError: If path or query contains dangerous content.

        """
        # Security validation - let ValueError from path validation propagate directly
        _validate_sql_string_literal(path, "path")

        try:
            sanitize_file_path(path)

            if query:
                validate_sql_query(query)

        except SecurityError as e:
            msg = f"Security validation failed for CSV read: {e}"
            raise SecurityError(msg) from e

        import duckdb

        with duckdb.connect() as con:
            # SECURITY: Use parameterized query construction with proper escaping
            # escape_sql_string_literal provides comprehensive SQL injection protection
            escaped_path = escape_sql_string_literal(path)
            con.execute(
                f"CREATE VIEW source AS SELECT * FROM read_csv_auto({escaped_path})",  # nosec B608 — path escaped by escape_sql_string_literal
            )

            sql = query or "SELECT * FROM source"
            return con.execute(sql).to_arrow_table()

    @staticmethod
    def from_parquet(
        path: str,
        id_col: str | None = None,
        query: str | None = None,
    ) -> pa.Table:
        """Read a Parquet file into an Arrow table.

        Args:
            path: Path to the Parquet file.
            id_col: Unused here; pass to normalise utilities afterward.
            query: Optional SQL query against the loaded Parquet.

        Returns:
            Arrow table with the Parquet contents.

        Raises:
            SecurityError: If path or query contains dangerous content.

        """
        # Security validation - let ValueError from path validation propagate directly
        _validate_sql_string_literal(path, "path")

        try:
            sanitize_file_path(path)

            if query:
                validate_sql_query(query)

        except SecurityError as e:
            msg = f"Security validation failed for Parquet read: {e}"
            raise SecurityError(msg) from e

        import duckdb

        with duckdb.connect() as con:
            # SECURITY: Use parameterized query construction with proper escaping
            # escape_sql_string_literal provides comprehensive SQL injection protection
            escaped_path = escape_sql_string_literal(path)
            con.execute(
                f"CREATE VIEW source AS SELECT * FROM read_parquet({escaped_path})",  # nosec B608 — path escaped by escape_sql_string_literal
            )

            sql = query or "SELECT * FROM source"
            return con.execute(sql).to_arrow_table()

    @staticmethod
    def from_json(
        path: str,
        id_col: str | None = None,
        query: str | None = None,
    ) -> pa.Table:
        """Read a JSON file into an Arrow table.

        Args:
            path: Path to the JSON file.
            id_col: Unused here; pass to normalise utilities afterward.
            query: Optional SQL query against the loaded JSON.

        Returns:
            Arrow table with the JSON contents.

        Raises:
            SecurityError: If path or query contains dangerous content.

        """
        # Security validation - let ValueError from path validation propagate directly
        _validate_sql_string_literal(path, "path")

        try:
            sanitize_file_path(path)

            if query:
                validate_sql_query(query)

        except SecurityError as e:
            msg = f"Security validation failed for JSON read: {e}"
            raise SecurityError(msg) from e

        import duckdb

        with duckdb.connect() as con:
            # SECURITY: Use parameterized query construction with proper escaping
            # escape_sql_string_literal provides comprehensive SQL injection protection
            escaped_path = escape_sql_string_literal(path)
            con.execute(
                f"CREATE VIEW source AS SELECT * FROM read_json_auto({escaped_path})",  # nosec B608 — path escaped by escape_sql_string_literal
            )

            sql = query or "SELECT * FROM source"
            return con.execute(sql).to_arrow_table()

    @staticmethod
    def from_sql(
        connection_string: str,
        query: str,
    ) -> pa.Table:
        """Execute *query* against an external database and return Arrow.

        Args:
            connection_string: DuckDB-compatible connection string (e.g.
                ``"postgresql://user:pass@host/db"``).
            query: SQL query to execute.

        Returns:
            Arrow table with the query results.

        Raises:
            SecurityError: If connection string or query contains dangerous content.

        """
        # Security validation
        try:
            validate_uri_scheme(connection_string)
            validate_sql_query(query)

        except (SecurityError, ValueError) as e:
            msg = f"Security validation failed for SQL execution: {e}"
            raise SecurityError(msg) from e

        import duckdb

        with duckdb.connect(connection_string) as con:
            return con.execute(query).to_arrow_table()

    @staticmethod
    def from_dataframe(
        df: pd.DataFrame,
        id_col: str | None = None,
    ) -> pa.Table:
        """Convert a pandas DataFrame to an Arrow table via DuckDB.

        Args:
            df: Source pandas DataFrame.
            id_col: Unused here; pass to normalise utilities afterward.

        Returns:
            Arrow table with the DataFrame contents.

        """
        import duckdb

        with duckdb.connect() as con:
            # Register the DataFrame with DuckDB so it can be queried
            con.register("df", df)
            return con.execute("SELECT * FROM df").to_arrow_table()

    @staticmethod
    def from_arrow(
        table: pa.Table,
    ) -> pa.Table:
        """Passthrough / re-normalise an Arrow table via DuckDB.

        Useful for applying a custom SQL transformation to an existing Arrow
        table before ingestion.

        Args:
            table: Source Arrow table.

        Returns:
            Arrow table (may be a copy after round-tripping through DuckDB).

        """
        import duckdb

        with duckdb.connect() as con:
            # DuckDB can query Arrow tables directly; avoid the reserved word "table"
            # by registering the table under a view name.
            con.register("arrow_source", table)
            return con.execute("SELECT * FROM arrow_source").to_arrow_table()
