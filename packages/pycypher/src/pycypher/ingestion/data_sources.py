"""Abstract DataSource hierarchy and URI-based factory.

:class:`DataSource` is the primary abstraction for reading tabular data into
Arrow tables.  Every concrete subclass wraps one kind of external storage and
exposes a single :meth:`~DataSource.read` method that returns a ``pa.Table``.

Use :func:`data_source_from_uri` to obtain the correct subclass for a given
URI string without writing dispatch code yourself.

Supported URI schemes
---------------------
+----------------------------------------+-----------------------------------+
| URI / value                            | Resolved class                    |
+========================================+===================================+
| ``file:///path/file.csv``              | :class:`FileDataSource`           |
| ``/path/file.csv`` (bare path)         | :class:`FileDataSource`           |
| ``s3://bucket/file.csv``               | :class:`FileDataSource`           |
| ``https://host/file.csv``              | :class:`FileDataSource`           |
+----------------------------------------+-----------------------------------+
| Same variations with ``.parquet``      | :class:`FileDataSource`           |
+----------------------------------------+-----------------------------------+
| Same variations with ``.json``         | :class:`FileDataSource`           |
+----------------------------------------+-----------------------------------+
| ``postgresql://…``, ``mysql://…``      | :class:`SqlDataSource`            |
| ``sqlite://…``, ``duckdb://…``         | :class:`SqlDataSource`            |
+----------------------------------------+-----------------------------------+
| ``pd.DataFrame``                       | :class:`DataFrameDataSource`      |
+----------------------------------------+-----------------------------------+
| ``pa.Table``                           | :class:`ArrowDataSource`          |
+----------------------------------------+-----------------------------------+
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa

from pycypher.ingestion.security import (
    SecurityError,
    escape_sql_string_literal,
    sanitize_file_path,
    validate_sql_query,
    validate_uri_scheme,
)

#: URI schemes that indicate a relational / SQL source.
#: Imported by ``config.py`` for URI validation — single source of truth.
_SQL_SCHEMES: frozenset[str] = frozenset(
    {"postgresql", "postgres", "mysql", "sqlite", "duckdb"},
)

#: File-extension suffixes that ``data_source_from_uri`` dispatches on.
#: Imported by ``config.py`` for URI validation — single source of truth.
_SUPPORTED_EXTENSIONS: frozenset[str] = frozenset(
    {".csv", ".parquet", ".json"},
)


def _validate_sql_string_literal(value: str, field_name: str) -> None:
    """Validate that *value* can be safely embedded in a SQL single-quoted string.

    DuckDB format functions such as ``read_csv_auto``, ``read_parquet``, and
    ``read_json_auto`` accept their arguments as SQL string literals.  A value
    containing a single quote (``'``) would close the string early, allowing
    an attacker to append arbitrary SQL.  A NUL byte terminates C-level string
    processing in some SQL engines.

    This guard is applied to every user-controlled value that is interpolated
    into a SQL string literal by :meth:`Format.view_sql`.

    Args:
        value: The string to validate.
        field_name: Human-readable name of the field (for error messages).

    Raises:
        ValueError: If *value* contains a single quote or a NUL byte.

    """
    import urllib.parse

    # First check the raw value
    if "'" in value:
        msg = (
            f"{field_name!r} contains a single quote character (\\') which would "
            "break out of the SQL string literal and allow SQL injection. "
            f"Received: {value!r}"
        )
        raise ValueError(
            msg,
        )
    if "\x00" in value:
        msg = (
            f"{field_name!r} contains a NUL byte which is not permitted in SQL "
            f"string literals. Received: {value!r}"
        )
        raise ValueError(
            msg,
        )

    # Check for URL-encoded attacks
    try:
        # URL decode to catch encoded single quotes and other dangerous characters
        decoded_value = urllib.parse.unquote(value)
        if decoded_value != value:  # Something was decoded
            if "'" in decoded_value:
                msg = (
                    f"{field_name!r} contains URL-encoded single quote (%27) which would "
                    "break out of the SQL string literal and allow SQL injection. "
                    f"Received: {value!r} (decodes to: {decoded_value!r})"
                )
                raise ValueError(
                    msg,
                )
            if "\x00" in decoded_value:
                msg = (
                    f"{field_name!r} contains URL-encoded NUL byte which is not permitted "
                    f"in SQL string literals. Received: {value!r} (decodes to: {decoded_value!r})"
                )
                raise ValueError(
                    msg,
                )
    except (ValueError, UnicodeDecodeError):
        # If URL decoding fails for malformed input, continue with other checks.
        # urllib.parse.unquote raises ValueError for malformed percent-encoding
        # and UnicodeDecodeError for invalid byte sequences.
        pass

    # Check for Unicode normalization attacks (different quote characters)
    import unicodedata

    normalized_value = unicodedata.normalize("NFKC", value)
    if "'" in normalized_value and normalized_value != value:
        msg = (
            f"{field_name!r} contains Unicode characters that normalize to single quote "
            "which would break out of the SQL string literal and allow SQL injection. "
            f"Received: {value!r} (normalizes to: {normalized_value!r})"
        )
        raise ValueError(
            msg,
        )


def _uri_to_duckdb_path(uri: str) -> str:
    """Resolve a URI to the path string DuckDB expects.

    ``file:///abs/path`` → ``/abs/path``.
    Cloud URIs (``s3://``, ``gs://``, ``https://``, …) are returned unchanged
    because DuckDB handles them natively via its extension layer.
    Bare filesystem paths are returned unchanged.

    Args:
        uri: A URI string or bare filesystem path.

    Returns:
        A path or URI string suitable for passing to DuckDB read functions.

    """
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return parsed.path
    return uri


# ---------------------------------------------------------------------------
# Format strategy hierarchy
# ---------------------------------------------------------------------------


class Format(ABC):
    """Abstract base for file-format strategies.

    Each subclass knows how to generate the DuckDB SQL fragment that reads
    one specific file format (CSV, Parquet, JSON, …).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short lowercase name identifying the format (e.g. ``"csv"``)."""

    @abstractmethod
    def view_sql(self, path: str) -> str:
        """Return a DuckDB SQL read-function call for *path*.

        Args:
            path: Filesystem path or cloud URI to the file.

        Returns:
            A SQL fragment such as ``"read_csv_auto('/data/f.csv')"``.

        """


@dataclass
class CsvFormat(Format):
    """Strategy for reading CSV files via DuckDB's ``read_csv_auto``.

    Attributes:
        delimiter: Field delimiter character.  Defaults to ``','``.
        header: Whether the file has a header row.  Defaults to ``True``.
        null_padding: Pad missing trailing columns with ``NULL``.

    """

    delimiter: str = ","
    header: bool = True
    null_padding: bool = False

    @property
    def name(self) -> str:
        """Return the format identifier for CSV files.

        Returns:
            The string ``"csv"`` identifying this format type.

        """
        return "csv"

    def view_sql(self, path: str) -> str:
        """Generate a DuckDB SQL fragment to read a CSV file with custom options.

        Produces a ``read_csv_auto()`` call that incorporates this format's
        delimiter, header, and null_padding settings. The path is validated
        to prevent SQL injection attacks.

        Args:
            path: Filesystem path or URI to the CSV file to read.

        Returns:
            A SQL fragment like ``"read_csv_auto('/data/file.csv', delim=';')"``
            that can be embedded in DuckDB queries.

        Raises:
            ValueError: If path or delimiter contains characters unsafe for SQL
                string literals (single quotes or NUL bytes).

        """
        _validate_sql_string_literal(path, "path")
        _validate_sql_string_literal(self.delimiter, "delimiter")
        escaped_path = escape_sql_string_literal(path)
        opts: list[str] = []
        if self.delimiter != ",":
            escaped_delim = escape_sql_string_literal(self.delimiter)
            opts.append(f"delim={escaped_delim}")
        if not self.header:
            opts.append("header=false")
        if self.null_padding:
            opts.append("null_padding=true")
        args = f", {', '.join(opts)}" if opts else ""
        return f"read_csv_auto({escaped_path}{args})"


@dataclass
class ParquetFormat(Format):
    """Strategy for reading Parquet files via DuckDB's ``read_parquet``."""

    @property
    def name(self) -> str:
        """Return the format identifier for Parquet files.

        Returns:
            The string ``"parquet"`` identifying this format type.

        """
        return "parquet"

    def view_sql(self, path: str) -> str:
        """Generate a DuckDB SQL fragment to read a Parquet file.

        Produces a ``read_parquet()`` call for the given path. Parquet files
        are self-describing and require no additional format options. The path
        is validated to prevent SQL injection attacks.

        Args:
            path: Filesystem path or URI to the Parquet file to read.

        Returns:
            A SQL fragment like ``"read_parquet('/data/file.parquet')"`` that
            can be embedded in DuckDB queries.

        Raises:
            ValueError: If path contains characters unsafe for SQL string
                literals (single quotes or NUL bytes).

        """
        _validate_sql_string_literal(path, "path")
        escaped_path = escape_sql_string_literal(path)
        return f"read_parquet({escaped_path})"


@dataclass
class JsonFormat(Format):
    """Strategy for reading JSON files via DuckDB's ``read_json_auto``.

    Attributes:
        records: JSON record layout.  ``'auto'`` (default) lets DuckDB infer
            the layout; ``'newline_delimited'`` forces NDJSON parsing.

    """

    records: str = "auto"

    @property
    def name(self) -> str:
        """Return the format identifier for JSON files.

        Returns:
            The string ``"json"`` identifying this format type.

        """
        return "json"

    def view_sql(self, path: str) -> str:
        """Generate a DuckDB SQL fragment to read a JSON file with record layout options.

        Produces a ``read_json_auto()`` call that incorporates this format's
        records layout setting. When records is ``"auto"``, DuckDB infers the
        JSON structure. When set to ``"newline_delimited"``, forces NDJSON
        parsing. The path and records values are validated to prevent SQL
        injection attacks.

        Args:
            path: Filesystem path or URI to the JSON file to read.

        Returns:
            A SQL fragment like ``"read_json_auto('/data/file.json')"`` or
            ``"read_json_auto('/data/file.json', format='newline_delimited')"``
            that can be embedded in DuckDB queries.

        Raises:
            ValueError: If path or records contains characters unsafe for SQL
                string literals (single quotes or NUL bytes).

        """
        _validate_sql_string_literal(path, "path")
        _validate_sql_string_literal(self.records, "records")
        escaped_path = escape_sql_string_literal(path)
        if self.records != "auto":
            escaped_records = escape_sql_string_literal(self.records)
            return f"read_json_auto({escaped_path}, format={escaped_records})"
        return f"read_json_auto({escaped_path})"


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class DataSource(ABC):
    """Abstract base for all tabular data sources.

    Every concrete subclass implements :meth:`read` to load its backing store
    and return the result as a ``pa.Table``.  All downstream pipeline
    components (normalisation, query execution) consume Arrow tables produced
    by :meth:`read`.
    """

    @property
    @abstractmethod
    def uri(self) -> str:
        """Canonical identifier for this data source.

        Returns a URI string for file/SQL sources, or a descriptive placeholder
        (e.g. ``"<dataframe>"``) for in-memory sources.
        """

    @abstractmethod
    def read(self) -> pa.Table:
        """Load the source and return its contents as an Arrow table.

        Returns:
            A ``pa.Table`` containing the full (or filtered, when a *query*
            was supplied) contents of the source.

        """


# ---------------------------------------------------------------------------
# File-backed source (format-as-strategy)
# ---------------------------------------------------------------------------


class FileDataSource(DataSource):
    """File-backed data source that delegates format parsing to a :class:`Format`.

    The *format* object encapsulates the DuckDB read function and its options,
    so adding new formats (Avro, ORC, Delta …) requires only a new
    :class:`Format` subclass without touching this class.

    Args:
        uri: Path or URI to the file.  Bare filesystem paths, ``file://``,
            ``s3://``, ``gs://``, ``https://``, and ``abfss://`` URIs are
            all accepted; DuckDB resolves them at read time.
        format: A :class:`Format` instance that supplies the DuckDB SQL
            read fragment.
        query: Optional DuckDB SQL applied after the file is registered as
            the view ``source``.  When ``None``, ``SELECT * FROM source``
            is used.

    """

    def __init__(
        self,
        uri: str,
        format: Format,
        *,
        query: str | None = None,
    ) -> None:
        self._uri = uri
        self._format = format
        self._query = query

    @property
    def uri(self) -> str:
        """The original URI or file path used to construct this data source."""
        return self._uri

    @property
    def format(self) -> Format:
        """The :class:`Format` strategy used to read this file."""
        return self._format

    @property
    def query(self) -> str | None:
        """Optional SQL override applied when reading this source."""
        return self._query

    def read(self) -> pa.Table:
        """Read the file via DuckDB and return an Arrow table.

        Returns:
            ``pa.Table`` with the file contents (or query results).

        Raises:
            SecurityError: If URI or query contains dangerous content.

        """
        # Security validation
        try:
            validate_uri_scheme(self._uri)

            # Additional path validation for file URIs
            # Extract path from URI for additional validation
            path = _uri_to_duckdb_path(self._uri)
            sanitize_file_path(path)

            if self._query:
                validate_sql_query(self._query)

        except (SecurityError, ValueError) as e:
            msg = f"Security validation failed for file data source: {e}"
            raise SecurityError(msg) from e

        import duckdb

        path = _uri_to_duckdb_path(self._uri)
        with duckdb.connect() as con:
            # SECURITY: The view_sql method validates all inputs via _validate_sql_string_literal
            # which prevents SQL injection via quotes, NUL bytes, URL encoding, and Unicode normalization
            con.execute(
                f"CREATE VIEW source AS SELECT * FROM {self._format.view_sql(path)}",  # nosec B608 — path escaped by view_sql via escape_sql_string_literal
            )
            sql = self._query or "SELECT * FROM source"
            return con.execute(sql).to_arrow_table()


# ---------------------------------------------------------------------------
# SQL (database) source
# ---------------------------------------------------------------------------


class SqlDataSource(DataSource):
    """Reads from a relational database via a DuckDB connection string.

    DuckDB supports connecting to PostgreSQL, MySQL, SQLite, and other DuckDB
    files via its scanner extensions.  The ``query`` parameter is required
    because there is no meaningful default when targeting a database.
    """

    def __init__(self, uri: str, query: str) -> None:
        """Initialise a SQL source.

        Args:
            uri: DuckDB-compatible connection string (e.g.
                ``"postgresql://user:pass@host:5432/mydb"``).
            query: SQL query to execute against the database.  The result set
                is returned as an Arrow table.

        """
        self._uri = uri
        self._query = query

    @property
    def uri(self) -> str:
        """The database connection URI (e.g. ``postgresql://…``, ``sqlite://…``)."""
        return self._uri

    @property
    def query(self) -> str:
        """SQL query executed against the database."""
        return self._query

    def read(self) -> pa.Table:
        """Connect to the database, execute the query, and return Arrow.

        Returns:
            ``pa.Table`` with the query result set.

        Raises:
            SecurityError: If URI or query contains dangerous content.

        """
        # Security validation
        try:
            validate_uri_scheme(self._uri)
            validate_sql_query(self._query)

        except (SecurityError, ValueError) as e:
            msg = f"Security validation failed for SQL data source: {e}"
            raise SecurityError(msg) from e

        import duckdb

        with duckdb.connect(self._uri) as con:
            return con.execute(self._query).to_arrow_table()


# ---------------------------------------------------------------------------
# In-memory sources (DataFrame, Arrow)
# ---------------------------------------------------------------------------


class DataFrameDataSource(DataSource):
    """Wraps a pandas DataFrame as a :class:`DataSource`.

    The DataFrame is passed through DuckDB to produce a ``pa.Table`` with
    DuckDB's type mapping, ensuring consistent Arrow schema handling.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """Initialise with a pandas DataFrame.

        Args:
            df: The source DataFrame.

        """
        self._df = df

    @property
    def uri(self) -> str:
        """Synthetic URI placeholder for in-memory DataFrame sources."""
        return "<dataframe>"

    @property
    def dataframe(self) -> pd.DataFrame:
        """The wrapped pandas DataFrame."""
        return self._df

    def read(self) -> pa.Table:
        """Convert the DataFrame to an Arrow table via DuckDB.

        Returns:
            ``pa.Table`` with the DataFrame contents.

        """
        import duckdb

        with duckdb.connect() as con:
            # Register the DataFrame with DuckDB so it can be queried
            con.register("df", self._df)
            return con.execute("SELECT * FROM df").to_arrow_table()


class ArrowDataSource(DataSource):
    """Wraps an existing ``pa.Table`` as a :class:`DataSource`.

    :meth:`read` is a direct passthrough — no copy or DuckDB round-trip is
    performed.
    """

    def __init__(self, table: pa.Table) -> None:
        """Initialise with an Arrow table.

        Args:
            table: The source Arrow table.

        """
        self._table = table

    @property
    def uri(self) -> str:
        """Synthetic URI placeholder for in-memory Arrow table sources."""
        return "<arrow>"

    @property
    def table(self) -> pa.Table:
        """The wrapped Arrow table."""
        return self._table

    def read(self) -> pa.Table:
        """Return the wrapped Arrow table.

        Returns:
            The ``pa.Table`` passed to the constructor.

        """
        return self._table


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def data_source_from_uri(
    uri: str | pd.DataFrame | pa.Table,
    *,
    query: str | None = None,
) -> DataSource:
    """Construct the appropriate :class:`DataSource` subclass from *uri*.

    Dispatch rules (evaluated in order):

    1. ``pa.Table`` → :class:`ArrowDataSource`
    2. ``pd.DataFrame`` → :class:`DataFrameDataSource`
    3. SQL-scheme string (``postgresql:``, ``postgres:``, ``mysql:``,
       ``sqlite:``, ``duckdb:``) → :class:`SqlDataSource`
       (*query* is required for SQL sources)
    4. String ending in ``.csv`` → :class:`FileDataSource` with :class:`CsvFormat`
    5. String ending in ``.parquet`` → :class:`FileDataSource` with :class:`ParquetFormat`
    6. String ending in ``.json`` → :class:`FileDataSource` with :class:`JsonFormat`

    The URI may use any scheme that DuckDB supports for steps 4-6:
    ``file://``, ``s3://``, ``gs://``, ``abfss://``, ``http://``,
    ``https://``, or a bare filesystem path.

    Args:
        uri: Source identifier.  Accepts a URI string, a ``pd.DataFrame``,
            or a ``pa.Table``.
        query: Optional SQL override for file-based sources.  Required for
            SQL-scheme (database) sources.

    Returns:
        A :class:`DataSource` instance of the appropriate subclass.

    Raises:
        ValueError: If *uri* is a SQL-scheme string but *query* is ``None``,
            or if the string's extension is not recognised.
        TypeError: If *uri* is not a ``str``, ``pd.DataFrame``, or ``pa.Table``.

    Examples:
        >>> src = data_source_from_uri("s3://bucket/people.parquet")
        >>> src = data_source_from_uri(
        ...     "postgresql://user:pass@host/db",
        ...     query="SELECT * FROM persons WHERE active",
        ... )
        >>> src = data_source_from_uri(my_dataframe)

    """
    if isinstance(uri, pa.Table):
        return ArrowDataSource(uri)
    if isinstance(uri, pd.DataFrame):
        return DataFrameDataSource(uri)
    if not isinstance(uri, str):
        from pycypher.exceptions import WrongCypherTypeError

        msg = f"uri must be str, pd.DataFrame, or pa.Table; got {type(uri).__name__!r}"
        raise WrongCypherTypeError(
            msg,
        )

    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in _SQL_SCHEMES:
        if query is None:
            from pycypher.ingestion.security import mask_uri_credentials

            msg = (
                f"SQL source {mask_uri_credentials(uri)!r} requires a 'query' parameter — "
                "there is no meaningful default for database sources."
            )
            raise ValueError(
                msg,
            )
        return SqlDataSource(uri, query)

    # Extension-based dispatch: works for file://, cloud, and bare paths.
    # Use the parsed path component so query-strings and fragments are ignored.
    path_lower = parsed.path.lower()
    if path_lower.endswith(".csv"):
        return FileDataSource(uri, CsvFormat(), query=query)
    if path_lower.endswith(".parquet"):
        return FileDataSource(uri, ParquetFormat(), query=query)
    if path_lower.endswith(".json"):
        return FileDataSource(uri, JsonFormat(), query=query)

    from pycypher.ingestion.security import mask_uri_credentials

    msg = (
        f"Cannot determine DataSource type for {mask_uri_credentials(uri)!r}. "
        "Provide a URI with a recognised extension (.csv, .parquet, .json) "
        "or a SQL-scheme connection string "
        "(postgresql://, mysql://, sqlite://, duckdb://)."
    )
    raise ValueError(
        msg,
    )
