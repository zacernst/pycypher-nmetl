"""Utilities for writing query result DataFrames to output sinks.

:func:`write_dataframe_to_uri` is the single entry point: it accepts a
``pd.DataFrame``, a destination URI, and an optional explicit
:class:`~pycypher.ingestion.config.OutputFormat`, then writes the file in the
appropriate format.

Supported URI forms
-------------------
* Bare filesystem paths: ``/path/to/output.csv``
* ``file://`` URIs: ``file:///path/to/output.csv``

Cloud URIs (``s3://``, ``gs://``, ``https://``, …) are **not** currently
supported and raise :exc:`NotImplementedError`.  Add cloud support by
integrating ``pyarrow.fs`` or ``fsspec`` as a separate enhancement.

Parent directories are created automatically when they do not exist.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    import pandas as pd

    from pycypher.ingestion.config import OutputFormat

# URI schemes that DuckDB handles as remote storage (not local filesystem)
_CLOUD_SCHEMES: frozenset[str] = frozenset(
    {"s3", "gs", "gcs", "abfss", "adl", "az", "http", "https"},
)


def _resolve_output(uri: str, fmt: OutputFormat | None) -> tuple[Path, OutputFormat]:
    """Resolve *uri* + *fmt* to a validated local path and concrete format.

    Rejects cloud URIs, resolves ``file://``/bare paths, sanitises against
    path traversal, creates parent directories, and infers the format from the
    extension when *fmt* is ``None``.  Shared by :func:`write_dataframe_to_uri`
    and :func:`write_relation_to_uri`.

    Raises:
        NotImplementedError: If *uri* uses a cloud URI scheme.
        ValueError: If *fmt* is ``None`` and the extension is unrecognised.

    """
    from pycypher.ingestion.config import OutputFormat
    from pycypher.ingestion.security import sanitize_file_path

    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in _CLOUD_SCHEMES:
        msg = (
            f"Cloud output URIs are not yet supported: {uri!r}.  "
            "Write to a local file first, then upload using your preferred "
            "cloud SDK (boto3, gcsfs, etc.)."
        )
        raise NotImplementedError(msg)

    # Resolve to a local filesystem path
    if scheme == "file":
        path = Path(parsed.path)
    else:
        # Bare filesystem path (no scheme, or Windows drive letter treated as scheme)
        path = Path(uri)

    # Validate the output path to prevent path traversal attacks.
    sanitize_file_path(str(path))

    # Create parent directories
    path.parent.mkdir(parents=True, exist_ok=True)

    # Infer format from extension when not given explicitly
    if fmt is None:
        ext = path.suffix.lower()
        if ext == ".csv":
            fmt = OutputFormat.CSV
        elif ext == ".parquet":
            fmt = OutputFormat.PARQUET
        elif ext == ".json":
            fmt = OutputFormat.JSON
        else:
            msg = (
                f"Cannot infer output format from URI {uri!r}.  "
                f"Extension {ext!r} is not recognised.  "
                "Supported extensions: .csv, .parquet, .json.  "
                "Pass an explicit OutputFormat to override."
            )
            raise ValueError(msg)

    return path, fmt


def write_dataframe_to_uri(
    df: pd.DataFrame,
    uri: str,
    fmt: OutputFormat | None = None,
) -> None:
    """Write *df* to *uri* in the appropriate format.

    Format is determined by *fmt* when provided; otherwise it is inferred
    from the file extension of *uri*.

    Args:
        df: The :class:`pandas.DataFrame` to serialise.
        uri: Destination URI.  Bare paths and ``file://`` URIs are accepted.
            Cloud URIs raise :exc:`NotImplementedError`.
        fmt: Explicit :class:`~pycypher.ingestion.config.OutputFormat`.
            When ``None``, the format is inferred from the URI extension.

    Raises:
        NotImplementedError: If *uri* uses a cloud URI scheme
            (``s3://``, ``gs://``, ``https://``, …).
        ValueError: If *fmt* is ``None`` and the URI's extension is not
            ``.csv``, ``.parquet``, or ``.json``.

    """
    from pycypher.ingestion.config import OutputFormat

    path, fmt = _resolve_output(uri, fmt)

    if fmt == OutputFormat.CSV:
        df.to_csv(path, index=False)
    elif fmt == OutputFormat.PARQUET:
        df.to_parquet(str(path), index=False)
    elif fmt == OutputFormat.JSON:
        df.to_json(str(path), orient="records", lines=True)
    else:
        msg = f"Unsupported output format: {fmt!r}"
        raise ValueError(msg)


#: DuckDB ``COPY … TO`` option strings per output format.  JSON defaults to
#: newline-delimited records, matching ``DataFrame.to_json(lines=True)``.
_COPY_FORMAT_OPTIONS: dict[str, str] = {
    "csv": "FORMAT CSV, HEADER",
    "parquet": "FORMAT PARQUET",
    "json": "FORMAT JSON",
}


def write_relation_to_uri(
    relation: object,
    uri: str,
    fmt: OutputFormat | None = None,
) -> None:
    """Stream a DuckDB relation to *uri* via ``COPY … TO`` (no pandas frame).

    Out-of-core counterpart to :func:`write_dataframe_to_uri`: instead of
    materialising a pandas DataFrame, the relation is streamed straight to disk
    by DuckDB, so results larger than RAM can be written.

    Args:
        relation: A ``DuckDBLazyFrame`` (as produced by
            :meth:`DataSource.read_relation` or the DuckDB backend).
        uri: Destination URI.  Bare paths and ``file://`` URIs are accepted;
            cloud URIs raise :exc:`NotImplementedError`.
        fmt: Explicit :class:`~pycypher.ingestion.config.OutputFormat`.
            When ``None``, inferred from the URI extension.

    Raises:
        TypeError: If *relation* is not a ``DuckDBLazyFrame``.
        NotImplementedError: If *uri* uses a cloud URI scheme.
        ValueError: If the format cannot be resolved.

    """
    import uuid

    from pycypher.backends.duckdb_backend import DuckDBLazyFrame
    from pycypher.ingestion.security import escape_sql_string_literal

    if not isinstance(relation, DuckDBLazyFrame):
        msg = (
            "write_relation_to_uri expects a DuckDBLazyFrame; got "
            f"{type(relation).__name__}. Use write_dataframe_to_uri for pandas."
        )
        raise TypeError(msg)

    path, fmt = _resolve_output(uri, fmt)
    options = _COPY_FORMAT_OPTIONS[fmt.value]

    con = relation._conn
    view = f"_out_{uuid.uuid4().hex}"
    # create_view binds the relation to a name so COPY can stream from it.
    relation.relation.create_view(view, replace=True)
    try:
        target = escape_sql_string_literal(str(path))
        con.execute(
            f'COPY "{view}" TO {target} ({options})',  # nosec B608 — view is a generated uuid name; target escaped via escape_sql_string_literal
        )
    finally:
        con.execute(f'DROP VIEW IF EXISTS "{view}"')
