"""Security utilities for input validation and sanitization.

This module provides security functions to prevent SQL injection, path traversal,
and other security vulnerabilities in data source operations.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from urllib.parse import urlparse

_logger = logging.getLogger(__name__)

# Re-export for backward compatibility — canonical definition is in exceptions.py.
from pycypher.exceptions import SecurityError as SecurityError


def sanitize_file_path(path: str) -> str:
    """Sanitize a file path to prevent path traversal attacks.

    Args:
        path: The file path to sanitize

    Returns:
        Sanitized file path

    Raises:
        SecurityError: If the path contains dangerous elements

    """
    if not path:
        msg = "Empty path not allowed"
        raise SecurityError(msg)

    # Check for path traversal attempts
    if ".." in path:
        msg = "Path traversal detected: .. not allowed in paths"
        raise SecurityError(msg)

    # Check for absolute paths that might access sensitive areas
    _SENSITIVE_PREFIXES = (
        "/etc/",
        "/root/",
        "/proc/",
        "/sys/",
        "/dev/",
        "/var/run/",
        "/var/lib/",
        "/boot/",
        "/sbin/",
    )
    if path.startswith(_SENSITIVE_PREFIXES):
        msg = f"Access to sensitive system path denied: {path}"
        raise SecurityError(msg)

    # Normalize the path to remove any remaining traversal attempts.
    # SECURITY: Return the *resolved* path to eliminate TOCTOU race conditions
    # where the original path could be swapped (e.g. via symlink) between
    # validation and actual file access.
    try:
        normalized = str(Path(path).resolve())
        if ".." in normalized:
            msg = "Path traversal detected after normalization"
            raise SecurityError(msg)
    except (OSError, ValueError) as e:
        msg = f"Invalid path: {e}"
        raise SecurityError(msg) from e

    # Re-check sensitive prefixes against the resolved path, since symlinks
    # could redirect to sensitive locations that the original path hid.
    if normalized.startswith(_SENSITIVE_PREFIXES):
        msg = f"Access to sensitive system path denied after resolution: {normalized}"
        raise SecurityError(msg)

    return normalized


def _strip_sql_comments(query: str) -> str:
    """Remove SQL comments from a query string before validation.

    Handles single-line comments (``--``, ``#``) and block comments
    (``/* ... */``), respecting string literals so that comment tokens
    inside quoted strings are preserved.

    Args:
        query: Raw SQL query string.

    Returns:
        Query with comments removed.

    Raises:
        SecurityError: If an unterminated block comment is detected.

    """
    result: list[str] = []
    i = 0
    in_single_quote = False
    in_double_quote = False

    while i < len(query):
        char = query[i]

        # Track string literals using SQL-standard doubled-quote escaping.
        if char == "'" and not in_double_quote:
            # Two consecutive single quotes ('') are an escape, not a toggle.
            if i + 1 < len(query) and query[i + 1] == "'":
                result.append("''")
                i += 2
                continue
            in_single_quote = not in_single_quote
            result.append(char)
            i += 1
            continue
        if char == '"' and not in_single_quote:
            if i + 1 < len(query) and query[i + 1] == '"':
                result.append('""')
                i += 2
                continue
            in_double_quote = not in_double_quote
            result.append(char)
            i += 1
            continue

        # Outside of string literals, strip comments.
        if not in_single_quote and not in_double_quote:
            # Block comment: /* ... */
            if char == "/" and i + 1 < len(query) and query[i + 1] == "*":
                end = query.find("*/", i + 2)
                if end == -1:
                    msg = "Unterminated block comment detected in SQL query"
                    raise SecurityError(msg)
                # Replace the comment with a single space to avoid token merging.
                result.append(" ")
                i = end + 2
                continue
            # Single-line comment: -- or #
            if (
                char == "-" and i + 1 < len(query) and query[i + 1] == "-"
            ) or char == "#":
                # Skip to end of line.
                newline = query.find("\n", i)
                if newline == -1:
                    break  # rest of query is a comment
                i = newline + 1
                continue

        result.append(char)
        i += 1

    return "".join(result)


def _count_unquoted_semicolons(query: str) -> int:
    """Count semicolons outside of string literals.

    Uses SQL-standard doubled-quote escaping (``''`` and ``""``) rather
    than backslash escaping to correctly handle all SQL dialects.

    Args:
        query: SQL query (comments should already be stripped).

    Returns:
        Number of semicolons found outside string literals.

    """
    count = 0
    in_single_quote = False
    in_double_quote = False
    i = 0

    while i < len(query):
        char = query[i]

        if char == "'" and not in_double_quote:
            if i + 1 < len(query) and query[i + 1] == "'":
                i += 2  # skip escaped quote
                continue
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            if i + 1 < len(query) and query[i + 1] == '"':
                i += 2
                continue
            in_double_quote = not in_double_quote
        elif char == ";" and not in_single_quote and not in_double_quote:
            count += 1

        i += 1

    return count


# Allowlist: only these statement types are permitted for data queries.
_ALLOWED_SQL_PREFIXES: frozenset[str] = frozenset(
    {
        "select",
        "with",  # CTEs that precede SELECT
    },
)


# DuckDB table-valued functions that can read files, list directories, or
# expose internal metadata.  User-supplied queries must not invoke these.
_DANGEROUS_TABLE_FUNCTIONS: frozenset[str] = frozenset(
    {
        "read_csv",
        "read_csv_auto",
        "read_parquet",
        "read_json",
        "read_json_auto",
        "read_json_objects",
        "read_json_objects_auto",
        "read_blob",
        "read_text",
        "read_ndjson",
        "read_ndjson_auto",
        "read_ndjson_objects",
        "glob",
        "parquet_scan",
        "parquet_metadata",
        "parquet_schema",
        "parquet_file_metadata",
        "parquet_kv_metadata",
        "sniff_csv",
        "query_table",
        "query",
    },
)

#: Prefix patterns for DuckDB system/metadata functions.
_DANGEROUS_FUNCTION_PREFIXES: tuple[str, ...] = (
    "duckdb_",
    "pg_",
    "pragma_",
    "information_schema",
)


def _reject_dangerous_table_functions(normalised_query: str) -> None:
    """Raise :class:`SecurityError` if *normalised_query* invokes dangerous table functions.

    DuckDB's ``SELECT`` can call table-valued functions such as
    ``read_csv('/etc/passwd')`` or ``glob('/home/*')`` that would let an
    attacker read arbitrary files or list directories.  This function
    scans the lowercased, comment-stripped query for known dangerous
    function-call patterns and rejects them.

    Args:
        normalised_query: Lowercased, whitespace-collapsed, comment-stripped SQL.

    Raises:
        SecurityError: If a dangerous table function call is detected.

    """
    for func_name in _DANGEROUS_TABLE_FUNCTIONS:
        # Match function_name( with optional whitespace before the paren
        pattern = rf"\b{re.escape(func_name)}\s*\("
        if re.search(pattern, normalised_query):
            msg = (
                f"Dangerous DuckDB table function {func_name!r} detected in query. "
                "User-supplied queries may only reference the 'source' view. "
                "Direct file/network access via table functions is not allowed."
            )
            raise SecurityError(msg)

    for prefix in _DANGEROUS_FUNCTION_PREFIXES:
        pattern = rf"\b{re.escape(prefix)}\w*\s*\("
        if re.search(pattern, normalised_query):
            msg = (
                f"Dangerous DuckDB function with prefix {prefix!r} detected in query. "
                "User-supplied queries may only reference the 'source' view. "
                "Access to internal metadata functions is not allowed."
            )
            raise SecurityError(msg)


def validate_sql_query(query: str) -> None:
    """Validate a SQL query using an allowlist approach.

    Only ``SELECT`` and ``WITH … SELECT`` queries are permitted.  The
    query is first stripped of comments (which are a common injection
    vector) and then checked against a strict allowlist of permitted
    statement prefixes.  Multiple statements are rejected.

    Args:
        query: SQL query to validate.

    Raises:
        SecurityError: If the query is empty, contains comments, multiple
            statements, or is not an allowed statement type.

    """
    if not query or not query.strip():
        msg = "Empty query not allowed"
        raise SecurityError(msg)

    # --- Phase 1: strip comments (injection vector) ---
    try:
        cleaned = _strip_sql_comments(query)
    except SecurityError:
        raise
    except (ValueError, IndexError) as exc:
        # _strip_sql_comments is deterministic and raises SecurityError for
        # known-bad input.  This catch handles unexpected parsing failures
        # (e.g. malformed edge-case strings) by promoting them to
        # SecurityError — fail-closed on any surprise.
        msg = f"Failed to parse SQL query for validation: {exc}"
        raise SecurityError(msg) from exc

    cleaned_stripped = cleaned.strip()
    if not cleaned_stripped:
        msg = "Query contains only comments"
        raise SecurityError(msg)

    # --- Phase 2: reject multiple statements ---
    semicolon_count = _count_unquoted_semicolons(cleaned_stripped)
    # Allow at most one trailing semicolon (some tools add it).
    if semicolon_count > 1:
        msg = "Multiple SQL statements detected - potential injection attack"
        raise SecurityError(msg)
    # If there is exactly one semicolon, it must be the very last non-space char.
    if semicolon_count == 1 and not cleaned_stripped.rstrip().endswith(";"):
        msg = "Semicolon found in the middle of the query - potential injection attack"
        raise SecurityError(msg)

    # --- Phase 3: allowlist check on the first keyword ---
    # Collapse whitespace so the first keyword is easy to isolate.
    normalised = (
        re.sub(r"\s+", " ", cleaned_stripped.rstrip(";")).strip().lower()
    )
    first_word = normalised.split(" ", maxsplit=1)[0] if normalised else ""

    if first_word not in _ALLOWED_SQL_PREFIXES:
        msg = (
            f"Only SELECT queries are allowed for data ingestion. "
            f"Got statement starting with: {first_word!r}"
        )
        raise SecurityError(msg)

    # --- Phase 4: block dangerous DuckDB table functions ---
    # DuckDB SELECT can call table functions that read arbitrary files,
    # list directories, or access internal metadata.  Only the ``source``
    # view (created by the caller) should appear in FROM clauses.
    _reject_dangerous_table_functions(normalised)


def sanitize_sql_identifier(identifier: str) -> str:
    """Sanitize SQL identifiers (table names, column names) to prevent injection.

    Args:
        identifier: SQL identifier to sanitize

    Returns:
        Sanitized identifier

    Raises:
        SecurityError: If the identifier is invalid

    """
    if not identifier:
        msg = "Empty identifier not allowed"
        raise SecurityError(msg)

    # SQL identifiers should only contain alphanumeric characters and underscores
    # and should not start with a number
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", identifier):
        msg = f"Invalid SQL identifier: '{identifier}'. Must contain only letters, numbers, and underscores, and cannot start with a number"
        raise SecurityError(msg)

    # Check against SQL reserved words (basic list)
    reserved_words = {
        "select",
        "from",
        "where",
        "insert",
        "update",
        "delete",
        "drop",
        "create",
        "alter",
        "table",
        "database",
        "index",
        "view",
        "grant",
        "revoke",
        "union",
        "order",
        "group",
        "having",
        "join",
        "inner",
        "outer",
        "left",
        "right",
        "exec",
        "execute",
        "sp_",
        "xp_",
    }

    if identifier.lower() in reserved_words or identifier.lower().startswith(
        ("sp_", "xp_"),
    ):
        msg = f"SQL reserved word not allowed as identifier: '{identifier}'"
        raise SecurityError(msg)

    return identifier


def validate_uri_scheme(uri: str) -> str:
    """Validate URI scheme for data sources.

    Args:
        uri: URI to validate

    Returns:
        The validated URI

    Raises:
        SecurityError: If the URI scheme is dangerous

    """
    if not uri:
        msg = "Empty URI not allowed"
        raise SecurityError(msg)

    try:
        parsed = urlparse(uri)
    except AttributeError as e:
        msg = f"Invalid URI format: {e}"
        raise SecurityError(msg) from e

    # Allow specific safe schemes only
    safe_schemes = {
        "file",
        "http",
        "https",
        "s3",
        "gs",
        "gcs",
        "abfss",
        "azure",  # Storage schemes
        "postgresql",
        "postgres",
        "mysql",
        "sqlite",
        "duckdb",  # Database schemes
        "",  # Empty scheme for bare file paths
    }

    if parsed.scheme and parsed.scheme.lower() not in safe_schemes:
        msg = f"URI scheme '{parsed.scheme}' not allowed. Allowed schemes: {sorted(safe_schemes)}"
        raise SecurityError(msg)

    # SSRF protection: block requests to private/internal networks.
    if parsed.scheme in ("http", "https") and parsed.hostname:
        _check_ssrf_hostname(parsed.hostname)

    return uri


def _check_ssrf_hostname(hostname: str) -> None:
    """Raise :class:`SecurityError` if *hostname* resolves to a private or internal IP.

    Blocks RFC 1918 private ranges, loopback, link-local, and well-known
    internal hostnames to prevent Server-Side Request Forgery (SSRF) attacks
    that could reach internal services.

    Args:
        hostname: The hostname or IP address to check.

    Raises:
        SecurityError: If the hostname is private, internal, or unresolvable.

    """
    import ipaddress
    import socket

    hostname_lower = hostname.lower()

    # Block well-known internal hostnames.
    _INTERNAL_HOSTNAMES: frozenset[str] = frozenset(
        {
            "localhost",
            "localhost.localdomain",
            "ip6-localhost",
            "ip6-loopback",
        },
    )
    if hostname_lower in _INTERNAL_HOSTNAMES:
        msg = (
            f"SSRF protection: hostname {hostname!r} resolves to a local address. "
            "Requests to internal services are not allowed."
        )
        raise SecurityError(msg)

    # Try to parse as a literal IP address first.
    try:
        addr = ipaddress.ip_address(hostname)
    except ValueError:
        # Not a literal IP — resolve DNS to check the actual address.
        try:
            resolved = socket.getaddrinfo(
                hostname,
                None,
                proto=socket.IPPROTO_TCP,
            )
        except socket.gaierror as exc:
            # SECURITY: Reject on DNS failure rather than allowing the request
            # through.  An attacker could intentionally cause DNS failures to
            # bypass SSRF checks, then exploit DNS rebinding or race conditions.
            msg = (
                f"SSRF protection: DNS resolution failed for hostname {hostname!r} "
                f"({exc}).  Requests to unresolvable hosts are blocked."
            )
            raise SecurityError(msg) from exc
        if not resolved:
            return
        # Check the first resolved address.
        try:
            addr = ipaddress.ip_address(resolved[0][4][0])
        except (ValueError, IndexError):
            return

    if (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_reserved
    ):
        msg = (
            f"SSRF protection: address {addr} (from hostname {hostname!r}) is in a "
            f"private/internal range. Requests to internal services are not allowed."
        )
        raise SecurityError(msg)


def escape_sql_string_literal(value: str) -> str:
    """Escape a string value for safe inclusion in SQL as a string literal.

    Args:
        value: String value to escape

    Returns:
        Escaped string safe for SQL inclusion

    """
    if value is None:
        return "NULL"

    # SECURITY: Reject NUL bytes which can truncate strings in some SQL
    # engines, potentially bypassing validation that checked the full string.
    if "\x00" in value:
        msg = "NUL byte (\\x00) not allowed in SQL string literals"
        raise SecurityError(msg)

    # Escape single quotes by doubling them
    escaped = value.replace("'", "''")

    # Return as a quoted string literal
    return f"'{escaped}'"


def parameterize_duckdb_query(template: str, **params: str) -> str:
    """Create a parameterized DuckDB query with proper escaping.

    Args:
        template: SQL template with {param} placeholders
        **params: Named parameters to substitute

    Returns:
        SQL with safely escaped parameters

    Raises:
        SecurityError: If template or parameters are invalid

    """
    if not template:
        msg = "Empty SQL template not allowed"
        raise SecurityError(msg)

    try:
        # First validate all parameter values
        escaped_params = {}
        for key, value in params.items():
            # Validate parameter name
            sanitize_sql_identifier(key)
            # Escape parameter value
            escaped_params[key] = escape_sql_string_literal(value)

        # Format the template with escaped parameters
        return template.format(**escaped_params)

    except KeyError as e:
        msg = f"Missing parameter in SQL template: {e}"
        raise SecurityError(msg) from e
    except (ValueError, TypeError, AttributeError) as e:
        msg = f"Error parameterizing SQL query: {e}"
        raise SecurityError(msg) from e


def mask_uri_credentials(uri: str) -> str:
    """Mask credentials in a URI for safe display in logs and console output.

    Replaces the password component of URIs like
    ``postgresql://user:s3cret@host/db`` with ``***`` to prevent credential
    leakage in logs, CLI output, and error messages.

    Args:
        uri: The URI string to mask.

    Returns:
        The URI with any password replaced by ``***``.  If the URI has no
        credentials or cannot be parsed, returns the original string unchanged.

    """
    if not uri:
        return uri

    try:
        parsed = urlparse(uri)
    except Exception:
        _logger.debug(
            "URI parse failed during password masking", exc_info=True
        )
        return uri

    if not parsed.password:
        return uri

    # Reconstruct with masked password.  urlparse splits userinfo into
    # username/password but doesn't provide a clean setter, so we replace
    # the userinfo portion in the netloc directly.
    masked_userinfo = f"{parsed.username}:***"
    # netloc may include host:port after the @
    host_part = parsed.hostname or ""
    if parsed.port:
        host_part = f"{host_part}:{parsed.port}"
    masked_netloc = f"{masked_userinfo}@{host_part}"

    from urllib.parse import urlunparse

    return urlunparse(
        (
            parsed.scheme,
            masked_netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        ),
    )
