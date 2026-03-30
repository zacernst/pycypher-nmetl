"""Centralized configuration for pycypher runtime behaviour.

All environment-variable-driven configuration is read here at import time
and exposed as module-level constants.  This avoids scattered ``os.environ``
reads and provides a single place to document, validate, and override
runtime settings.

Environment Variables
---------------------

``PYCYPHER_QUERY_TIMEOUT_S``
    Default wall-clock budget for a single query (seconds).
    ``None`` (default) means no timeout.

``PYCYPHER_MAX_CROSS_JOIN_ROWS``
    Hard ceiling on cross-join result size (rows).  Prevents accidental
    Cartesian explosions.  Default: ``10_000_000``.

``PYCYPHER_RESULT_CACHE_MAX_MB``
    Maximum in-memory size for the query result cache (megabytes).
    Default: ``100``.

``PYCYPHER_RESULT_CACHE_TTL_S``
    Time-to-live for cached query results (seconds).  ``0`` (default)
    means entries never expire (only evicted by size pressure).

``PYCYPHER_MAX_UNBOUNDED_PATH_HOPS``
    Hard cap on BFS hops for unbounded variable-length paths (e.g.
    ``[*]``).  Default: ``20``.

``PYCYPHER_AST_CACHE_MAX``
    Maximum number of parsed ASTs cached per ``GrammarParser`` instance.
    LRU eviction when full.  Default: ``1024``.  ``0`` disables caching.

``PYCYPHER_MAX_QUERY_SIZE_BYTES``
    Hard ceiling on raw query string size (bytes).  Rejects queries larger
    than this before parsing.  Default: ``1_048_576`` (1 MiB).

``PYCYPHER_MAX_QUERY_NESTING_DEPTH``
    Maximum nesting depth for AST traversal (e.g. deeply nested WHERE
    clauses).  Prevents stack exhaustion on adversarial inputs.
    Default: ``200``.

``PYCYPHER_MAX_COLLECTION_SIZE``
    Hard ceiling on generated collection sizes — ``range()`` lists,
    ``repeat()``/``lpad()``/``rpad()`` output lengths, and ``UNWIND``
    expansion.  Prevents memory exhaustion from adversarial inputs.
    Default: ``1_000_000`` (1 M elements/characters).

``PYCYPHER_AUDIT_LOG``
    Enable structured query audit logging.  Set to ``1``, ``true``, or
    ``yes`` to emit one JSON record per query to the ``pycypher.audit``
    logger.  Off by default.  Records include query ID, timestamp,
    elapsed time, row count, and parameter names (never values).

Examples
--------

Set a 30-second query timeout and reduce the cross-join ceiling::

    export PYCYPHER_QUERY_TIMEOUT_S=30
    export PYCYPHER_MAX_CROSS_JOIN_ROWS=100000

Disable AST caching (useful for grammar development)::

    export PYCYPHER_AST_CACHE_MAX=0

Increase the result cache to 500 MB with a 5-minute TTL::

    export PYCYPHER_RESULT_CACHE_MAX_MB=500
    export PYCYPHER_RESULT_CACHE_TTL_S=300

Inspect active configuration at runtime::

    nmetl config --show-effective

"""

from __future__ import annotations

import os

from shared.logger import LOGGER

__all__ = [
    "AST_CACHE_MAX_ENTRIES",
    "COMPLEXITY_WARN_THRESHOLD",
    "CROSS_JOIN_WARN_THRESHOLDS",
    "MAX_COLLECTION_SIZE",
    "MAX_COMPLEXITY_SCORE",
    "MAX_CROSS_JOIN_ROWS",
    "MAX_QUERY_NESTING_DEPTH",
    "MAX_QUERY_SIZE_BYTES",
    "MAX_UNBOUNDED_PATH_HOPS",
    "QUERY_TIMEOUT_S",
    "RATE_LIMIT_BURST",
    "RATE_LIMIT_QPS",
    "RESULT_CACHE_MAX_MB",
    "RESULT_CACHE_TTL_S",
]


def _read_int(name: str, default: int) -> int:
    """Read an integer from the environment with validation."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw.replace("_", ""))
    except ValueError:
        LOGGER.warning(
            "Invalid value for %s=%r (expected integer); using default %d",
            name,
            raw,
            default,
        )
        return default
    if value < 0:
        LOGGER.warning(
            "Negative value for %s=%d; using default %d",
            name,
            value,
            default,
        )
        return default
    return value


def _read_float(name: str, default: float | None) -> float | None:
    """Read a float from the environment with validation."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        LOGGER.warning(
            "Invalid value for %s=%r (expected number); using default %s",
            name,
            raw,
            default,
        )
        return default
    if value < 0:
        LOGGER.warning(
            "Negative value for %s=%s; using default %s",
            name,
            value,
            default,
        )
        return default
    return value


# ---------------------------------------------------------------------------
# Validated configuration constants
# ---------------------------------------------------------------------------

QUERY_TIMEOUT_S: float | None = _read_float("PYCYPHER_QUERY_TIMEOUT_S", None)
"""Default query timeout in seconds, or ``None`` for no limit."""

MAX_CROSS_JOIN_ROWS: int = _read_int(
    "PYCYPHER_MAX_CROSS_JOIN_ROWS",
    1_000_000,
)
"""Hard ceiling on cross-join result size (rows)."""

CROSS_JOIN_WARN_THRESHOLDS: tuple[int, ...] = (100_000, 1_000_000)
"""Progressive warning thresholds for cross-join cardinality."""

RESULT_CACHE_MAX_MB: int = _read_int("PYCYPHER_RESULT_CACHE_MAX_MB", 100)
"""Maximum result cache size in MB."""

RESULT_CACHE_TTL_S: float | None = _read_float(
    "PYCYPHER_RESULT_CACHE_TTL_S",
    0.0,
)
"""Result cache TTL in seconds (0 = no expiry)."""

MAX_UNBOUNDED_PATH_HOPS: int = _read_int(
    "PYCYPHER_MAX_UNBOUNDED_PATH_HOPS",
    20,
)
"""Hard cap on BFS hops for unbounded variable-length paths."""

AST_CACHE_MAX_ENTRIES: int = _read_int(
    "PYCYPHER_AST_CACHE_MAX",
    1024,
)
"""Maximum number of parsed ASTs to cache per GrammarParser instance.

When the cache reaches this size, the least-recently-used entry is evicted.
Set to ``0`` to disable AST caching entirely.
"""

MAX_QUERY_SIZE_BYTES: int = _read_int(
    "PYCYPHER_MAX_QUERY_SIZE_BYTES",
    1_048_576,
)
"""Hard ceiling on query string size (bytes).  Default: 1 MiB."""

MAX_QUERY_NESTING_DEPTH: int = _read_int(
    "PYCYPHER_MAX_QUERY_NESTING_DEPTH",
    200,
)
"""Maximum AST traversal nesting depth.  Default: 200."""

MAX_COLLECTION_SIZE: int = _read_int(
    "PYCYPHER_MAX_COLLECTION_SIZE",
    1_000_000,
)
"""Hard ceiling on generated collection/string sizes.  Default: 1M."""

MAX_COMPLEXITY_SCORE: int | None = (
    _read_int("PYCYPHER_MAX_COMPLEXITY_SCORE", 0) or None
)
"""Hard ceiling on query complexity score.  Queries exceeding this are
rejected before execution.  ``None`` (default, env ``0``) disables the gate.
Typical production values: 50–200."""

COMPLEXITY_WARN_THRESHOLD: int | None = (
    _read_int("PYCYPHER_COMPLEXITY_WARN_THRESHOLD", 0) or None
)
"""Soft threshold for query complexity warnings.  Queries scoring above this
emit a warning but still execute.  ``None`` (default, env ``0``) disables
warnings.  Set lower than ``MAX_COMPLEXITY_SCORE`` to get early alerts."""

RATE_LIMIT_QPS: float = _read_float("PYCYPHER_RATE_LIMIT_QPS", 0.0) or 0.0
"""Maximum sustained queries per second.  ``0`` (default) disables rate
limiting entirely.  When enabled, queries exceeding this rate receive a
:class:`~pycypher.exceptions.RateLimitError`."""

RATE_LIMIT_BURST: int = _read_int("PYCYPHER_RATE_LIMIT_BURST", 10)
"""Maximum burst size for rate limiting.  Allows short bursts above the
sustained QPS rate.  Only meaningful when ``RATE_LIMIT_QPS > 0``."""
