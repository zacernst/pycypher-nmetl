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
    "QUERIES",
    "QUERY_TIMEOUT_S",
    "RATE_LIMIT_BURST",
    "RATE_LIMIT_QPS",
    "RESULT_CACHE_MAX_MB",
    "RESULT_CACHE_TTL_S",
    "apply_preset",
    "show_config",
]


QUERIES = [
    "SELECT percentile(duration, 99) * 1000 AS 'Response time (p99 ms)' FROM Transaction FACET appName AS 'entityName'",
    "SELECT count(apm.service.transaction.duration) AS 'Throughput' FROM Metric WHERE appName LIKE '%' FACET appName AS 'entityName'",
    "SELECT (count(apm.service.error.count) / count(apm.service.transaction.duration)) * 100 AS 'Error rate (%)' FROM Metric WHERE appName LIKE '%' FACET appName AS 'entityName'",
    "SELECT percentile(duration, 99) * 1000 AS 'HTTP response time (ms)' FROM MobileRequest FACET appName AS 'entityName'",
    "SELECT count(apm.mobile.status.error.rate) AS 'HTTP error rate' FROM Metric WHERE appName LIKE '%' FACET appName AS 'entityName'",
    "SELECT count(apm.mobile.failed.call.rate) AS 'Network failures' FROM Metric WHERE appName LIKE '%' FACET appName AS 'entityName'",
    "SELECT percentile(interactionToNextPaint, 99) * 1000 AS 'INP (ms)' FROM PageViewTiming FACET appName AS 'entityName'",
    "SELECT percentile(largestContentfulPaint, 99) * 1000 AS 'LCP (ms)' FROM PageViewTiming FACET appName AS 'entityName'",
    "SELECT percentile(duration, 99) * 1000 AS 'Page load (ms)' FROM PageView FACET appName AS 'entityName'",
    "SELECT rate(count(*), 1 minute) AS 'JavaScript error rate' FROM JavaScriptError FACET appName AS 'entityName'",
    "SELECT percentile(duration, 99) AS 'p99 duration (ms)' FROM SyntheticCheck FACET monitorName AS 'entityName'",
    "SELECT filter(count(*), WHERE result = 'FAILED') AS 'Failures' FROM SyntheticCheck WHERE NOT isMuted FACET location, monitorName AS 'entityName'",
    "SELECT count(apm.mobile.application.launch.count) AS 'App launches' FROM Metric WHERE appName LIKE '%' FACET appName AS 'entityName'",
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


# ---------------------------------------------------------------------------
# Configuration presets
# ---------------------------------------------------------------------------

_PRESETS: dict[str, dict[str, int | float | None]] = {
    "development": {
        # Safe defaults for learning — no timeouts, generous limits.
        "QUERY_TIMEOUT_S": None,
        "MAX_CROSS_JOIN_ROWS": 1_000_000,
        "MAX_COMPLEXITY_SCORE": None,
        "RESULT_CACHE_MAX_MB": 100,
        "RESULT_CACHE_TTL_S": 0.0,
        "RATE_LIMIT_QPS": 0.0,
    },
    "production": {
        # Defensive limits for user-facing queries.
        "QUERY_TIMEOUT_S": 30.0,
        "MAX_CROSS_JOIN_ROWS": 100_000,
        "MAX_COMPLEXITY_SCORE": 100,
        "RESULT_CACHE_MAX_MB": 100,
        "RESULT_CACHE_TTL_S": 300.0,
        "RATE_LIMIT_QPS": 50.0,
    },
    "high_performance": {
        # Trusted environment — maximize throughput.
        "QUERY_TIMEOUT_S": 120.0,
        "MAX_CROSS_JOIN_ROWS": 10_000_000,
        "MAX_COMPLEXITY_SCORE": None,
        "RESULT_CACHE_MAX_MB": 500,
        "RESULT_CACHE_TTL_S": 600.0,
        "RATE_LIMIT_QPS": 0.0,
    },
}


def apply_preset(name: str) -> None:
    """Apply a named configuration preset by updating module globals.

    Available presets:

    ``"development"`` (default)
        Safe for learning — no timeouts, generous limits, no rate limiting.

    ``"production"``
        Defensive limits for user-facing queries — 30 s timeout,
        100 K cross-join ceiling, complexity gate at 100, rate limiting.

    ``"high_performance"``
        Trusted environment — 2 min timeout, 10 M cross-join ceiling,
        500 MB cache, no complexity gate, no rate limiting.

    Example::

        from pycypher.config import apply_preset
        apply_preset("production")

    Raises:
        ValueError: If *name* is not a recognised preset.
    """
    preset = _PRESETS.get(name)
    if preset is None:
        available = ", ".join(sorted(_PRESETS))
        msg = f"Unknown preset {name!r}. Available: {available}"
        raise ValueError(msg)

    g = globals()
    for key, value in preset.items():
        if key not in g:
            continue
        g[key] = value

    LOGGER.info("Applied configuration preset %r", name)


def show_config() -> dict[str, int | float | None]:
    """Return a dict of all current configuration values.

    Useful for debugging and logging the active configuration::

        from pycypher.config import show_config
        print(show_config())
    """
    return {
        "QUERY_TIMEOUT_S": QUERY_TIMEOUT_S,
        "MAX_CROSS_JOIN_ROWS": MAX_CROSS_JOIN_ROWS,
        "RESULT_CACHE_MAX_MB": RESULT_CACHE_MAX_MB,
        "RESULT_CACHE_TTL_S": RESULT_CACHE_TTL_S,
        "MAX_UNBOUNDED_PATH_HOPS": MAX_UNBOUNDED_PATH_HOPS,
        "AST_CACHE_MAX_ENTRIES": AST_CACHE_MAX_ENTRIES,
        "MAX_QUERY_SIZE_BYTES": MAX_QUERY_SIZE_BYTES,
        "MAX_QUERY_NESTING_DEPTH": MAX_QUERY_NESTING_DEPTH,
        "MAX_COLLECTION_SIZE": MAX_COLLECTION_SIZE,
        "MAX_COMPLEXITY_SCORE": MAX_COMPLEXITY_SCORE,
        "QUERIES": QUERIES,
        "COMPLEXITY_WARN_THRESHOLD": COMPLEXITY_WARN_THRESHOLD,
        "RATE_LIMIT_QPS": RATE_LIMIT_QPS,
        "RATE_LIMIT_BURST": RATE_LIMIT_BURST,
    }
