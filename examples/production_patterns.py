"""Production deployment patterns — rate limiting, audit logging, timeouts.

Demonstrates configuring PyCypher for production workloads with resource
protection, observability, and caching.

Run with: uv run python examples/production_patterns.py
"""

from __future__ import annotations

import logging
import os

import pandas as pd

from pycypher import ContextBuilder, Star, QueryTimeoutError, apply_preset

# Apply production-safe defaults: 30s timeout, 100K cross-join limit,
# complexity gate, rate limiting.  See `pycypher.config` for details.
apply_preset("production")


def setup_audit_logging() -> None:
    """Configure audit logging to stderr for demonstration."""
    os.environ["PYCYPHER_AUDIT_LOG"] = "1"
    from pycypher.audit import enable_audit_log

    enable_audit_log()
    print("Audit logging enabled — watch stderr for JSON records\n")


def demonstrate_timeouts(star: Star) -> None:
    """Show timeout protection for runaway queries."""
    print("=== Timeout Protection ===")
    try:
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name",
            timeout_seconds=10.0,
        )
        print(f"Query returned {len(result)} rows within timeout")
    except QueryTimeoutError as e:
        print(f"Query timed out after {e.elapsed_seconds:.1f}s")
    print()


def demonstrate_caching(star: Star) -> None:
    """Show result caching behavior."""
    print("=== Result Caching ===")

    # First execution — cache miss
    result1 = star.execute_query(
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name"
    )
    print(f"First call: {len(result1)} rows (cache miss)")

    # Second execution — cache hit (same query string)
    result2 = star.execute_query(
        "MATCH (p:Person) RETURN p.name AS name ORDER BY p.name"
    )
    print(f"Second call: {len(result2)} rows (cache hit — no re-execution)")

    try:
        from pycypher import get_cache_stats

        stats = get_cache_stats(star=star)
        print(f"Cache hit rate: {stats['result_cache_hit_rate']:.0%}")
    except (ImportError, AttributeError):
        pass
    print()


def demonstrate_rate_limiting() -> None:
    """Show rate limiting configuration."""
    print("=== Rate Limiting ===")
    try:
        from pycypher.rate_limiter import QueryRateLimiter

        limiter = QueryRateLimiter(qps=100.0, burst=200)
        limiter.acquire()
        print("Rate limiter: query allowed (within limits)")
        print("Configure via PYCYPHER_RATE_LIMIT_QPS and PYCYPHER_RATE_LIMIT_BURST")
    except ImportError:
        print("Rate limiter not available")
    print()


def main() -> None:
    # Suppress debug logs for cleaner output
    logging.getLogger("pycypher").setLevel(logging.WARNING)

    # Build sample data
    people = pd.DataFrame(
        {
            "__ID__": range(1, 101),
            "name": [f"Person_{i}" for i in range(1, 101)],
            "age": [20 + (i % 50) for i in range(1, 101)],
            "dept": ["Eng", "Sales", "Marketing", "Support"][i % 4]
            for i in range(100),
        }
    )

    # Create Star with production-appropriate caching
    context = ContextBuilder.from_dict({"Person": people})
    star = Star(
        context=context,
        result_cache_max_mb=100,
        result_cache_ttl_seconds=60.0,
    )

    # Demonstrate production patterns
    setup_audit_logging()
    demonstrate_timeouts(star)
    demonstrate_caching(star)
    demonstrate_rate_limiting()

    print("=== Production Environment Variables ===")
    print("  PYCYPHER_QUERY_TIMEOUT_S=30")
    print("  PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000")
    print("  PYCYPHER_RESULT_CACHE_MAX_MB=200")
    print("  PYCYPHER_RESULT_CACHE_TTL_S=60")
    print("  PYCYPHER_MAX_UNBOUNDED_PATH_HOPS=10")
    print("  PYCYPHER_SLOW_QUERY_MS=500")
    print("  PYCYPHER_AUDIT_LOG=1")
    print("  PYCYPHER_RATE_LIMIT_QPS=50")
    print("  PYCYPHER_RATE_LIMIT_BURST=100")


if __name__ == "__main__":
    main()
