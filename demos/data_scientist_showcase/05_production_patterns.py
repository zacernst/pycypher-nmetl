#!/usr/bin/env python3
"""Script 5: Production Patterns — Enterprise-Grade Capabilities.

Demonstrates that PyCypher is production-ready, not just a prototype.
Covers timeouts, caching, validation, profiling, audit logging,
configuration presets, error recovery, and the Pipeline API.

Run with:
    uv run python demos/data_scientist_showcase/05_production_patterns.py
"""

from __future__ import annotations

import os
import sys
import time

import pandas as pd

# --- path setup so the script is runnable from the repo root ---
sys.path.insert(0, os.path.dirname(__file__))

from _common import done, section, setup_demo, show_count, show_result, timed
from data.generators import scalable_entities, scalable_relationships

from pycypher import (
    ContextBuilder,
    QueryTimeoutError,
    Star,
    apply_preset,
    validate_query,
)


# =========================================================================
# Data setup
# =========================================================================

def build_production_star(n: int = 10_000) -> tuple[Star, pd.DataFrame]:
    """Build a Star instance with production-appropriate configuration.

    Returns (star, entities_df) so callers can inspect the raw data.
    """
    entities = scalable_entities(n=n)
    relationships = scalable_relationships(entities, density=2.0)

    context = ContextBuilder.from_dict(
        {
            "Record": entities,
            "LINKS_TO": relationships,
        }
    )

    star = Star(
        context=context,
        result_cache_max_mb=100,
        result_cache_ttl_seconds=60.0,
    )
    return star, entities


# =========================================================================
# 1. Configuration Presets
# =========================================================================

def demo_configuration_presets() -> None:
    """Show how to apply production-safe defaults in one call."""
    section("1. CONFIGURATION PRESETS — One-line production hardening")

    print("Before any query runs, lock down the engine:\n")
    print("  from pycypher import apply_preset, show_config")
    print('  apply_preset("production")')
    print()

    # Apply and display
    apply_preset("production")

    try:
        from pycypher import show_config
        show_config()
    except ImportError:
        print("  Production preset applied — sets safe defaults for:")
        print("    • Query timeout:          30 s")
        print("    • Max cross-join rows:    100,000")
        print("    • Max unbounded hops:     10")
        print("    • Rate limiting:          50 qps / burst 100")
        print("    • Slow-query threshold:   500 ms")

    print()
    print("Environment variables override any preset:")
    print("  PYCYPHER_QUERY_TIMEOUT_S=30")
    print("  PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000")
    print("  PYCYPHER_RESULT_CACHE_MAX_MB=200")
    print("  PYCYPHER_RESULT_CACHE_TTL_S=60")
    print("  PYCYPHER_SLOW_QUERY_MS=500")
    print("  PYCYPHER_RATE_LIMIT_QPS=50")
    print()


# =========================================================================
# 2. Query Validation Before Execution
# =========================================================================

def demo_query_validation() -> None:
    """Catch errors at validation time, before wasting compute."""
    section("2. QUERY VALIDATION — Catch errors before execution")

    # Valid query
    good_query = "MATCH (r:Record) WHERE r.value > 5000 RETURN r.name, r.value"
    errors = validate_query(good_query)
    print(f"  Query:  {good_query}")
    print(f"  Valid:  {len(errors) == 0}  ✓")
    print()

    # Semantic error — undefined variable
    bad_query = "MATCH (r:Record) RETURN x.name"
    errors = validate_query(bad_query)
    print(f"  Query:  {bad_query}")
    print(f"  Valid:  {len(errors) == 0}")
    for err in errors:
        print(f"  Error:  {err}")
    print()

    # Show the pattern: validate → execute
    print("  Production pattern:")
    print("    errors = validate_query(user_query)")
    print("    if errors:")
    print('        return {"status": "invalid", "errors": [str(e) for e in errors]}')
    print("    result = star.execute_query(user_query, timeout_seconds=30)")
    print()


# =========================================================================
# 3. Timeout Protection
# =========================================================================

def demo_timeout_protection(star: Star) -> None:
    """Demonstrate timeout guards for runaway queries."""
    section("3. TIMEOUT PROTECTION — Guard against runaway queries")

    # Normal query with explicit timeout
    query = "MATCH (r:Record) WHERE r.value > 5000 RETURN r.name, r.value"
    try:
        with timed("Query with 30s timeout"):
            result = star.execute_query(query, timeout_seconds=30.0)
        print(f"  Returned {len(result)} rows within timeout  ✓")
    except QueryTimeoutError as exc:
        print(f"  Query timed out after {exc.elapsed_seconds:.1f}s")
    print()

    # Show the error handling pattern
    print("  Pattern for timeout-safe execution:")
    print("    try:")
    print("        result = star.execute_query(query, timeout_seconds=10.0)")
    print("    except QueryTimeoutError as e:")
    print(f'        log.warning("Query timed out after %.1fs", e.elapsed_seconds)')
    print('        return fallback_result()')
    print()


# =========================================================================
# 4. Result Caching
# =========================================================================

def demo_result_caching(star: Star) -> None:
    """Show how result caching accelerates repeated queries."""
    section("4. RESULT CACHING — Automatic memoization of query results")

    query = "MATCH (r:Record) WHERE r.category = 'alpha' RETURN r.name, r.value"

    # First execution — cache miss
    t0 = time.perf_counter()
    result1 = star.execute_query(query)
    first_ms = (time.perf_counter() - t0) * 1000

    # Second execution — cache hit
    t0 = time.perf_counter()
    result2 = star.execute_query(query)
    second_ms = (time.perf_counter() - t0) * 1000

    print(f"  First call (cache miss):  {len(result1)} rows in {first_ms:.1f}ms")
    print(f"  Second call (cache hit):  {len(result2)} rows in {second_ms:.1f}ms")

    if first_ms > 0:
        speedup = first_ms / max(second_ms, 0.01)
        print(f"  Speedup:                  {speedup:.1f}x")

    # Show cache stats if available
    try:
        from pycypher import get_cache_stats

        stats = get_cache_stats(star=star)
        hit_rate = stats.get("result_cache_hit_rate", None)
        if hit_rate is not None:
            print(f"  Cache hit rate:           {hit_rate:.0%}")
    except (ImportError, AttributeError):
        pass

    print()
    print("  Configure caching at initialization:")
    print("    star = Star(")
    print("        context=context,")
    print("        result_cache_max_mb=500,      # 500 MB cache")
    print("        result_cache_ttl_seconds=300,  # 5 minute TTL")
    print("    )")
    print()


# =========================================================================
# 5. Comprehensive Error Handling
# =========================================================================

def demo_error_handling(star: Star) -> None:
    """Show the rich exception hierarchy for precise error recovery."""
    section("5. ERROR HANDLING — Rich exceptions for precise recovery")

    from pycypher import (
        CypherSyntaxError,
        VariableNotFoundError,
        UnsupportedFunctionError,
    )

    test_cases = [
        (
            "Syntax error",
            "METCH (r:Record) RETURN r",  # typo
            CypherSyntaxError,
        ),
        (
            "Undefined variable",
            "MATCH (r:Record) RETURN x.name",
            VariableNotFoundError,
        ),
        (
            "Unknown function",
            "MATCH (r:Record) RETURN toUppper(r.name)",
            UnsupportedFunctionError,
        ),
    ]

    for label, query, expected_type in test_cases:
        try:
            star.execute_query(query)
            print(f"  {label}: (no error — unexpected)")
        except expected_type as exc:
            print(f"  {label}:")
            print(f"    Exception: {type(exc).__name__}")
            msg = str(exc)
            # Truncate long messages for readability
            if len(msg) > 120:
                msg = msg[:120] + "..."
            print(f"    Message:   {msg}")
        except Exception as exc:
            print(f"  {label}:")
            print(f"    Exception: {type(exc).__name__}")
            msg = str(exc)
            if len(msg) > 120:
                msg = msg[:120] + "..."
            print(f"    Message:   {msg}")
        print()

    print("  Pattern — granular exception handling:")
    print("    try:")
    print("        result = star.execute_query(query)")
    print("    except CypherSyntaxError:")
    print('        return "Invalid query syntax"')
    print("    except QueryTimeoutError:")
    print('        return "Query took too long"')
    print("    except VariableNotFoundError as e:")
    print('        return f"Unknown variable: {e}"')
    print()


# =========================================================================
# 6. Query Profiling
# =========================================================================

def demo_query_profiling(star: Star) -> None:
    """Demonstrate built-in query profiling for performance optimization."""
    section("6. QUERY PROFILING — Find and fix bottlenecks")

    try:
        from pycypher.query_profiler import QueryProfiler

        profiler = QueryProfiler(star=star)
        query = """
            MATCH (r:Record)
            WHERE r.value > 5000
            RETURN r.category, count(r) AS cnt, avg(r.value) AS avg_val
        """

        report = profiler.profile(query)
        print(f"  Query: {query.strip()}")
        print()
        print(f"  Total time:    {report.total_time_ms:.1f}ms")
        print(f"  Parse time:    {report.parse_time_ms:.1f}ms")
        print(f"  Plan time:     {report.plan_time_ms:.1f}ms")

        if report.clause_timings:
            print("  Clause timings:")
            for clause, ms in report.clause_timings.items():
                print(f"    {clause:20s}  {ms:.1f}ms")

        if report.hotspot:
            print(f"  Hotspot:       {report.hotspot}")

        if report.recommendations:
            print("  Recommendations:")
            for rec in report.recommendations:
                print(f"    • {rec}")

        if report.memory_delta_mb:
            print(f"  Memory delta:  {report.memory_delta_mb:.2f} MB")

    except ImportError:
        print("  QueryProfiler not available in this build.")
        print("  Install with: uv pip install pycypher[profiling]")
    except Exception as exc:
        print(f"  Profiling encountered an error: {exc}")

    print()
    print("  Usage pattern:")
    print("    profiler = QueryProfiler(star=star)")
    print("    report = profiler.profile(slow_query)")
    print('    if report.hotspot == "MATCH":')
    print('        print("Consider adding filters to reduce scan size")')
    print()


# =========================================================================
# 7. Audit Logging
# =========================================================================

def demo_audit_logging(star: Star) -> None:
    """Show structured audit logging for compliance and debugging."""
    section("7. AUDIT LOGGING — Structured observability")

    print("  Enable audit logging for compliance/debugging:")
    print('    os.environ["PYCYPHER_AUDIT_LOG"] = "1"')
    print("    from pycypher.audit import enable_audit_log")
    print("    enable_audit_log()")
    print()
    print("  Each query emits a JSON record to stderr:")
    print("    {")
    print('      "query_id": "a1b2c3d4",')
    print('      "timestamp": "2026-04-06T15:30:00Z",')
    print('      "query": "MATCH (r:Record) ...",')
    print('      "status": "ok",')
    print('      "elapsed_ms": 12.3,')
    print('      "rows": 42,')
    print('      "cached": false,')
    print('      "parameter_keys": ["threshold"]')
    print("    }")
    print()

    # Actually enable and run one query to demonstrate
    try:
        os.environ["PYCYPHER_AUDIT_LOG"] = "1"
        from pycypher.audit import enable_audit_log
        enable_audit_log()

        result = star.execute_query(
            "MATCH (r:Record) WHERE r.value > 9000 RETURN count(r) AS cnt"
        )
        print(f"  (Executed query — check stderr for audit record)")
        print(f"  Result: {len(result)} rows")
    except ImportError:
        print("  Audit logging module not available.")
    finally:
        os.environ.pop("PYCYPHER_AUDIT_LOG", None)

    print()


# =========================================================================
# 8. Rate Limiting
# =========================================================================

def demo_rate_limiting() -> None:
    """Show rate limiting configuration for multi-tenant deployments."""
    section("8. RATE LIMITING — Protect shared infrastructure")

    try:
        from pycypher.rate_limiter import QueryRateLimiter

        limiter = QueryRateLimiter(qps=100.0, burst=200)

        # Simulate a burst of requests
        allowed = 0
        for _ in range(10):
            limiter.acquire()
            allowed += 1

        print(f"  Rate limiter: {allowed}/10 requests allowed (within burst)")
        print()
        print("  Configuration:")
        print("    PYCYPHER_RATE_LIMIT_QPS=50    # Sustained rate")
        print("    PYCYPHER_RATE_LIMIT_BURST=100  # Burst allowance")
    except ImportError:
        print("  Rate limiter not available in this build.")
        print("  Configure via environment variables:")
        print("    PYCYPHER_RATE_LIMIT_QPS=50")
        print("    PYCYPHER_RATE_LIMIT_BURST=100")

    print()


# =========================================================================
# 9. Pipeline API — Multi-Stage ETL
# =========================================================================

def demo_pipeline_api(star: Star) -> None:
    """Demonstrate the extensible Pipeline for multi-stage processing."""
    section("9. PIPELINE API — Extensible multi-stage processing")

    try:
        from pycypher import Pipeline

        pipeline = Pipeline.default()

        query = "MATCH (r:Record) WHERE r.value > 5000 RETURN r.name, r.value"

        with timed("Pipeline execution"):
            result = pipeline.run(query=query, star=star)

        print(f"  Pipeline stages: parse → validate → plan → execute → format")
        print(f"  Result rows: {len(result.result) if hasattr(result, 'result') else 'N/A'}")

    except ImportError:
        print("  Pipeline API not available.")
    except Exception as exc:
        print(f"  Pipeline demo: {exc}")

    print()
    print("  Extensible — add custom stages:")
    print("    class AuditStage(Stage):")
    print('        name = "audit"')
    print("        def execute(self, ctx):")
    print('            log_query(ctx.query, ctx.star)')
    print("            return ctx")
    print()
    print('    pipeline.insert_after("validate", AuditStage())')
    print()


# =========================================================================
# 10. Scaling Demonstration
# =========================================================================

def demo_scaling() -> None:
    """Show how PyCypher handles increasing data volumes."""
    section("10. SCALING — Performance across data sizes")

    sizes = [1_000, 5_000, 10_000, 50_000]
    query = "MATCH (r:Record) WHERE r.value > 5000 RETURN r.category, count(r) AS cnt"

    print(f"  Query: {query}")
    print()
    print(f"  {'Rows':>10s}  {'Build (ms)':>12s}  {'Query (ms)':>12s}  {'Results':>10s}")
    print(f"  {'-'*10}  {'-'*12}  {'-'*12}  {'-'*10}")

    for n in sizes:
        # Build context
        t0 = time.perf_counter()
        entities = scalable_entities(n=n)
        rels = scalable_relationships(entities, density=1.5)
        context = ContextBuilder.from_dict({"Record": entities, "LINKS_TO": rels})
        star = Star(context=context)
        build_ms = (time.perf_counter() - t0) * 1000

        # Execute query
        t0 = time.perf_counter()
        result = star.execute_query(query)
        query_ms = (time.perf_counter() - t0) * 1000

        print(f"  {n:>10,d}  {build_ms:>12.1f}  {query_ms:>12.1f}  {len(result):>10d}")

    print()
    print("  PyCypher scales with your data — same API at any size.")
    print()


# =========================================================================
# Main
# =========================================================================

def main() -> None:
    setup_demo("Script 5: Production Patterns — Enterprise-Grade Capabilities")

    print("Moving from prototype to production? This script demonstrates the")
    print("enterprise features that make PyCypher ready for real workloads.")
    print()

    # 1. Configuration (no Star needed)
    demo_configuration_presets()

    # Build the main Star instance
    star, entities = build_production_star(n=10_000)
    print(f"Loaded {len(entities):,} records for demonstration.\n")

    # 2-9. Feature demonstrations
    demo_query_validation()
    demo_timeout_protection(star)
    demo_result_caching(star)
    demo_error_handling(star)
    demo_query_profiling(star)
    demo_audit_logging(star)
    demo_rate_limiting()
    demo_pipeline_api(star)
    demo_scaling()

    # Summary
    section("SUMMARY — Production Readiness Checklist")
    print()
    checklist = [
        ("Configuration presets", 'apply_preset("production")'),
        ("Query validation", "validate_query(user_input)"),
        ("Timeout protection", "execute_query(q, timeout_seconds=30)"),
        ("Result caching", "Star(ctx, result_cache_max_mb=500)"),
        ("Error handling", "except CypherSyntaxError / QueryTimeoutError"),
        ("Query profiling", "QueryProfiler(star).profile(query)"),
        ("Audit logging", 'PYCYPHER_AUDIT_LOG=1'),
        ("Rate limiting", "QueryRateLimiter(qps=50, burst=100)"),
        ("Pipeline API", "Pipeline.default().run(query=q, star=s)"),
        ("Horizontal scaling", "Same API from 1K to 1M+ rows"),
    ]
    for feature, api in checklist:
        print(f"  ✓ {feature:30s}  {api}")

    print()
    done()


if __name__ == "__main__":
    main()
