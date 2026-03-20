#!/usr/bin/env python3
"""PyCypher Social Network Demo — End-to-End Feature Showcase.

Demonstrates the complete PyCypher pipeline using a social network graph:
- People connected by KNOWS relationships
- People working at Companies via WORKS_AT relationships

Run with:
    uv run python examples/social_network/run_demo.py
"""

from __future__ import annotations

import logging
from pathlib import Path

# Suppress framework warnings/errors for clean demo output — the demo
# intentionally triggers error-handling paths that produce log noise.
logging.disable(logging.CRITICAL)

import pandas as pd
from pycypher.ingestion import ContextBuilder
from pycypher.star import Star

DATA_DIR = Path(__file__).parent / "data"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def section(title: str) -> None:
    """Print a section header."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


def show(query: str, result: pd.DataFrame) -> None:
    """Print a query and its result."""
    print(f"\n  Query:\n    {query.strip()}")
    print(f"\n  Result ({len(result)} rows):")
    print("    " + result.to_string(index=False).replace("\n", "\n    "))


# ---------------------------------------------------------------------------
# 1. Data Loading
# ---------------------------------------------------------------------------


def load_graph() -> Star:
    """Load the social network graph from CSV files using ContextBuilder."""
    section("1. DATA LOADING  —  ContextBuilder with CSV files")

    people = pd.read_csv(DATA_DIR / "people.csv")
    companies = pd.read_csv(DATA_DIR / "companies.csv")
    knows = pd.read_csv(DATA_DIR / "knows.csv")
    works_at = pd.read_csv(DATA_DIR / "works_at.csv")

    context = ContextBuilder.from_dict(
        {
            "Person": people,
            "Company": companies,
            "KNOWS": knows,
            "WORKS_AT": works_at,
        },
    )
    star = Star(context=context)

    print(f"\n  Loaded {len(people)} people, {len(companies)} companies")
    print(
        f"  Loaded {len(knows)} KNOWS relationships, {len(works_at)} WORKS_AT relationships"
    )
    print("  Graph ready for querying!")
    return star


# ---------------------------------------------------------------------------
# 2. Basic Queries
# ---------------------------------------------------------------------------


def demo_basic_queries(star: Star) -> None:
    """MATCH and RETURN basics."""
    section("2. BASIC QUERIES  —  MATCH / RETURN")

    query = "MATCH (p:Person) RETURN p.name AS name, p.city AS city"
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 3. Filtering
# ---------------------------------------------------------------------------


def demo_filtering(star: Star) -> None:
    """WHERE clause with property predicates."""
    section("3. FILTERING  —  WHERE clause")

    query = """
        MATCH (p:Person)
        WHERE p.age > 35
        RETURN p.name AS name, p.age AS age, p.city AS city
    """
    result = star.execute_query(query)
    show(query, result)

    query2 = """
        MATCH (p:Person)
        WHERE p.city = 'San Francisco'
        RETURN p.name AS name, p.age AS age
    """
    result2 = star.execute_query(query2)
    show(query2, result2)


# ---------------------------------------------------------------------------
# 4. Relationships
# ---------------------------------------------------------------------------


def demo_relationships(star: Star) -> None:
    """Relationship traversal patterns."""
    section("4. RELATIONSHIPS  —  Pattern matching with edges")

    query = """
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name AS person, friend.name AS friend
    """
    result = star.execute_query(query)
    show(query, result)

    # Two-hop path
    query2 = """
        MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
        WHERE a.name = 'Alice Chen'
        RETURN a.name AS person, b.name AS via, c.name AS friend_of_friend
    """
    result2 = star.execute_query(query2)
    show(query2, result2)


# ---------------------------------------------------------------------------
# 5. Aggregation
# ---------------------------------------------------------------------------


def demo_aggregation(star: Star) -> None:
    """count(), avg(), collect() with grouping."""
    section("5. AGGREGATION  —  count, avg, collect")

    query = """
        MATCH (p:Person)
        RETURN p.city AS city, count(p) AS population, avg(p.age) AS avg_age
    """
    result = star.execute_query(query)
    show(query, result)

    query2 = """
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        RETURN p.name AS person, count(friend) AS friend_count
    """
    result2 = star.execute_query(query2)
    show(query2, result2)


# ---------------------------------------------------------------------------
# 6. WITH clause
# ---------------------------------------------------------------------------


def demo_with_clause(star: Star) -> None:
    """Intermediate processing pipeline with WITH."""
    section("6. WITH CLAUSE  —  Pipeline chaining")

    query = """
        MATCH (p:Person)-[:KNOWS]->(friend:Person)
        WITH p.name AS person, count(friend) AS friends
        WHERE friends >= 3
        RETURN person, friends
    """
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 7. SET Operations
# ---------------------------------------------------------------------------


def demo_set_operations(star: Star) -> None:
    """Property modification with SET clause."""
    section("7. SET OPERATIONS  —  Property modification")

    query = """
        MATCH (p:Person)
        WHERE p.city = 'San Francisco'
        SET p.region = 'West Coast'
        RETURN p.name AS name, p.city AS city, p.region AS region
    """
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 8. Scalar Functions
# ---------------------------------------------------------------------------


def demo_scalar_functions(star: Star) -> None:
    """String, math, and utility functions."""
    section("8. SCALAR FUNCTIONS  —  String, math, and more")

    query = """
        MATCH (p:Person)
        RETURN
            p.name AS name,
            toUpper(p.city) AS city_upper,
            size(p.name) AS name_length
    """
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 9. OPTIONAL MATCH
# ---------------------------------------------------------------------------


def demo_optional_match(star: Star) -> None:
    """Left-join semantics with OPTIONAL MATCH."""
    section("9. OPTIONAL MATCH  —  Left-join semantics")

    query = """
        MATCH (p:Person)
        OPTIONAL MATCH (p)-[:WORKS_AT]->(c:Company)
        RETURN p.name AS person, c.name AS company
    """
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 10. UNWIND
# ---------------------------------------------------------------------------


def demo_unwind(star: Star) -> None:
    """List expansion with UNWIND."""
    section("10. UNWIND  —  List expansion")

    query = """
        UNWIND [1, 2, 3, 4, 5] AS num
        RETURN num, num * num AS squared
    """
    result = star.execute_query(query)
    show(query, result)


# ---------------------------------------------------------------------------
# 11. Query Validation
# ---------------------------------------------------------------------------


def demo_validation() -> None:
    """Syntax validation without execution."""
    section("11. QUERY VALIDATION  —  Syntax checking")

    from pycypher import validate_query

    valid_query = "MATCH (p:Person) RETURN p.name"
    errors = validate_query(valid_query)
    print(f"\n  Query: {valid_query}")
    print(f"  Valid: {len(errors) == 0}")
    if errors:
        for e in errors:
            print(f"  Error: {e}")

    # Semantic error: undefined variable
    bad_query = "MATCH (p:Person) RETURN m.name"
    errors2 = validate_query(bad_query)
    print(f"\n  Query: {bad_query}")
    print(f"  Valid: {len(errors2) == 0}")
    for e in errors2:
        print(f"  Error: {e}")


# ---------------------------------------------------------------------------
# 12. Error Handling
# ---------------------------------------------------------------------------


def demo_error_handling(star: Star) -> None:
    """Custom exceptions with helpful messages."""
    section("12. ERROR HANDLING  —  Custom exceptions")

    # Unknown variable
    try:
        star.execute_query("MATCH (p:Person) RETURN x.name")
    except Exception as exc:
        print(f"\n  Query: MATCH (p:Person) RETURN x.name")
        print(f"  Exception: {type(exc).__name__}")
        print(f"  Message: {exc}")

    # Unknown function
    try:
        star.execute_query("MATCH (p:Person) RETURN toUppper(p.name)")
    except Exception as exc:
        print(f"\n  Query: MATCH (p:Person) RETURN toUppper(p.name)")
        print(f"  Exception: {type(exc).__name__}")
        print(f"  Message: {exc}")


# ---------------------------------------------------------------------------
# 13. Observability — Metrics & Profiling
# ---------------------------------------------------------------------------


def demo_observability(star: Star) -> None:
    """Query profiling and cumulative metrics."""
    section("13. OBSERVABILITY  —  Metrics & profiling")

    from pycypher.query_profiler import QueryProfiler

    profiler = QueryProfiler(star=star)
    report = profiler.profile(
        "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name AS person, f.name AS friend"
    )
    print(f"\n  {report}")

    # Show cumulative query metrics from the session
    try:
        from shared.metrics import QUERY_METRICS

        snapshot = QUERY_METRICS.snapshot()
        print(f"\n  Cumulative session metrics:\n  {snapshot.summary()}")
    except Exception:
        print("\n  (Metrics collection not available)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the complete PyCypher demo."""
    print("PyCypher Social Network Demo")
    print("============================")
    print("Demonstrating Cypher query execution over Pandas DataFrames\n")

    star = load_graph()
    demo_basic_queries(star)
    demo_filtering(star)
    demo_relationships(star)
    demo_aggregation(star)
    demo_with_clause(star)
    demo_set_operations(star)
    demo_scalar_functions(star)
    demo_optional_match(star)
    demo_unwind(star)
    demo_validation()
    demo_error_handling(star)

    demo_observability(star)

    section("DEMO COMPLETE")
    print("\n  All 13 sections executed successfully.")
    print("  See https://pycypher.readthedocs.io for full documentation.\n")


if __name__ == "__main__":
    main()
