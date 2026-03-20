#!/usr/bin/env python3
"""PyCypher Quickstart — Your First Graph Query in 30 Lines.

This example demonstrates the core PyCypher workflow:
  1. Create in-memory data with pandas DataFrames
  2. Build a query context with ContextBuilder
  3. Execute Cypher queries with Star
  4. Handle errors gracefully

No external files needed — everything runs in-memory.

Run with:
    uv run python examples/quickstart.py
"""

from __future__ import annotations

import logging

# Suppress internal logging for clean demo output
logging.disable(logging.CRITICAL)

import pandas as pd
from pycypher import ContextBuilder, Star, VariableNotFoundError

# ---------------------------------------------------------------------------
# 1. Create some data
# ---------------------------------------------------------------------------

people = pd.DataFrame(
    {
        "__ID__": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [30, 25, 35, 28],
    }
)

knows = pd.DataFrame(
    {
        "__ID__": [1, 2, 3],
        "__SOURCE__": [1, 2, 1],
        "__TARGET__": [2, 3, 4],
        "since": [2020, 2021, 2019],
    }
)

# ---------------------------------------------------------------------------
# 2. Build the context and query engine
# ---------------------------------------------------------------------------

context = (
    ContextBuilder()
    .add_entity("Person", people)
    .add_relationship(
        "KNOWS", knows, source_col="__SOURCE__", target_col="__TARGET__"
    )
    .build()
)

star = Star(context=context)

# ---------------------------------------------------------------------------
# 3. Run queries
# ---------------------------------------------------------------------------

# Simple: find all people
print("=== All People ===")
result = star.execute_query(
    "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC"
)
print(result.to_string(index=False))

# Relationships: who knows whom?
print("\n=== Who Knows Whom ===")
result = star.execute_query(
    "MATCH (a:Person)-[k:KNOWS]->(b:Person) "
    "RETURN a.name AS person, b.name AS knows, k.since AS since "
    "ORDER BY k.since ASC"
)
print(result.to_string(index=False))

# Filtering: people over 28
print("\n=== People Over 28 ===")
result = star.execute_query(
    "MATCH (p:Person) WHERE p.age > 28 RETURN p.name, p.age ORDER BY p.name"
)
print(result.to_string(index=False))

# Aggregation: count connections per person
print("\n=== Connection Count ===")
result = star.execute_query(
    "MATCH (p:Person)-[:KNOWS]->(q:Person) "
    "RETURN p.name AS person, count(q) AS connections "
    "ORDER BY connections DESC"
)
print(result.to_string(index=False))

# ---------------------------------------------------------------------------
# 4. Error handling
# ---------------------------------------------------------------------------

print("\n=== Error Handling ===")
try:
    star.execute_query("MATCH (n:Person) RETURN m.name")
except VariableNotFoundError as e:
    print(f"Caught error: {e}")
    print(f"  Missing variable: {e.variable_name}")
    print(f"  Available: {e.available_variables}")

print("\nDone!")
