#!/usr/bin/env python3
"""PyCypher Hello World — Working query in under 5 minutes.

Run with:
    uv run python examples/hello_world.py
"""

from __future__ import annotations

import logging

logging.disable(logging.CRITICAL)

import pandas as pd
from pycypher import ContextBuilder, Star

# ── Level 1: Your First Query (3 lines of setup) ─────────────────────

people = pd.DataFrame({
    "__ID__": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "age": [30, 25, 35],
})

context = ContextBuilder.from_dict({"Person": people})
star = Star(context=context)

result = star.execute_query(
    "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
)
print("── Level 1: Basic Query ──")
print(result.to_string(index=False))
# Expected:
#   name  age
#  Alice   30
#    Bob   25
#  Carol   35

# ── Level 2: Filtering with WHERE ────────────────────────────────────

result = star.execute_query(
    "MATCH (p:Person) WHERE p.age >= 30 RETURN p.name AS name, p.age AS age"
)
print("\n── Level 2: WHERE Filtering ──")
print(result.to_string(index=False))
# Expected:
#   name  age
#  Alice   30
#  Carol   35

# ── Level 3: Multiple Entity Types ───────────────────────────────────

products = pd.DataFrame({
    "__ID__": [10, 20, 30],
    "title": ["Widget", "Gadget", "Gizmo"],
    "price": [9.99, 49.99, 24.99],
})

context = ContextBuilder.from_dict({
    "Person": people,
    "Product": products,
})
star = Star(context=context)

result = star.execute_query(
    "MATCH (p:Product) WHERE p.price < 30 "
    "RETURN p.title AS product, p.price AS price ORDER BY p.price ASC"
)
print("\n── Level 3: Multiple Entity Types ──")
print(result.to_string(index=False))
# Expected:
#  product  price
#   Widget   9.99
#    Gizmo  24.99

# ── Level 4: Relationships ────────────────────────────────────────────

purchases = pd.DataFrame({
    "__ID__": [100, 101, 102],
    "__SOURCE__": [1, 2, 1],    # Person IDs
    "__TARGET__": [10, 20, 30],  # Product IDs
    "date": ["2024-01", "2024-02", "2024-03"],
})

context = (
    ContextBuilder()
    .add_entity("Person", people)
    .add_entity("Product", products)
    .add_relationship(
        "BOUGHT", purchases,
        source_col="__SOURCE__", target_col="__TARGET__",
    )
    .build()
)
star = Star(context=context)

result = star.execute_query(
    "MATCH (person:Person)-[:BOUGHT]->(item:Product) "
    "RETURN person.name AS buyer, item.title AS product "
    "ORDER BY buyer, product"
)
print("\n── Level 4: Relationships ──")
print(result.to_string(index=False))
# Expected:
#  buyer product
#  Alice   Gizmo
#  Alice  Widget
#    Bob  Gadget

# ── Level 5: Aggregation ─────────────────────────────────────────────

result = star.execute_query(
    "MATCH (person:Person)-[:BOUGHT]->(item:Product) "
    "RETURN person.name AS buyer, count(item) AS items_bought, "
    "       collect(item.title) AS products "
    "ORDER BY items_bought DESC"
)
print("\n── Level 5: Aggregation ──")
print(result.to_string(index=False))
# Expected:
#  buyer  items_bought      products
#  Alice             2  [Widget, Gizmo]
#    Bob             1        [Gadget]

print("\nAll examples passed!")
