#!/usr/bin/env python3
"""PyCypher Quick Start — Your first graph query in 5 minutes.

This script introduces PyCypher through a progressive 5-level tutorial
that builds from simple entity queries to relationship traversal and
aggregation. Each level adds one concept, so you always know exactly
what changed.

Target audience: data scientists who know pandas but are new to Cypher.

Run with:
    cd demos/data_scientist_showcase
    uv run python 01_quick_start.py
"""

from __future__ import annotations

import sys
import time

import pandas as pd

# -- local utilities ---------------------------------------------------------
sys.path.insert(0, ".")
from _common import done, section, setup_demo, show_result, timed

# -- pycypher imports --------------------------------------------------------
from pycypher import ContextBuilder, Star


def main() -> None:
    setup_demo("Script 1: Quick Start — Your First Graph Query in 5 Minutes")

    # ── LEVEL 1: Your First Query (3 lines of setup) ──────────────────

    section("Level 1: Basic Entity Query")
    print(
        "The core PyCypher workflow is just three steps:\n"
        "  1. Put your data in a pandas DataFrame\n"
        "  2. Build a query context with ContextBuilder\n"
        "  3. Run Cypher queries with Star\n"
    )

    # Step 1 — data as a DataFrame
    customers = pd.DataFrame(
        {
            "__ID__": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [34, 28, 45, 31, 52],
            "city": ["Atlanta", "Boston", "Chicago", "Denver", "El Paso"],
        }
    )
    print("Input DataFrame:")
    print(customers.to_string(index=False))
    print()

    # Step 2 — build the context
    context = ContextBuilder.from_dict({"Customer": customers})

    # Step 3 — query
    star = Star(context=context)

    with timed("Query"):
        result = star.execute_query(
            "MATCH (c:Customer) RETURN c.name AS name, c.city AS city"
        )

    show_result(result, label="All customers and their cities")

    print(
        "That's it! The Cypher pattern MATCH (c:Customer) reads like English:\n"
        '  "Find every node labelled Customer, call it c."\n'
    )

    # ── LEVEL 2: Filtering with WHERE ─────────────────────────────────

    section("Level 2: Property Filtering with WHERE")
    print(
        "Add a WHERE clause to filter — same idea as pandas .query() or\n"
        "SQL WHERE, but on graph entities.\n"
    )

    with timed("Query"):
        result = star.execute_query(
            "MATCH (c:Customer) WHERE c.age > 30 "
            "RETURN c.name AS name, c.age AS age, c.city AS city "
            "ORDER BY c.age DESC"
        )

    show_result(result, label="Customers older than 30 (sorted by age)")

    print(
        "Cypher WHERE supports the operators you expect:\n"
        "  >, <, >=, <=, =, <>, AND, OR, NOT, CONTAINS, STARTS WITH\n"
    )

    # ── LEVEL 3: Multiple Entity Types ────────────────────────────────

    section("Level 3: Multiple Entity Types")
    print(
        "Real datasets have multiple entity types. Just add more DataFrames.\n"
    )

    products = pd.DataFrame(
        {
            "__ID__": [101, 102, 103, 104, 105],
            "name": ["Laptop", "Headphones", "Keyboard", "Monitor", "Mouse"],
            "price": [1299.99, 199.99, 89.99, 549.99, 49.99],
            "category": ["Electronics", "Electronics", "Accessories", "Electronics", "Accessories"],
        }
    )

    # Rebuild context with both entity types
    context = ContextBuilder.from_dict(
        {"Customer": customers, "Product": products}
    )
    star = Star(context=context)

    with timed("Query"):
        result = star.execute_query(
            "MATCH (p:Product) WHERE p.price < 200 "
            "RETURN p.name AS product, p.price AS price, p.category AS category "
            "ORDER BY p.price ASC"
        )

    show_result(result, label="Products under $200")

    print(
        "You can query any entity type independently. The graph context\n"
        "holds all your data; Cypher selects what you need.\n"
    )

    # ── LEVEL 4: Relationships ────────────────────────────────────────

    section("Level 4: Relationship Traversal")
    print(
        "This is where graph queries shine. Relationships connect entities\n"
        "and let you traverse connections without writing JOIN logic.\n"
    )

    # Relationship DataFrame: Customer --BOUGHT--> Product
    purchases = pd.DataFrame(
        {
            "__ID__": [1001, 1002, 1003, 1004, 1005, 1006],
            "__SOURCE__": [1, 1, 2, 3, 4, 5],      # Customer IDs
            "__TARGET__": [101, 103, 102, 101, 104, 103],  # Product IDs
            "purchase_date": [
                "2024-01-15", "2024-02-20", "2024-01-30",
                "2024-03-10", "2024-02-14", "2024-04-01",
            ],
        }
    )

    print("Purchase relationships (who bought what):")
    print(purchases[["__SOURCE__", "__TARGET__", "purchase_date"]].to_string(index=False))
    print()

    # Build context with entities AND relationships
    context = (
        ContextBuilder()
        .add_entity("Customer", customers)
        .add_entity("Product", products)
        .add_relationship(
            "BOUGHT",
            purchases,
            source_col="__SOURCE__",
            target_col="__TARGET__",
        )
        .build()
    )
    star = Star(context=context)

    with timed("Query"):
        result = star.execute_query(
            "MATCH (c:Customer)-[:BOUGHT]->(p:Product) "
            "RETURN c.name AS customer, p.name AS product, p.price AS price "
            "ORDER BY customer, product"
        )

    show_result(result, label="Who bought what")

    print(
        "The pattern (c:Customer)-[:BOUGHT]->(p:Product) reads:\n"
        '  "Find Customers connected to Products via a BOUGHT relationship."\n'
        "\n"
        "No JOIN syntax needed — the graph structure IS the join.\n"
    )

    # ── LEVEL 5: Aggregation ──────────────────────────────────────────

    section("Level 5: Aggregation")
    print(
        "Combine traversal with aggregation to answer analytical questions\n"
        "in a single query.\n"
    )

    # Question 1: How many items did each customer buy?
    with timed("Query"):
        result = star.execute_query(
            "MATCH (c:Customer)-[:BOUGHT]->(p:Product) "
            "RETURN c.name AS customer, "
            "       count(p) AS items_bought, "
            "       collect(p.name) AS products "
            "ORDER BY items_bought DESC"
        )

    show_result(result, label="Purchase summary per customer")

    # Question 2: Total spend per customer
    with timed("Query"):
        result = star.execute_query(
            "MATCH (c:Customer)-[:BOUGHT]->(p:Product) "
            "RETURN c.name AS customer, "
            "       count(p) AS items, "
            "       sum(p.price) AS total_spend "
            "ORDER BY total_spend DESC"
        )

    show_result(result, label="Total spend per customer")

    print(
        "Available aggregation functions:\n"
        "  count(), sum(), avg(), min(), max(), collect()\n"
    )

    # ── RECAP ──────────────────────────────────────────────────────────

    section("Recap")
    print(
        "In 5 levels you learned the complete PyCypher workflow:\n"
        "\n"
        "  Level 1: MATCH + RETURN        — query entities\n"
        "  Level 2: WHERE                  — filter by properties\n"
        "  Level 3: Multiple entity types  — combine DataFrames\n"
        "  Level 4: Relationships          — traverse connections\n"
        "  Level 5: Aggregation            — count, sum, collect\n"
        "\n"
        "Key takeaway: Cypher lets you ask graph questions in plain,\n"
        "readable syntax — no JOIN boilerplate, no index management.\n"
        "\n"
        "Next: Run 02_backend_performance.py to see how PyCypher\n"
        "scales automatically from 1K to 200K+ rows.\n"
    )

    done()


if __name__ == "__main__":
    main()
