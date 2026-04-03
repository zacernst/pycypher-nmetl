"""Backend selection example — choosing the right DataFrame engine.

Demonstrates how to configure PyCypher with different backends (pandas,
duckdb, polars) and when each is appropriate.

Run with: uv run python examples/backend_selection.py
"""

from __future__ import annotations

import pandas as pd

from pycypher import ContextBuilder, Star


def main() -> None:
    # Sample data
    people = pd.DataFrame(
        {
            "__ID__": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "age": [30, 25, 35, 28, 42],
            "dept": ["Eng", "Sales", "Eng", "Sales", "Eng"],
        }
    )

    # --- Default (pandas) backend ---
    context = ContextBuilder.from_dict({"Person": people})
    star = Star(context=context)
    result = star.execute_query(
        "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name, p.age AS age"
    )
    print("=== Pandas backend ===")
    print(result)
    print()

    # --- Auto backend (selects best available) ---
    try:
        context_auto = ContextBuilder.from_dict({"Person": people}).build(
            backend="auto"
        )
        star_auto = Star(context=context_auto)
        result_auto = star_auto.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS headcount"
        )
        print(f"=== Auto backend ({context_auto.backend_name}) ===")
        print(result_auto)
    except Exception as e:
        print(f"Auto backend: {e}")
    print()

    # --- DuckDB backend (install duckdb for this to work) ---
    try:
        context_duckdb = ContextBuilder.from_dict({"Person": people}).build(
            backend="duckdb"
        )
        star_duckdb = Star(context=context_duckdb)
        result_duckdb = star_duckdb.execute_query(
            "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS headcount"
        )
        print("=== DuckDB backend ===")
        print(result_duckdb)
    except Exception as e:
        print(f"DuckDB backend not available: {e}")
    print()

    print("Done! Each backend produces identical results.")
    print("Choose based on your workload:")
    print("  pandas  — small-medium data, prototyping")
    print("  duckdb  — large datasets, analytical queries")
    print("  polars  — CPU-bound transformations")
    print("  auto    — let PyCypher pick the best available")


if __name__ == "__main__":
    main()
