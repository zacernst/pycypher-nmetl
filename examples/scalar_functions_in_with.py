"""Demonstration of scalar function parsing in WITH clauses.

PyCypher's grammar parser recognizes scalar functions (toUpper, toLower,
trim, size, substring, toString, toInteger, toFloat, toBoolean, coalesce)
inside WITH clauses, producing typed AST nodes that the relational-algebra
engine can evaluate.

This script shows:
1. Parsing WITH clauses that call scalar functions
2. Inspecting the resulting AST structure
3. End-to-end execution through the Star engine
"""

from __future__ import annotations

import logging

# Suppress framework debug logging for clean demo output
logging.disable(logging.DEBUG)

import pandas as pd
from pycypher.ast_models import (
    ASTConverter,
    FunctionInvocation,
    Query,
    ReturnItem,
    With,
)
from pycypher.grammar_parser import GrammarParser
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.scalar_functions import ScalarFunctionRegistry
from pycypher.star import Star


# ---------------------------------------------------------------------------
# Helper: pretty-print an AST node tree
# ---------------------------------------------------------------------------
def show_ast(label: str, cypher: str) -> Query:
    """Parse a Cypher query, print its AST, and return the typed AST."""
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"  Query: {cypher}")
    print(f"{'=' * 70}")

    ast = ASTConverter.from_cypher(cypher)
    print(ast.pretty())
    return ast  # type: ignore[return-value]


def main() -> None:
    # ===================================================================
    # Part 1 — Parsing scalar functions inside WITH
    # ===================================================================

    print("\n" + "#" * 70)
    print("#  PART 1: Parsing scalar functions inside WITH clauses")
    print("#" * 70)

    # --- 1a. toUpper in WITH ------------------------------------------------
    ast1 = show_ast(
        "toUpper in WITH",
        "MATCH (n:Person) WITH toUpper(n.name) AS upper_name RETURN upper_name",
    )

    # Walk the AST to find the FunctionInvocation node
    for node in ast1.traverse():
        if isinstance(node, With):
            for item in node.items:
                if isinstance(item, ReturnItem) and isinstance(
                    item.expression, FunctionInvocation
                ):
                    fn = item.expression
                    print(
                        f"\n  → Found FunctionInvocation: name={fn.name!r}, "
                        f"alias={item.alias!r}"
                    )

    # --- 1b. Nested scalar functions ----------------------------------------
    show_ast(
        "Nested: toLower(trim(…))",
        "MATCH (n:Person) WITH toLower(trim(n.name)) AS cleaned RETURN cleaned",
    )

    # --- 1c. size() on a string property ------------------------------------
    show_ast(
        "size() for string length",
        "MATCH (n:Person) WITH size(n.name) AS name_len RETURN name_len",
    )

    # --- 1d. toInteger conversion -------------------------------------------
    show_ast(
        "toInteger type conversion",
        "MATCH (n:Person) WITH toInteger(n.score) AS int_score RETURN int_score",
    )

    # --- 1e. Multiple scalar functions in one WITH --------------------------
    show_ast(
        "Multiple functions in one WITH",
        "MATCH (n:Person) "
        "WITH toUpper(n.name) AS upper_name, "
        "     size(n.name)    AS name_len "
        "RETURN upper_name, name_len",
    )

    # ===================================================================
    # Part 2 — Listing available built-in scalar functions
    # ===================================================================

    print("\n" + "#" * 70)
    print("#  PART 2: Available built-in scalar functions")
    print("#" * 70)

    registry = ScalarFunctionRegistry.get_instance()
    for name, meta in sorted(registry._functions.items()):
        print(
            f"  {meta.name:14s}  args=[{meta.min_args}..{meta.max_args or '∞'}]  "
            f"{meta.description}"
        )

    # ===================================================================
    # Part 3 — End-to-end execution with scalar functions in WITH
    # ===================================================================

    print("\n" + "#" * 70)
    print("#  PART 3: End-to-end execution through the Star engine")
    print("#" * 70)

    # Build a small in-memory dataset
    person_df = pd.DataFrame(
        {
            ID_COLUMN: [1, 2, 3],
            "name": ["  Alice  ", "BOB", "carol"],
            "age": [30, 40, 25],
        }
    )

    knows_df = pd.DataFrame(
        {
            ID_COLUMN: [100, 101],
            RELATIONSHIP_SOURCE_COLUMN: [1, 2],
            RELATIONSHIP_TARGET_COLUMN: [2, 3],
        }
    )

    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=[ID_COLUMN, "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=person_df,
    )

    knows_table = RelationshipTable(
        relationship_type="KNOWS",
        identifier="KNOWS",
        column_names=[
            ID_COLUMN,
            RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN,
        ],
        source_obj_attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        attribute_map={
            RELATIONSHIP_SOURCE_COLUMN: RELATIONSHIP_SOURCE_COLUMN,
            RELATIONSHIP_TARGET_COLUMN: RELATIONSHIP_TARGET_COLUMN,
        },
        source_obj=knows_df,
    )

    context = Context(
        entity_mapping=EntityMapping(mapping={"Person": person_table}),
        relationship_mapping=RelationshipMapping(
            mapping={"KNOWS": knows_table}
        ),
    )

    # --- 3a. Execute: toUpper in WITH ---------------------------------------
    print("\n--- 3a. toUpper via WITH ---")
    query_str = "MATCH (n:Person) WITH toUpper(n.name) AS upper_name RETURN upper_name AS upper_name"
    print(f"  Query : {query_str}")
    try:
        star = Star(context=context)
        result_df = star.execute_query(query_str)
        print(f"  Result:\n{result_df.to_string(index=False)}")
    except Exception as exc:
        print(f"  (Execution not yet supported end-to-end: {exc})")

    # --- 3b. Execute: trim + toLower ----------------------------------------
    print("\n--- 3b. trim via WITH ---")
    query_str = "MATCH (n:Person) WITH trim(n.name) AS trimmed RETURN trimmed AS trimmed"
    print(f"  Query : {query_str}")
    try:
        star = Star(context=context)
        result_df = star.execute_query(query_str)
        print(f"  Result:\n{result_df.to_string(index=False)}")
    except Exception as exc:
        print(f"  (Execution not yet supported end-to-end: {exc})")

    # --- 3c. Scalar function registry: direct invocation --------------------
    print("\n--- 3c. Direct registry invocation (bypassing parser) ---")
    series = pd.Series(["  Alice  ", "BOB", "carol"])
    print(f"  Input:    {series.tolist()}")
    print(f"  toUpper:  {registry.execute('toUpper', [series]).tolist()}")
    print(f"  toLower:  {registry.execute('toLower', [series]).tolist()}")
    print(f"  trim:     {registry.execute('trim', [series]).tolist()}")
    print(f"  size:     {registry.execute('size', [series]).tolist()}")

    int_series = pd.Series(["42", "7", "100"])
    print(f"\n  Input:      {int_series.tolist()}")
    print(
        f"  toInteger:  {registry.execute('toInteger', [int_series]).tolist()}"
    )
    print(
        f"  toFloat:    {registry.execute('toFloat', [int_series]).tolist()}"
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
