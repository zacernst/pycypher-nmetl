"""Cypher-expression → DuckDB-SQL compiler for the out-of-core relation engine.

Compiles a conservative subset of Cypher expressions (property lookups on a
single node variable, literals, arithmetic, comparisons, boolean logic, and
NULL checks) into a DuckDB SQL expression string.  Returns ``None`` for
anything outside the subset so callers treat the query as ineligible and fall
back to the pandas engine.

Safety: operators are matched against fixed whitelists and never interpolated
raw; identifiers go through :func:`sanitize_sql_identifier` and string literals
through :func:`escape_sql_string_literal`, so a compiled expression cannot
inject SQL.

See ``docs/duckdb_out_of_core_design.md`` (Phases 6-7).
"""

from __future__ import annotations

from typing import Any

#: Cypher comparison operator → DuckDB SQL operator.
_CMP_OPS: dict[str, str] = {
    "=": "=",
    "<>": "<>",
    "!=": "<>",
    "<": "<",
    "<=": "<=",
    ">": ">",
    ">=": ">=",
}

#: Allowed arithmetic operators (identity mapping).
_ARITH_OPS: frozenset[str] = frozenset({"+", "-", "*", "/"})

#: Allowed NULL-check operators.
_NULL_OPS: dict[str, str] = {"IS NULL": "IS NULL", "IS NOT NULL": "IS NOT NULL"}


def compile_expression(
    expr: Any,
    resolve: Any,
) -> str | None:
    """Compile *expr* to a DuckDB SQL expression string, or ``None``.

    Args:
        expr: A Cypher expression AST node.
        resolve: A callable ``resolve(var_name, property) -> str | None`` that
            returns the SQL column reference for a property lookup (already
            quoted / alias-qualified), or ``None`` if the variable/property is
            not resolvable.  This decouples the compiler from single-variable
            vs. multi-variable (join) column resolution.

    Returns:
        A parenthesised SQL expression string, or ``None`` if *expr* uses any
        construct outside the supported subset (or references an unresolvable
        variable/property).

    """
    from pycypher.ast_models import (
        And,
        Arithmetic,
        BooleanLiteral,
        Comparison,
        FloatLiteral,
        IntegerLiteral,
        Not,
        NullCheck,
        Or,
        PropertyLookup,
        StringLiteral,
        Variable,
    )
    from pycypher.ingestion.security import escape_sql_string_literal

    def rec(node: Any) -> str | None:
        # --- Leaves ---
        if isinstance(node, PropertyLookup):
            if not isinstance(node.expression, Variable):
                return None
            return resolve(node.expression.name, node.property)
        if isinstance(node, IntegerLiteral):
            return str(int(node.value))
        if isinstance(node, FloatLiteral):
            return repr(float(node.value))
        if isinstance(node, BooleanLiteral):
            return "TRUE" if node.value else "FALSE"
        if isinstance(node, StringLiteral):
            return escape_sql_string_literal(str(node.value))

        # --- Comparisons ---
        if isinstance(node, Comparison):
            op = _CMP_OPS.get(node.operator)
            if op is None:
                return None
            left = rec(node.left)
            right = rec(node.right)
            if left is None or right is None:
                return None
            return f"({left} {op} {right})"

        # --- Arithmetic ---
        if isinstance(node, Arithmetic):
            if node.operator not in _ARITH_OPS:
                return None
            left = rec(node.left)
            right = rec(node.right)
            if left is None or right is None:
                return None
            return f"({left} {node.operator} {right})"

        # --- Boolean logic (n-ary operands) ---
        if isinstance(node, (And, Or)):
            joiner = " AND " if isinstance(node, And) else " OR "
            parts = []
            for operand in node.operands:
                compiled = rec(operand)
                if compiled is None:
                    return None
                parts.append(compiled)
            if not parts:
                return None
            return "(" + joiner.join(parts) + ")"

        if isinstance(node, Not):
            operand = rec(node.operand)
            if operand is None:
                return None
            return f"(NOT {operand})"

        # --- NULL checks ---
        if isinstance(node, NullCheck):
            op = _NULL_OPS.get(node.operator)
            if op is None:
                return None
            operand = rec(node.operand)
            if operand is None:
                return None
            return f"({operand} {op})"

        # Unsupported node type.
        return None

    return rec(expr)
