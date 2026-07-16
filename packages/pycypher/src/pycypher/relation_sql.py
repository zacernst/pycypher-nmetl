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

#: Cypher aggregation function → DuckDB SQL aggregate.
_AGG_FUNCS: dict[str, str] = {
    "count": "COUNT",
    "sum": "SUM",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
}


def is_aggregate(expr: Any) -> bool:
    """True if *expr* is an aggregation (``count(*)`` or a supported agg call)."""
    from pycypher.ast_models import CountStar, FunctionInvocation

    if isinstance(expr, CountStar):
        return True
    return (
        isinstance(expr, FunctionInvocation)
        and expr.name.lower() in _AGG_FUNCS
    )


def compile_aggregate(
    expr: Any,
    resolve: Any,
    functions: frozenset[str] | set[str] | None = None,
    resolve_var: Any = None,
) -> str | None:
    """Compile an aggregation expression to a DuckDB SQL aggregate, or ``None``.

    Supports ``count(*)``, ``count(var)``, and
    ``count|sum|avg|min|max(<expr>)`` with optional ``DISTINCT``.  The argument
    expression is compiled with the same *functions*/*resolve_var* as
    :func:`compile_expression`, so registered UDFs and post-``WITH`` scalar
    variables work inside aggregates.  Returns ``None`` for anything else, so
    the query stays ineligible and falls back.
    """
    from pycypher.ast_models import CountStar, FunctionInvocation, Variable

    if isinstance(expr, CountStar):
        return "COUNT(*)"
    if not isinstance(expr, FunctionInvocation):
        return None
    func = _AGG_FUNCS.get(expr.name.lower())
    if func is None:
        return None
    args = (
        expr.arguments.get("arguments", [])
        if isinstance(expr.arguments, dict)
        else []
    )
    if len(args) != 1:
        return None
    distinct = bool(getattr(expr, "distinct", False))
    arg = args[0]
    distinct_kw = "DISTINCT " if distinct else ""
    if func == "COUNT" and isinstance(arg, Variable):
        # A post-WITH scalar column → COUNT(col) (counts non-null); a bound node
        # variable (no scalar resolution) → COUNT(*).
        col = resolve_var(arg.name) if resolve_var is not None else None
        if col is not None:
            return f"COUNT({distinct_kw}{col})"
        return None if distinct else "COUNT(*)"
    inner = compile_expression(arg, resolve, functions, resolve_var)
    if inner is None:
        return None
    return f"{func}({distinct_kw}{inner})"


def compile_expression(
    expr: Any,
    resolve: Any,
    functions: frozenset[str] | set[str] | None = None,
    resolve_var: Any = None,
) -> str | None:
    """Compile *expr* to a DuckDB SQL expression string, or ``None``.

    Args:
        expr: A Cypher expression AST node.
        resolve: A callable ``resolve(var_name, property) -> str | None`` that
            returns the SQL column reference for a property lookup (already
            quoted / alias-qualified), or ``None`` if the variable/property is
            not resolvable.  This decouples the compiler from single-variable
            vs. multi-variable (join) column resolution.
        functions: Optional set of registered scalar-UDF names (lowercase). A
            ``FunctionInvocation`` compiles to a SQL call only when its name is
            in this set (and all arguments compile); otherwise ``None``.

    Returns:
        A parenthesised SQL expression string, or ``None`` if *expr* uses any
        construct outside the supported subset (or references an unresolvable
        variable/property/function).

    """
    from pycypher.ast_models import (
        And,
        Arithmetic,
        BooleanLiteral,
        Comparison,
        FloatLiteral,
        FunctionInvocation,
        IntegerLiteral,
        Not,
        NullCheck,
        Or,
        PropertyLookup,
        StringLiteral,
        Variable,
    )
    from pycypher.ingestion.security import (
        escape_sql_string_literal,
        sanitize_sql_identifier,
    )

    udf_names = functions or frozenset()

    def rec(node: Any) -> str | None:
        # --- Leaves ---
        if isinstance(node, PropertyLookup):
            if not isinstance(node.expression, Variable):
                return None
            return resolve(node.expression.name, node.property)
        # --- Bare variable reference (post-WITH scalar column) ---
        if isinstance(node, Variable):
            if resolve_var is None:
                return None
            return resolve_var(node.name)
        # --- Registered scalar UDF call ---
        if isinstance(node, FunctionInvocation):
            fname = node.name.lower()
            if fname not in udf_names:
                return None
            raw_args = (
                node.arguments.get("arguments", [])
                if isinstance(node.arguments, dict)
                else []
            )
            compiled_args = [rec(a) for a in raw_args]
            if any(c is None for c in compiled_args):
                return None
            safe = sanitize_sql_identifier(fname)
            return f'{safe}({", ".join(compiled_args)})'
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
