"""Query complexity scoring for resource protection.

Analyzes a parsed Cypher AST to compute a complexity score based on:
- Number of clauses
- Number of MATCH patterns and joins
- Variable-length path expansion (unbounded is expensive)
- Aggregation and DISTINCT operations
- Nesting depth (FOREACH, subqueries)
- Cross-product potential (multiple unrelated MATCH clauses)

The score can be compared against a configurable limit to reject or warn
about queries that are likely to consume excessive resources.

Usage::

    from pycypher.query_complexity import score_query, QueryComplexityError

    score = score_query(parsed_query)
    # score.total == 15, score.breakdown == {"clauses": 3, "joins": 4, ...}

    # Or with a hard limit:
    score = score_query(parsed_query, max_score=50)
    # Raises QueryComplexityError if score exceeds limit
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Re-export for backward compatibility — canonical definition is in exceptions.py.
from pycypher.exceptions import QueryComplexityError as QueryComplexityError

#: Default complexity weights for each AST feature.
DEFAULT_WEIGHTS: dict[str, int] = {
    "clause": 1,
    "match_pattern": 2,
    "optional_match": 3,
    "variable_length_path": 5,
    "unbounded_path": 10,
    "aggregation": 3,
    "distinct": 2,
    "order_by": 1,
    "foreach": 4,
    "foreach_nesting": 8,
    "union": 3,
    "create": 2,
    "merge": 3,
    "delete": 2,
    "cross_product_risk": 8,
    "exists_subquery": 4,
    "list_comprehension": 2,
    "case_expression": 1,
}


@dataclass
class ComplexityScore:
    """Result of query complexity analysis.

    Attributes:
        total: The total complexity score.
        breakdown: Per-feature contribution to the score.
        warnings: Human-readable warnings about risky patterns.

    """

    total: int = 0
    breakdown: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def score_query(
    query: Any,
    *,
    weights: dict[str, int] | None = None,
    max_score: int | None = None,
) -> ComplexityScore:
    """Compute a complexity score for a parsed Cypher query AST.

    Args:
        query: A parsed :class:`~pycypher.ast_models.Query` or
            :class:`~pycypher.ast_models.UnionQuery` AST node.
        weights: Optional custom weights (merged with defaults).
        max_score: If set, raises :class:`QueryComplexityError` when exceeded.

    Returns:
        A :class:`ComplexityScore` with total, breakdown, and warnings.

    Raises:
        QueryComplexityError: If *max_score* is set and the score exceeds it.

    """
    w = {**DEFAULT_WEIGHTS, **(weights or {})}
    result = ComplexityScore()

    _score_node(query, w, result, depth=0)

    if max_score is not None and result.total > max_score:
        raise QueryComplexityError(
            score=result.total,
            limit=max_score,
            breakdown=result.breakdown,
        )

    return result


def _add(result: ComplexityScore, key: str, points: int) -> None:
    """Add points to a complexity category."""
    result.total += points
    result.breakdown[key] = result.breakdown.get(key, 0) + points


def _score_node(
    node: Any,
    w: dict[str, int],
    result: ComplexityScore,
    *,
    depth: int,
) -> None:
    """Recursively score an AST node."""
    from pycypher.ast_models import (
        Create,
        Delete,
        Foreach,
        Match,
        Merge,
        Query,
        Return,
        UnionQuery,
        With,
    )

    if isinstance(node, UnionQuery):
        _add(result, "union", w["union"] * len(node.statements))
        for stmt in node.statements:
            _score_node(stmt, w, result, depth=depth)
        return

    if isinstance(node, Query):
        for clause in node.clauses:
            _add(result, "clauses", w["clause"])
            _score_node(clause, w, result, depth=depth)

        # Detect cross-product risk: multiple MATCH clauses with disjoint variables
        match_clauses = [
            c for c in node.clauses if isinstance(c, Match) and not c.optional
        ]
        if len(match_clauses) > 1:
            _add(result, "cross_product_risk", w["cross_product_risk"])
            result.warnings.append(
                f"Multiple MATCH clauses ({len(match_clauses)}) detected — "
                f"may produce cross-product if variables are disjoint.",
            )
        return

    if isinstance(node, Match):
        if node.optional:
            _add(result, "optional_match", w["optional_match"])
        else:
            _add(result, "match_pattern", w["match_pattern"])
        # Count variable-length paths
        if node.pattern and hasattr(node.pattern, "paths"):
            for path in node.pattern.paths:
                for elem in path.elements:
                    if hasattr(elem, "length") and elem.length is not None:
                        if getattr(elem.length, "unbounded", False):
                            _add(result, "unbounded_path", w["unbounded_path"])
                            result.warnings.append(
                                "Unbounded variable-length path [*] detected — "
                                "consider adding a hop bound.",
                            )
                        else:
                            _add(
                                result,
                                "variable_length_path",
                                w["variable_length_path"],
                            )
        # Score WHERE expression recursively
        if node.where is not None:
            _score_expression(node.where, w, result, depth=depth)
        return

    if isinstance(node, (With, Return)):
        if getattr(node, "distinct", False):
            _add(result, "distinct", w["distinct"])
        if getattr(node, "order_by", None):
            _add(result, "order_by", w["order_by"])
        # Check for aggregation in return items
        for item in getattr(node, "items", []):
            expr = getattr(item, "expression", None)
            if expr is not None:
                _score_expression(expr, w, result, depth=depth)
        return

    if isinstance(node, Foreach):
        nested_cost = w["foreach"] if depth == 0 else w["foreach_nesting"]
        _add(result, "foreach", nested_cost)
        for inner in getattr(node, "clauses", []):
            _score_node(inner, w, result, depth=depth + 1)
        return

    if isinstance(node, Create):
        _add(result, "create", w["create"])
        return

    if isinstance(node, Merge):
        _add(result, "merge", w["merge"])
        return

    if isinstance(node, Delete):
        _add(result, "delete", w["delete"])
        return

    # Other clause types (Set, Remove, Unwind) get base clause cost only


def _score_expression(
    expr: Any,
    w: dict[str, int],
    result: ComplexityScore,
    *,
    depth: int,
) -> None:
    """Score complexity of an expression (WHERE predicates, RETURN items)."""
    from pycypher.ast_models import (
        CaseExpression,
        Exists,
        FunctionInvocation,
        ListComprehension,
        PatternComprehension,
    )

    if expr is None:
        return

    if isinstance(expr, FunctionInvocation):
        # Check if it's an aggregation function
        agg_names = {
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "collect",
            "stdev",
            "stdevp",
            "percentilecont",
            "percentiledisc",
        }
        if (
            hasattr(expr, "function_name")
            and expr.function_name.lower() in agg_names
        ):
            _add(result, "aggregation", w["aggregation"])

    elif isinstance(expr, Exists):
        _add(result, "exists_subquery", w["exists_subquery"])
        if hasattr(expr, "query") and expr.query is not None:
            _score_node(expr.query, w, result, depth=depth + 1)

    elif isinstance(expr, ListComprehension) or isinstance(
        expr,
        PatternComprehension,
    ):
        _add(result, "list_comprehension", w["list_comprehension"])

    elif isinstance(expr, CaseExpression):
        _add(result, "case_expression", w["case_expression"])

    # Recurse into sub-expressions
    for attr_name in (
        "left",
        "right",
        "expression",
        "arguments",
        "elements",
        "when_clauses",
    ):
        child = getattr(expr, attr_name, None)
        if child is None:
            continue
        if isinstance(child, list):
            for item in child:
                _score_expression(item, w, result, depth=depth)
        else:
            _score_expression(child, w, result, depth=depth)
