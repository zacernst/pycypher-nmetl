"""Query explanation engine — generates EXPLAIN-style text plans.

Extracted from :mod:`pycypher.star` to separate presentation logic
from execution orchestration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

from pycypher.ast_models import ASTConverter

if TYPE_CHECKING:
    from pycypher.cardinality_estimator import CardinalityFeedbackStore
    from pycypher.relational_models import Context


class QueryExplainer:
    """Generate text execution plans without running queries.

    Args:
        context: The PyCypher :class:`Context`.
        cardinality_feedback: Feedback store for correcting heuristic estimates.

    """

    def __init__(
        self,
        context: Context,
        cardinality_feedback: CardinalityFeedbackStore,
    ) -> None:
        self._context = context
        self._cardinality_feedback = cardinality_feedback

    def explain_query(self, query: str) -> str:
        """Return a text execution plan without running the query.

        Parses the Cypher query, walks the AST clauses, and runs the query
        planner to produce cardinality estimates, join strategies, and memory
        projections.  Similar to SQL ``EXPLAIN``.

        Args:
            query: Cypher query string.

        Returns:
            Human-readable execution plan as a multi-line string.

        """
        from pycypher.ast_models import Query, UnionQuery
        from pycypher.query_planner import QueryPlanAnalyzer

        if not query.strip():
            return "Error: empty query"

        converter = ASTConverter()
        parsed = converter.from_cypher(query)

        lines: list[str] = [
            "Execution Plan",
            "=" * 60,
            f"Query: {query.strip()!r}",
            f"Backend: {self._context.backend_name}",
            "",
        ]

        if isinstance(parsed, UnionQuery):
            lines.append(
                f"UNION query with {len(parsed.statements)} sub-queries",
            )
            for i, sub_q in enumerate(parsed.statements):
                lines.append(
                    f"  Sub-query {i + 1}: {len(sub_q.clauses)} clauses",
                )
            return "\n".join(lines)

        if not isinstance(parsed, Query):
            lines.append(f"Unexpected AST type: {type(parsed).__name__}")
            return "\n".join(lines)

        # Clause summary
        lines.append("Clauses:")
        for i, clause in enumerate(parsed.clauses):
            clause_name = type(clause).__name__
            detail = ""
            if hasattr(clause, "optional") and clause.optional:
                detail = " (OPTIONAL)"
            if hasattr(clause, "distinct") and clause.distinct:
                detail += " DISTINCT"
            lines.append(f"  {i + 1}. {clause_name}{detail}")
        lines.append("")

        # Entity/relationship stats
        entities = self._context.entity_mapping.mapping
        rels = self._context.relationship_mapping.mapping
        if entities or rels:
            lines.append("Data Context:")
            for name in sorted(entities):
                src = entities[name].source_obj
                n = len(src) if hasattr(src, "__len__") else "?"
                lines.append(f"  Entity {name}: {n} rows")
            for name in sorted(rels):
                src = rels[name].source_obj
                n = len(src) if hasattr(src, "__len__") else "?"
                lines.append(f"  Relationship {name}: {n} rows")
            lines.append("")

        # Query planner analysis
        analysis = QueryPlanAnalyzer(
            parsed,
            self._context,
            feedback_store=self._cardinality_feedback,
        ).analyze()
        lines.append(
            f"Estimated peak memory: {analysis.estimated_peak_bytes:,} bytes",
        )

        if analysis.clause_cardinalities:
            lines.append("Cardinality estimates:")
            for i, card in enumerate(analysis.clause_cardinalities):
                clause_name = (
                    type(parsed.clauses[i]).__name__
                    if i < len(parsed.clauses)
                    else "?"
                )
                lines.append(
                    f"  Clause {i + 1} ({clause_name}): ~{card:,} rows",
                )

        if analysis.join_plans:
            lines.append("Join strategies:")
            for jp in analysis.join_plans:
                lines.append(
                    f"  {jp.left_name} \u22c8 {jp.right_name}: "
                    f"{jp.strategy.value} (~{jp.estimated_rows:,} rows, "
                    f"{jp.estimated_memory_bytes:,} bytes)",
                )
                if jp.notes:
                    lines.append(f"    Note: {jp.notes}")

        if analysis.has_pushdown_opportunities:
            lines.append("Optimization opportunities:")
            for p in analysis.pushdown_opportunities:
                lines.append(
                    f"  Filter on '{p.variable}' can be pushed before join",
                )

        # Complexity scoring
        from pycypher.query_complexity import score_query

        try:
            complexity = score_query(parsed)
            lines.append(f"Complexity score: {complexity.total}")
            if complexity.breakdown:
                top = sorted(
                    complexity.breakdown.items(),
                    key=lambda x: x[1],
                    reverse=True,
                )[:5]
                detail = ", ".join(f"{k}={v}" for k, v in top)
                lines.append(f"  Top contributors: {detail}")
            for w in complexity.warnings:
                lines.append(f"  Warning: {w}")
        except (AttributeError, TypeError, ValueError, KeyError) as _score_exc:
            LOGGER.debug(
                "PROFILE complexity analysis failed: %s", _score_exc, exc_info=True,
            )

        # Rule-based optimizer analysis
        from pycypher.query_optimizer import QueryOptimizer

        opt_plan = QueryOptimizer.default().optimize(parsed, self._context)
        if opt_plan.applied_rules:
            lines.append("")
            lines.append("Optimizer rules applied:")
            for r in opt_plan.results:
                if r.applied:
                    speedup = (
                        f" ({r.estimated_speedup:.1f}x)"
                        if r.estimated_speedup > 1.0
                        else ""
                    )
                    lines.append(
                        f"  + {r.rule_name}: {r.description}{speedup}"
                    )
            lines.append(
                f"  Total estimated speedup: {opt_plan.total_estimated_speedup:.2f}x",
            )

        return "\n".join(lines)
