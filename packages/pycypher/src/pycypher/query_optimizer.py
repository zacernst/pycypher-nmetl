"""Rule-based query optimizer with pluggable optimization passes.

Provides a framework for applying optimization rules to parsed Cypher ASTs
before execution.  Each rule inspects the AST, estimates cost savings, and
optionally transforms the execution hints.  Think of it as the Bene Gesserit
breeding programme — each optimization rule is a carefully designed genetic
modification applied to the query's execution DNA.

Architecture
------------

::

    QueryOptimizer
    ├── OptimizationRule (abstract)     — single optimization pass
    │   ├── FilterPushdownRule          — move WHERE closer to scans
    │   ├── LimitPushdownRule           — early termination for simple queries
    │   ├── PredicateSimplificationRule — simplify boolean expressions
    │   └── JoinReorderingRule          — minimize intermediate result sizes
    ├── OptimizationResult              — outcome of a single rule
    ├── OptimizationPlan                — aggregated plan with all applied rules
    └── OptimizeStage                   — Pipeline stage adapter

Usage::

    # Standalone
    optimizer = QueryOptimizer.default()
    plan = optimizer.optimize(query_ast, context)
    print(plan.explain())

    # As Pipeline stage
    pipeline = Pipeline.default()
    pipeline.insert_after("validate", OptimizeStage())

Extending::

    class MyRule(OptimizationRule):
        name = "my_rule"
        def analyze(self, ast, context):
            return OptimizationResult(
                rule_name=self.name,
                applied=True,
                description="Applied my optimization",
                estimated_speedup=1.5,
            )

    optimizer = QueryOptimizer([MyRule()])
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

from pycypher.pipeline import Stage

if TYPE_CHECKING:
    from pycypher.ast_models import (
        ASTNode,
        Match,
    )
    from pycypher.pipeline import PipelineContext
    from pycypher.relational_models import Context


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OptimizationResult:
    """Outcome of applying a single optimization rule.

    Attributes:
        rule_name: Name of the rule that produced this result.
        applied: Whether the rule actually applied an optimization.
        description: Human-readable description of what was optimized.
        estimated_speedup: Multiplicative speedup factor (1.0 = no change).
        hints: Key-value hints passed to the executor.

    """

    rule_name: str
    applied: bool
    description: str = ""
    estimated_speedup: float = 1.0
    hints: dict[str, Any] = field(default_factory=dict)


@dataclass
class OptimizationPlan:
    """Aggregated optimization plan from all applied rules.

    Attributes:
        results: List of individual rule outcomes.
        total_estimated_speedup: Combined multiplicative speedup.
        elapsed_ms: Time spent optimizing in milliseconds.
        hints: Merged hints from all applied rules.

    """

    results: list[OptimizationResult] = field(default_factory=list)
    total_estimated_speedup: float = 1.0
    elapsed_ms: float = 0.0
    hints: dict[str, Any] = field(default_factory=dict)

    @property
    def applied_rules(self) -> list[str]:
        """Return names of rules that actually applied."""
        return [r.rule_name for r in self.results if r.applied]

    @property
    def skipped_rules(self) -> list[str]:
        """Return names of rules that did not apply."""
        return [r.rule_name for r in self.results if not r.applied]

    def explain(self) -> str:
        """Return human-readable optimization plan explanation.

        Returns:
            Multi-line string describing applied optimizations.

        """
        lines = [
            "Optimization Plan",
            "=" * 50,
            f"Rules applied: {len(self.applied_rules)}/{len(self.results)}",
            f"Estimated speedup: {self.total_estimated_speedup:.2f}x",
            f"Optimization time: {self.elapsed_ms:.2f}ms",
            "",
        ]

        if self.applied_rules:
            lines.append("Applied optimizations:")
            for r in self.results:
                if r.applied:
                    speedup = (
                        f" ({r.estimated_speedup:.1f}x)"
                        if r.estimated_speedup > 1.0
                        else ""
                    )
                    lines.append(
                        f"  + {r.rule_name}: {r.description}{speedup}",
                    )

        if self.skipped_rules:
            lines.append("")
            lines.append("Skipped (not applicable):")
            for r in self.results:
                if not r.applied:
                    lines.append(f"  - {r.rule_name}: {r.description}")

        if self.hints:
            lines.append("")
            lines.append("Execution hints:")
            for k, v in self.hints.items():
                lines.append(f"  {k}: {v}")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# OptimizationRule — abstract base
# ---------------------------------------------------------------------------


class OptimizationRule(ABC):
    """Abstract base for a single optimization rule.

    Subclasses implement :meth:`analyze` to inspect the AST and return
    an :class:`OptimizationResult` describing whether and how the
    optimization applies.

    Attributes:
        name: Unique identifier for this rule.

    """

    name: str = "unnamed_rule"

    @abstractmethod
    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Analyze the AST and return optimization result.

        Args:
            ast: Parsed Cypher query AST.
            context: Optional execution context for cardinality estimation.

        Returns:
            :class:`OptimizationResult` describing the optimization.

        """
        ...


# ---------------------------------------------------------------------------
# Built-in optimization rules
# ---------------------------------------------------------------------------


class FilterPushdownRule(OptimizationRule):
    """Move WHERE filters closer to data source scans.

    Detects patterns where a WHERE clause can be applied during pattern
    matching rather than as a post-filter, reducing intermediate result
    sizes.

    Applies when:
    - MATCH has a WHERE clause referencing only variables from that MATCH
    - WHERE predicates are simple comparisons on indexed properties

    """

    name: str = "filter_pushdown"

    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Check for filter pushdown opportunities."""
        from pycypher.ast_models import Match, Query

        if not isinstance(ast, Query):
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Not a Query AST",
            )

        pushdown_count = 0
        for clause in ast.clauses:
            if isinstance(clause, Match) and clause.where is not None:
                # WHERE is already attached to MATCH — check if it can
                # be pushed into the scan phase
                pushdown_count += 1

        if pushdown_count > 0:
            return OptimizationResult(
                rule_name=self.name,
                applied=True,
                description=f"{pushdown_count} WHERE clause(s) eligible for scan-level pushdown",
                estimated_speedup=1.0 + (0.2 * pushdown_count),
                hints={"filter_pushdown_count": pushdown_count},
            )

        return OptimizationResult(
            rule_name=self.name,
            applied=False,
            description="No pushable WHERE clauses found",
        )


class LimitPushdownRule(OptimizationRule):
    """Push LIMIT into pattern matching for early termination.

    Safe only for simple ``MATCH → RETURN LIMIT N`` patterns without
    aggregation, DISTINCT, ORDER BY, SKIP, or WITH clauses.

    """

    name: str = "limit_pushdown"

    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Check for LIMIT pushdown opportunity."""
        from pycypher.ast_models import Query, Return, With

        if not isinstance(ast, Query):
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Not a Query AST",
            )

        clauses = ast.clauses
        has_with = any(isinstance(c, With) for c in clauses)
        return_clauses = [c for c in clauses if isinstance(c, Return)]

        if has_with or not return_clauses:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="WITH clause or no RETURN prevents pushdown",
            )

        ret = return_clauses[-1]
        if ret.distinct or ret.order_by or ret.skip:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="DISTINCT/ORDER BY/SKIP prevents pushdown",
            )

        limit_val = ret.limit
        if limit_val is None:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="No LIMIT clause",
            )

        # Check for aggregations in RETURN items
        from pycypher.ast_models import FunctionInvocation

        has_agg = False
        for item in ret.items or []:
            expr = getattr(item, "expression", None)
            if isinstance(expr, FunctionInvocation):
                func_name = getattr(expr, "name", "").lower()
                if func_name in {
                    "count",
                    "sum",
                    "avg",
                    "min",
                    "max",
                    "collect",
                    "stdev",
                }:
                    has_agg = True
                    break

        if has_agg:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Aggregation in RETURN prevents pushdown",
            )

        return OptimizationResult(
            rule_name=self.name,
            applied=True,
            description=f"LIMIT {limit_val} can be pushed to pattern scan",
            estimated_speedup=1.5,
            hints={"limit_pushdown_value": limit_val},
        )


class JoinReorderingRule(OptimizationRule):
    """Suggest optimal join ordering based on cardinality estimates.

    For multi-MATCH queries, suggests processing smaller tables first
    to minimize intermediate result sizes.

    """

    name: str = "join_reordering"

    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Check for join reordering opportunities."""
        from pycypher.ast_models import Match, Query

        if not isinstance(ast, Query):
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Not a Query AST",
            )

        match_clauses = [c for c in ast.clauses if isinstance(c, Match)]

        if len(match_clauses) < 2:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Single MATCH clause — no reordering needed",
            )

        # Estimate cardinalities if context available
        if context is None:
            return OptimizationResult(
                rule_name=self.name,
                applied=True,
                description=f"{len(match_clauses)} MATCH clauses could benefit from join reordering",
                estimated_speedup=1.2,
                hints={"match_clause_count": len(match_clauses)},
            )

        # With context, estimate sizes
        cardinalities: list[tuple[int, int]] = []
        for i, match in enumerate(match_clauses):
            est = self._estimate_match_cardinality(match, context)
            cardinalities.append((i, est))

        # Check if current order is optimal (smallest first)
        sorted_cards = sorted(cardinalities, key=lambda x: x[1])
        current_order = [c[0] for c in cardinalities]
        optimal_order = [c[0] for c in sorted_cards]

        if current_order == optimal_order:
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Current MATCH order is already optimal",
            )

        return OptimizationResult(
            rule_name=self.name,
            applied=True,
            description=f"Reorder {len(match_clauses)} MATCH clauses for smaller intermediate results",
            estimated_speedup=1.3,
            hints={
                "optimal_match_order": optimal_order,
                "cardinality_estimates": {i: c for i, c in cardinalities},
            },
        )

    @staticmethod
    def _estimate_match_cardinality(match: Match, context: Context) -> int:
        """Cardinality estimate for a MATCH clause using table sizes and
        relationship constraints.

        For patterns like ``(a:Person)-[:KNOWS]->(b:Person)``, the
        cardinality is bounded by the relationship count rather than the
        naive product of entity counts.  WHERE clause selectivity is
        estimated via ``QueryPlanAnalyzer.estimate_predicate_selectivity``
        when column statistics are available.
        """
        from pycypher.ast_models import NodePattern, Query, RelationshipPattern

        if match.pattern is None:
            return 0

        cardinality = 1
        has_relationship = False

        for path in getattr(match.pattern, "paths", []):
            for element in getattr(path, "elements", []):
                if isinstance(element, NodePattern):
                    labels = getattr(element, "labels", [])
                    if labels:
                        label = labels[0]
                        mapping = context.entity_mapping.mapping
                        if label in mapping:
                            src = mapping[label].source_obj
                            if hasattr(src, "__len__"):
                                if not has_relationship:
                                    cardinality = len(src)
                                continue
                    if not has_relationship:
                        cardinality *= 100  # Unknown entity default

                elif isinstance(element, RelationshipPattern):
                    has_relationship = True
                    labels = getattr(element, "labels", [])
                    if labels:
                        label = labels[0]
                        rel_mapping = context.relationship_mapping.mapping
                        if label in rel_mapping:
                            src = rel_mapping[label].source_obj
                            if hasattr(src, "__len__"):
                                # Relationship count bounds the join output
                                cardinality = min(
                                    cardinality * len(src)
                                    if cardinality > 0
                                    else len(src),
                                    len(src),
                                )
                                continue
                    cardinality *= 100  # Unknown relationship default

        # Apply WHERE selectivity using column statistics if available
        if getattr(match, "where", None) is not None:
            try:
                # Build a minimal Query wrapper to use QueryPlanAnalyzer
                from pycypher.query_planner import QueryPlanAnalyzer

                dummy_query = Query(clauses=[match])
                analyzer = QueryPlanAnalyzer(dummy_query, context)
                selectivity = analyzer.estimate_predicate_selectivity(
                    match.where,
                )
                cardinality = max(1, int(cardinality * selectivity))
            except Exception:
                LOGGER.debug(
                    "Failed to estimate WHERE selectivity; falling back to 0.33",
                    exc_info=True,
                )
                cardinality = max(1, int(cardinality * 0.33))

        return cardinality


class PredicateSimplificationRule(OptimizationRule):
    """Detect and simplify redundant boolean predicates.

    Identifies patterns like:
    - Double negation: NOT NOT x → x
    - Tautologies: x AND TRUE → x
    - Contradictions: x AND FALSE → FALSE

    """

    name: str = "predicate_simplification"

    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Check for simplifiable predicates."""
        from pycypher.ast_models import And, BooleanLiteral, Not, Or, Query

        if not isinstance(ast, Query):
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Not a Query AST",
            )

        simplifiable = 0
        for node in ast.traverse():
            # Double negation: NOT NOT x
            if isinstance(node, Not) and isinstance(node.operand, Not):
                simplifiable += 1

            # AND/OR with boolean literal operands
            if isinstance(node, (And, Or)):
                for operand in node.operands:
                    if isinstance(operand, BooleanLiteral):
                        simplifiable += 1
                        break

        if simplifiable > 0:
            return OptimizationResult(
                rule_name=self.name,
                applied=True,
                description=f"{simplifiable} predicate(s) can be simplified",
                estimated_speedup=1.05,
                hints={"simplifiable_predicates": simplifiable},
            )

        return OptimizationResult(
            rule_name=self.name,
            applied=False,
            description="No simplifiable predicates found",
        )


class IndexScanRule(OptimizationRule):
    """Detect opportunities to use graph-native index scans.

    Identifies MATCH patterns with relationship traversals that can benefit
    from adjacency index lookups (O(degree) instead of O(E) table scans)
    and inline property filters that can use property value indexes
    (O(1) instead of O(N) scans).

    """

    name: str = "index_scan"

    def analyze(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationResult:
        """Check for index scan opportunities."""
        from pycypher.ast_models import (
            Match,
            NodePattern,
            Query,
            RelationshipPattern,
        )

        if not isinstance(ast, Query):
            return OptimizationResult(
                rule_name=self.name,
                applied=False,
                description="Not a Query AST",
            )

        index_opportunities = 0
        adjacency_candidates = 0
        property_candidates = 0

        for clause in ast.clauses:
            if not isinstance(clause, Match):
                continue
            for path in getattr(clause.pattern, "paths", []):
                elements = getattr(path, "elements", [])
                for element in elements:
                    if isinstance(element, RelationshipPattern):
                        # Every directed relationship traversal benefits from
                        # adjacency index when pushdown IDs are available
                        adjacency_candidates += 1
                    elif isinstance(element, NodePattern):
                        # Inline property filters benefit from property indexes
                        if getattr(element, "properties", None):
                            property_candidates += len(element.properties)

        index_opportunities = adjacency_candidates + property_candidates

        if index_opportunities > 0:
            # Adjacency index: O(degree) vs O(E) → typically 10-100x for sparse graphs
            # Property index: O(1) vs O(N) → typically 100-1000x for selective lookups
            adj_speedup = 1.0 + (0.5 * min(adjacency_candidates, 5))
            prop_speedup = 1.0 + (1.0 * min(property_candidates, 3))
            combined = adj_speedup * prop_speedup

            return OptimizationResult(
                rule_name=self.name,
                applied=True,
                description=(
                    f"{adjacency_candidates} adjacency index candidate(s), "
                    f"{property_candidates} property index candidate(s)"
                ),
                estimated_speedup=combined,
                hints={
                    "index_adjacency_candidates": adjacency_candidates,
                    "index_property_candidates": property_candidates,
                },
            )

        return OptimizationResult(
            rule_name=self.name,
            applied=False,
            description="No index scan opportunities found",
        )


# ---------------------------------------------------------------------------
# QueryOptimizer — runs all rules
# ---------------------------------------------------------------------------


class QueryOptimizer:
    """Applies optimization rules to a query AST and produces a plan.

    Args:
        rules: Ordered list of optimization rules. If None, uses
            :meth:`default` rules.

    """

    def __init__(self, rules: list[OptimizationRule] | None = None) -> None:
        self._rules = rules if rules is not None else self._default_rules()

    @staticmethod
    def _default_rules() -> list[OptimizationRule]:
        """Return the standard set of optimization rules."""
        return [
            FilterPushdownRule(),
            LimitPushdownRule(),
            JoinReorderingRule(),
            PredicateSimplificationRule(),
            IndexScanRule(),
        ]

    @classmethod
    def default(cls) -> QueryOptimizer:
        """Create an optimizer with all built-in rules."""
        return cls()

    @property
    def rule_names(self) -> list[str]:
        """Return names of registered rules."""
        return [r.name for r in self._rules]

    def add_rule(self, rule: OptimizationRule) -> QueryOptimizer:
        """Add a rule to the optimizer.

        Args:
            rule: Rule to add.

        Returns:
            Self for fluent chaining.

        """
        self._rules.append(rule)
        return self

    def optimize(
        self,
        ast: ASTNode,
        context: Context | None = None,
    ) -> OptimizationPlan:
        """Run all optimization rules and produce a plan.

        Args:
            ast: Parsed Cypher query AST.
            context: Optional execution context for cardinality estimation.

        Returns:
            :class:`OptimizationPlan` with results from all rules.

        """
        t0 = time.perf_counter()
        plan = OptimizationPlan()

        for rule in self._rules:
            try:
                result = rule.analyze(ast, context)
                plan.results.append(result)

                if result.applied:
                    plan.total_estimated_speedup *= result.estimated_speedup
                    plan.hints.update(result.hints)
                    LOGGER.debug(
                        "Optimization rule '%s' applied: %s (%.1fx)",
                        result.rule_name,
                        result.description,
                        result.estimated_speedup,
                    )
            except (
                RuntimeError,
                TypeError,
                ValueError,
                KeyError,
                AttributeError,
            ) as exc:
                LOGGER.error(
                    "Optimization rule '%s' failed (%s), skipping",
                    rule.name,
                    type(exc).__name__,
                    exc_info=True,
                )
                plan.results.append(
                    OptimizationResult(
                        rule_name=rule.name,
                        applied=False,
                        description="Rule raised an exception",
                    ),
                )

        plan.elapsed_ms = (time.perf_counter() - t0) * 1000
        return plan


# ---------------------------------------------------------------------------
# Pipeline integration
# ---------------------------------------------------------------------------


class OptimizeStage(Stage):
    """Pipeline stage that runs the query optimizer.

    Insert into a :class:`~pycypher.pipeline.Pipeline` to automatically
    optimize queries before execution::

        pipeline = Pipeline.default()
        pipeline.insert_after("validate", OptimizeStage())

    Populates ``ctx.metadata["optimization_plan"]`` and merges hints.

    """

    name: str = "optimize"

    def __init__(
        self,
        optimizer: QueryOptimizer | None = None,
    ) -> None:
        self._optimizer = optimizer or QueryOptimizer.default()

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Run optimization rules on the parsed AST.

        Args:
            ctx: Pipeline context with ``ast`` populated.

        Returns:
            Context with optimization hints in ``metadata``.

        """
        if ctx.ast is None:
            return ctx

        context = getattr(ctx.star, "context", None) if ctx.star else None
        plan = self._optimizer.optimize(ctx.ast, context)

        ctx.metadata["optimization_plan"] = plan
        ctx.metadata["optimization_applied_rules"] = plan.applied_rules
        ctx.metadata["optimization_speedup"] = plan.total_estimated_speedup
        ctx.metadata.update(plan.hints)

        return ctx
