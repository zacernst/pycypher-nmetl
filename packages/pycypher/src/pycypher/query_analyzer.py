"""Pre-execution query analysis, planning, and MATCH reordering.

Extracted from :mod:`pycypher.star` to separate query analysis concerns
from the main execution orchestration.  The :class:`QueryAnalyzer` performs:

* Lazy computation graph planning (memory estimation, structural analysis)
* Rule-based query optimization
* Cardinality-based MATCH clause reordering
* LIMIT pushdown hint extraction
* Memory budget enforcement
* Cardinality feedback recording
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.cardinality_estimator import CardinalityFeedbackStore
    from pycypher.frame_joiner import FrameJoiner
    from pycypher.relational_models import Context


class QueryAnalyzer:
    """Pre-execution query analysis and optimization.

    Encapsulates all planning logic that runs before clause-by-clause
    execution: computation graph building, optimizer rules, cardinality
    estimation, memory budget checks, and MATCH reordering.

    Args:
        context: The PyCypher :class:`Context` for entity/relationship stats.
        cardinality_feedback: Feedback store for correcting heuristic estimates.
        frame_joiner: FrameJoiner for passing pre-computed join plans.
        agg_planner: AggregationPlanner for aggregation detection in LIMIT pushdown.

    """

    def __init__(
        self,
        context: Context,
        cardinality_feedback: CardinalityFeedbackStore,
        frame_joiner: FrameJoiner,
        agg_planner: Any,
    ) -> None:
        self._context = context
        self._cardinality_feedback = cardinality_feedback
        self._frame_joiner = frame_joiner
        self._agg_planner = agg_planner

        # Populated by analyze_and_plan()
        self.last_plan_time_ms: float = 0.0
        self.last_estimated_memory_bytes: int = 0
        self.last_optimization_plan: Any = None
        self.last_analysis: Any = None

    def plan_query(self, query: Any) -> dict[str, Any]:
        """Build a computation graph from the query AST and run optimisation passes.

        Returns execution hints derived from the lazy evaluation optimizer's
        analysis of the query structure.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            A dict with keys: ``estimated_memory_bytes``, ``node_count``,
            ``has_filter``, ``has_join``, ``optimized_graph``.

        """
        from pycypher.lazy_eval import (
            OpType,
            build_computation_graph,
            estimate_memory,
            fuse_filters,
            push_filters_down,
        )

        graph = build_computation_graph(query)

        # Run optimisation passes
        optimized = fuse_filters(graph)
        optimized = push_filters_down(optimized)

        mem_estimate = estimate_memory(optimized)

        has_filter = any(
            n.op_type == OpType.FILTER for n in optimized.nodes.values()
        )
        has_join = any(
            n.op_type == OpType.JOIN for n in optimized.nodes.values()
        )

        return {
            "estimated_memory_bytes": mem_estimate,
            "node_count": len(optimized.nodes),
            "has_filter": has_filter,
            "has_join": has_join,
            "optimized_graph": optimized,
        }

    def extract_limit_hint(self, query: Any) -> int | None:
        """Extract a LIMIT value for pushdown if the query pattern is safe.

        Returns the integer LIMIT value when the query has the simple pattern
        ``MATCH ... RETURN ... LIMIT N`` (no aggregation, no DISTINCT, no
        ORDER BY, no SKIP, no WITH).  Returns ``None`` when pushdown is unsafe.

        Args:
            query: Parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            Integer limit for pushdown, or ``None`` if pushdown is unsafe.

        """
        from pycypher.ast_models import Match, Return, With

        clauses = query.clauses
        if not clauses:
            return None

        match_count = sum(1 for c in clauses if isinstance(c, Match))
        with_count = sum(1 for c in clauses if isinstance(c, With))
        return_clauses = [c for c in clauses if isinstance(c, Return)]

        if match_count != 1 or with_count > 0 or len(return_clauses) != 1:
            return None

        ret = return_clauses[0]

        if ret.distinct or ret.order_by or ret.skip is not None:
            return None

        if ret.limit is None:
            return None

        for item in ret.items:
            if item.expression is not None and self._agg_planner.contains_aggregation(
                item.expression,
            ):
                return None

        limit_val = ret.limit
        if isinstance(limit_val, int):
            return limit_val

        return None

    def record_cardinality_feedback(self, actual_rows: int) -> None:
        """Record actual row count into the cardinality feedback store.

        Uses the last analysis result to compare estimated vs actual
        cardinality per entity type, building up correction factors for
        future queries.
        """
        analysis = self.last_analysis
        if analysis is None:
            return

        if analysis.clause_cardinalities:
            final_estimate = analysis.clause_cardinalities[-1]
            entity_types_seen: set[str] = set()
            for jp in analysis.join_plans:
                entity_types_seen.add(jp.left_name)
                entity_types_seen.add(jp.right_name)
            for et in entity_types_seen:
                self._cardinality_feedback.record(
                    et,
                    final_estimate,
                    actual_rows,
                )

        self.last_analysis = None

    def analyze_and_plan(self, query: Any) -> int | None:
        """Run query planning, memory budget enforcement, and LIMIT pushdown.

        Performs all pre-execution analysis:

        * Builds a lazy computation graph for memory estimates.
        * Runs the rule-based :class:`QueryOptimizer`.
        * Runs :class:`QueryPlanAnalyzer` for cardinality and join strategies.
        * Enforces the memory budget.
        * Extracts a LIMIT pushdown hint when safe.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            An optional row-limit hint for MATCH pushdown, or ``None``.

        Raises:
            QueryMemoryBudgetError: If an explicit memory budget is exceeded.

        """
        # --- Lazy evaluation planning phase ---
        _plan_t0 = time.perf_counter()
        _plan_hints = self.plan_query(query)
        _plan_elapsed_ms = (time.perf_counter() - _plan_t0) * 1000.0
        self.last_plan_time_ms = _plan_elapsed_ms
        self.last_estimated_memory_bytes = _plan_hints.get(
            "estimated_memory_bytes",
            0,
        )
        LOGGER.debug(
            "query plan: nodes=%d  memory_est=%d bytes  has_filter=%s  has_join=%s",
            _plan_hints["node_count"],
            _plan_hints["estimated_memory_bytes"],
            _plan_hints["has_filter"],
            _plan_hints["has_join"],
        )

        # --- Rule-based query optimizer ---
        from pycypher.query_optimizer import QueryOptimizer

        _opt_plan = QueryOptimizer.default().optimize(query, self._context)
        self.last_optimization_plan = _opt_plan
        if _opt_plan.applied_rules:
            LOGGER.debug(
                "optimizer: %d rule(s) applied (%s), estimated speedup %.2fx in %.2fms",
                len(_opt_plan.applied_rules),
                ", ".join(_opt_plan.applied_rules),
                _opt_plan.total_estimated_speedup,
                _opt_plan.elapsed_ms,
            )

        # --- Backend re-evaluation using optimizer hints ---
        from pycypher.backend_engine import select_backend_for_query

        _new_backend = select_backend_for_query(
            current_backend=self._context.backend,
            optimization_hints=_opt_plan.hints,
            estimated_rows=0,
        )
        if _new_backend is not None:
            LOGGER.info(
                "backend switch: %s → %s (optimizer-driven)",
                self._context.backend.name,
                _new_backend.name,
            )
            self._context._backend = _new_backend

        # --- Query planner analysis ---
        from pycypher.query_planner import QueryPlanAnalyzer

        _analysis = QueryPlanAnalyzer(
            query,
            self._context,
            feedback_store=self._cardinality_feedback,
        ).analyze()
        self.last_analysis = _analysis
        if _analysis.join_plans:
            for _jp in _analysis.join_plans:
                LOGGER.debug(
                    "query planner: join %s ⋈ %s → %s (%s rows, %s bytes)  %s",
                    _jp.left_name,
                    _jp.right_name,
                    _jp.strategy.value,
                    f"{_jp.estimated_rows:,}",
                    f"{_jp.estimated_memory_bytes:,}",
                    _jp.notes,
                )
        if _analysis.has_pushdown_opportunities:
            for _pd_opp in _analysis.pushdown_opportunities:
                LOGGER.debug(
                    "query planner: pushdown opportunity on '%s': %s",
                    _pd_opp.variable,
                    _pd_opp.predicate_summary,
                )

        # Pass pre-computed join plans to FrameJoiner
        if _analysis.join_plans:
            self._frame_joiner.set_join_plans(_analysis.join_plans)

        # Memory budget enforcement.
        _budget = (
            self._context._memory_budget_bytes
            if self._context._memory_budget_bytes is not None
            else 2 * 1024 * 1024 * 1024
        )
        if _analysis.exceeds_budget(budget_bytes=_budget):
            if self._context._memory_budget_bytes is not None:
                from pycypher.exceptions import QueryMemoryBudgetError

                raise QueryMemoryBudgetError(
                    estimated_bytes=_analysis.estimated_peak_bytes,
                    budget_bytes=_budget,
                )
            LOGGER.warning(
                "query planner: estimated peak memory %s bytes exceeds 2 GB budget; "
                "consider adding LIMIT or narrowing the MATCH pattern",
                f"{_analysis.estimated_peak_bytes:,}",
            )

        # --- MATCH clause reordering based on cardinality estimates ---
        self.apply_match_reordering(query)

        # --- LIMIT pushdown hint ---
        _opt_limit = _opt_plan.hints.get("limit_pushdown_value")
        _limit_hint = _opt_limit if isinstance(_opt_limit, int) else None
        if _limit_hint is None:
            _limit_hint = self.extract_limit_hint(query)
        if _limit_hint is not None:
            LOGGER.debug(
                "LIMIT pushdown hint: %d rows",
                _limit_hint,
            )
        return _limit_hint

    def apply_match_reordering(self, query: Any) -> None:
        """Reorder consecutive MATCH clauses by estimated cardinality.

        Processes MATCH clauses smallest-first to minimize intermediate
        result sizes and reduce cross-join explosion risk.  Only reorders
        *consecutive* MATCH runs — never moves a MATCH past a WITH, RETURN,
        SET, or other clause boundary.

        Mutates ``query.clauses`` in place.

        """
        from pycypher.ast_models import Match
        from pycypher.query_optimizer import JoinReorderingRule

        clauses = query.clauses
        if len(clauses) < 2:
            return

        i = 0
        reordered = False
        while i < len(clauses):
            if isinstance(clauses[i], Match) and not getattr(
                clauses[i],
                "optional",
                False,
            ):
                run_start = i
                while (
                    i < len(clauses)
                    and isinstance(clauses[i], Match)
                    and not getattr(clauses[i], "optional", False)
                ):
                    i += 1
                run_end = i

                if run_end - run_start >= 2:
                    run = clauses[run_start:run_end]

                    has_shortest_path = any(
                        getattr(p, "shortest_path_mode", "none") != "none"
                        for m in run
                        for p in getattr(m.pattern, "paths", [])
                    )
                    if has_shortest_path:
                        LOGGER.debug(
                            "Skipping MATCH reordering: run contains "
                            "shortestPath / allShortestPaths pattern",
                        )
                        continue

                    def _defined_vars(m: Match) -> set[str]:
                        """Variable names introduced by a MATCH pattern."""
                        names: set[str] = set()
                        for path in getattr(m.pattern, "paths", []):
                            for el in getattr(path, "elements", []):
                                v = getattr(el, "variable", None)
                                if v and hasattr(v, "name"):
                                    names.add(v.name)
                        return names

                    def _referenced_vars(expr: Any) -> set[str]:
                        from pycypher.ast_models import (
                            ASTNode,
                            extract_referenced_variables,
                        )

                        if expr is None:
                            return set()
                        if isinstance(expr, ASTNode):
                            return extract_referenced_variables(expr)
                        return set()

                    per_match_vars = [_defined_vars(m) for m in run]
                    all_vars = set().union(*per_match_vars)
                    has_cross_ref = False
                    for idx, match in enumerate(run):
                        if getattr(match, "where", None) is not None:
                            where_refs = _referenced_vars(match.where)
                            other_vars = all_vars - per_match_vars[idx]
                            if where_refs & other_vars:
                                has_cross_ref = True
                                break

                    if has_cross_ref:
                        LOGGER.debug(
                            "Skipping MATCH reordering: WHERE clause has "
                            "cross-MATCH variable references",
                        )
                        continue

                    estimates = []
                    for idx, match in enumerate(run):
                        est = JoinReorderingRule._estimate_match_cardinality(
                            match,
                            self._context,
                        )
                        estimates.append((idx, est))

                    sorted_est = sorted(estimates, key=lambda x: x[1])
                    optimal_order = [e[0] for e in sorted_est]
                    current_order = list(range(len(run)))

                    if optimal_order != current_order:
                        reordered_run = [run[j] for j in optimal_order]
                        clauses[run_start:run_end] = reordered_run
                        reordered = True
                        LOGGER.info(
                            "Reordered %d MATCH clauses by cardinality: %s → %s "
                            "(estimates: %s)",
                            len(run),
                            current_order,
                            optimal_order,
                            {i: c for i, c in estimates},
                        )
            else:
                i += 1

        if not reordered:
            LOGGER.debug("No MATCH clause reordering needed")
