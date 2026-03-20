"""Intelligent query planner for optimised join and aggregation strategies.

The query planner analyses the structure of a query (join cardinalities,
filter selectivity, available indices) and selects optimal execution
strategies.  Think of it as the DHF stack coordinator in *Altered Carbon* —
routing consciousness data through the most efficient neural pathway based
on bandwidth, latency, and sleeve compatibility.

Architecture
------------

::

    QueryPlanner              — low-level strategy selection (join/agg)
    QueryPlanAnalyzer         — AST-aware analysis (cardinality, memory, pushdown)
    ├── JoinStrategy (enum: HASH, BROADCAST, MERGE, NESTED_LOOP)
    ├── JoinPlan (dataclass: left, right, strategy, estimated_cost)
    ├── QueryPlan (dataclass: ordered list of JoinPlans + agg strategy)
    └── AnalysisResult (dataclass: per-clause cardinality, memory, pushdown)

The planner is invoked **before** execution to produce a ``QueryPlan``.
The executor (``star.py``) then follows the plan rather than using a
fixed join order.

Usage::

    # Low-level strategy selection
    planner = QueryPlanner()
    plan = planner.plan_join(left_size=1_000_000, right_size=100, ...)
    # plan.strategy == JoinStrategy.BROADCAST

    # AST-aware analysis
    analyzer = QueryPlanAnalyzer(query_ast, context)
    result = analyzer.analyze()
    # result.clause_cardinalities, result.estimated_peak_bytes, etc.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.ast_models import Query
    from pycypher.relational_models import Context

__all__ = [
    "AggStrategy",
    "JoinPlan",
    "JoinStrategy",
    "QueryPlan",
    "QueryPlanAnalyzer",
    "QueryPlanner",
]


# ---------------------------------------------------------------------------
# Join strategy enumeration
# ---------------------------------------------------------------------------


class JoinStrategy(enum.Enum):
    """Available join algorithms, ordered by memory overhead."""

    #: Hash join — build hash table on the smaller side, probe with the larger.
    #: Best for general-purpose equi-joins.  O(N + M) time, O(min(N,M)) space.
    HASH = "hash"

    #: Broadcast join — replicate the small table to every partition.
    #: Best when one side is orders of magnitude smaller (< 10K rows).
    #: O(N) time, O(small) space.
    BROADCAST = "broadcast"

    #: Merge join — both sides pre-sorted on the join key.
    #: Best when data is already sorted (e.g. from ORDER BY or index).
    #: O(N + M) time, O(1) space.
    MERGE = "merge"

    #: Nested loop — brute-force.  Only for very small datasets or
    #: non-equi joins.  O(N × M) time.
    NESTED_LOOP = "nested_loop"


# ---------------------------------------------------------------------------
# Aggregation strategy
# ---------------------------------------------------------------------------


class AggStrategy(enum.Enum):
    """Available aggregation algorithms."""

    #: Hash aggregation — build hash table on group keys.
    #: Best for moderate cardinality groups.
    HASH_AGG = "hash_agg"

    #: Sort-based aggregation — sort then scan.
    #: Best when data is already sorted on group keys.
    SORT_AGG = "sort_agg"

    #: Streaming aggregation — process chunks, merge partial results.
    #: Best for very large datasets that don't fit in memory.
    STREAMING_AGG = "streaming_agg"


# ---------------------------------------------------------------------------
# Join plan
# ---------------------------------------------------------------------------


@dataclass
class JoinPlan:
    """Describes how a single join operation should be executed.

    Attributes:
        left_name: Identifier for the left table/frame.
        right_name: Identifier for the right table/frame.
        join_key: Column(s) to join on.
        strategy: The selected join algorithm.
        estimated_rows: Estimated output row count.
        estimated_memory_bytes: Estimated peak memory during the join.
        notes: Human-readable explanation of the strategy choice.

    """

    left_name: str
    right_name: str
    join_key: str | list[str]
    strategy: JoinStrategy
    estimated_rows: int = 0
    estimated_memory_bytes: int = 0
    notes: str = ""


@dataclass
class AggPlan:
    """Describes how an aggregation should be executed.

    Attributes:
        strategy: The selected aggregation algorithm.
        group_cardinality: Estimated number of distinct groups.
        estimated_memory_bytes: Estimated peak memory.
        notes: Human-readable explanation.

    """

    strategy: AggStrategy
    group_cardinality: int = 0
    estimated_memory_bytes: int = 0
    notes: str = ""


@dataclass
class QueryPlan:
    """Full execution plan for a query.

    Attributes:
        joins: Ordered list of join plans.
        aggregation: Aggregation plan (if query has GROUP BY / aggregation).
        total_estimated_memory_bytes: Sum of estimated peak memory across all ops.
        warnings: Any optimiser warnings (e.g. potential OOM, cross-join).

    """

    joins: list[JoinPlan] = field(default_factory=list)
    aggregation: AggPlan | None = None
    total_estimated_memory_bytes: int = 0
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# QueryPlanner
# ---------------------------------------------------------------------------


# Tuning constants — consciousness transfer analogy: these are the
# sleeve compatibility thresholds.
_BROADCAST_THRESHOLD: int = 10_000
_MERGE_SORTED_THRESHOLD: float = 0.8  # fraction of data already sorted
_STREAMING_AGG_THRESHOLD: int = 10_000_000
_CROSS_JOIN_WARNING_THRESHOLD: int = 100_000


class QueryPlanner:
    """Analyses query structure and produces optimised execution plans.

    The planner uses simple heuristics today and can be extended with
    cost-based optimisation and statistics collection in future phases.
    """

    def __init__(
        self,
        *,
        memory_budget_bytes: int = 2 * 1024 * 1024 * 1024,  # 2GB default
    ) -> None:
        """Initialize query planner.

        Args:
            memory_budget_bytes: Maximum memory budget for join operations.
                Defaults to 2 GB.
        """
        self._memory_budget = memory_budget_bytes

    def plan_join(
        self,
        *,
        left_name: str,
        right_name: str,
        left_rows: int,
        right_rows: int,
        join_key: str | list[str],
        left_sorted: bool = False,
        right_sorted: bool = False,
        avg_row_bytes: int = 100,
    ) -> JoinPlan:
        """Select the optimal join strategy for two frames.

        Args:
            left_name: Identifier for the left frame.
            right_name: Identifier for the right frame.
            left_rows: Number of rows in the left frame.
            right_rows: Number of rows in the right frame.
            join_key: Column(s) to join on.
            left_sorted: Whether left is sorted on join key.
            right_sorted: Whether right is sorted on join key.
            avg_row_bytes: Average row size for memory estimation.

        Returns:
            A :class:`JoinPlan` with the selected strategy.

        """
        smaller = min(left_rows, right_rows)
        larger = max(left_rows, right_rows)

        # Strategy selection — like choosing the right needle-cast
        # infrastructure based on consciousness payload size.

        if smaller <= _BROADCAST_THRESHOLD:
            # Small-large: replicate the small side.
            estimated_mem = smaller * avg_row_bytes
            return JoinPlan(
                left_name=left_name,
                right_name=right_name,
                join_key=join_key,
                strategy=JoinStrategy.BROADCAST,
                estimated_rows=min(left_rows, right_rows),  # conservative
                estimated_memory_bytes=estimated_mem,
                notes=(
                    f"Broadcast join: smaller side ({smaller:,} rows) below "
                    f"threshold ({_BROADCAST_THRESHOLD:,}). "
                    f"Replicating to avoid hash table overhead."
                ),
            )

        if left_sorted and right_sorted:
            # Both sorted: merge join is optimal.
            estimated_mem = avg_row_bytes * 2  # only 2 row buffers needed
            return JoinPlan(
                left_name=left_name,
                right_name=right_name,
                join_key=join_key,
                strategy=JoinStrategy.MERGE,
                estimated_rows=min(left_rows, right_rows),
                estimated_memory_bytes=estimated_mem,
                notes="Merge join: both sides pre-sorted on join key.",
            )

        # Default: hash join on the smaller side.
        hash_table_bytes = smaller * avg_row_bytes
        estimated_output = min(left_rows, right_rows)  # conservative
        estimated_mem = hash_table_bytes + estimated_output * avg_row_bytes

        if estimated_mem > self._memory_budget:
            # Would exceed budget — warn but still use hash join
            # (future: switch to partitioned hash join)
            return JoinPlan(
                left_name=left_name,
                right_name=right_name,
                join_key=join_key,
                strategy=JoinStrategy.HASH,
                estimated_rows=estimated_output,
                estimated_memory_bytes=estimated_mem,
                notes=(
                    f"Hash join selected but estimated memory "
                    f"({estimated_mem / 1024 / 1024:.0f} MB) exceeds budget "
                    f"({self._memory_budget / 1024 / 1024:.0f} MB). "
                    f"Consider partitioned execution."
                ),
            )

        return JoinPlan(
            left_name=left_name,
            right_name=right_name,
            join_key=join_key,
            strategy=JoinStrategy.HASH,
            estimated_rows=estimated_output,
            estimated_memory_bytes=estimated_mem,
            notes=(
                f"Hash join: building on smaller side ({smaller:,} rows), "
                f"probing with larger ({larger:,} rows)."
            ),
        )

    def plan_aggregation(
        self,
        *,
        input_rows: int,
        group_cardinality: int,
        is_sorted: bool = False,
        avg_row_bytes: int = 100,
    ) -> AggPlan:
        """Select the optimal aggregation strategy.

        Args:
            input_rows: Number of input rows.
            group_cardinality: Estimated number of distinct groups.
            is_sorted: Whether input is sorted on group keys.
            avg_row_bytes: Average row size for memory estimation.

        Returns:
            An :class:`AggPlan` with the selected strategy.

        """
        if is_sorted:
            return AggPlan(
                strategy=AggStrategy.SORT_AGG,
                group_cardinality=group_cardinality,
                estimated_memory_bytes=avg_row_bytes * 2,
                notes="Sort-based aggregation: input already sorted on group keys.",
            )

        if input_rows > _STREAMING_AGG_THRESHOLD:
            chunk_mem = min(input_rows, 1_000_000) * avg_row_bytes
            return AggPlan(
                strategy=AggStrategy.STREAMING_AGG,
                group_cardinality=group_cardinality,
                estimated_memory_bytes=chunk_mem
                + group_cardinality * avg_row_bytes,
                notes=(
                    f"Streaming aggregation: {input_rows:,} rows exceeds "
                    f"threshold ({_STREAMING_AGG_THRESHOLD:,}). Processing in chunks."
                ),
            )

        return AggPlan(
            strategy=AggStrategy.HASH_AGG,
            group_cardinality=group_cardinality,
            estimated_memory_bytes=group_cardinality * avg_row_bytes,
            notes=f"Hash aggregation: {group_cardinality:,} groups fit in memory.",
        )

    def plan_cross_join(
        self,
        *,
        left_name: str,
        right_name: str,
        left_rows: int,
        right_rows: int,
        avg_row_bytes: int = 100,
    ) -> JoinPlan:
        """Plan a cross join with memory safety checks.

        Args:
            left_name: Identifier for the left frame.
            right_name: Identifier for the right frame.
            left_rows: Number of rows in the left frame.
            right_rows: Number of rows in the right frame.
            avg_row_bytes: Average row size.

        Returns:
            A :class:`JoinPlan` for the cross join.

        """
        output_rows = left_rows * right_rows
        estimated_mem = output_rows * avg_row_bytes
        notes = f"Cross join: {left_rows:,} × {right_rows:,} = {output_rows:,} rows."

        plan = JoinPlan(
            left_name=left_name,
            right_name=right_name,
            join_key=[],
            strategy=JoinStrategy.NESTED_LOOP,
            estimated_rows=output_rows,
            estimated_memory_bytes=estimated_mem,
            notes=notes,
        )

        if output_rows > _CROSS_JOIN_WARNING_THRESHOLD:
            plan.notes += (
                f" WARNING: output exceeds {_CROSS_JOIN_WARNING_THRESHOLD:,} rows. "
                f"Consider adding filters or LIMIT."
            )

        return plan

    def estimate_memory(
        self,
        plan: QueryPlan,
    ) -> dict[str, Any]:
        """Summarise memory requirements for a query plan.

        Args:
            plan: The query plan to analyse.

        Returns:
            A dictionary with memory estimates and budget comparison.

        """
        total = plan.total_estimated_memory_bytes
        for join_plan in plan.joins:
            total += join_plan.estimated_memory_bytes
        if plan.aggregation:
            total += plan.aggregation.estimated_memory_bytes

        return {
            "total_estimated_bytes": total,
            "total_estimated_mb": total / (1024 * 1024),
            "budget_bytes": self._memory_budget,
            "budget_mb": self._memory_budget / (1024 * 1024),
            "within_budget": total <= self._memory_budget,
            "utilisation_pct": (total / self._memory_budget) * 100
            if self._memory_budget
            else 0,
        }


# Module-level singleton — avoids per-join instantiation overhead.
_DEFAULT_PLANNER: QueryPlanner = QueryPlanner()


def get_default_planner() -> QueryPlanner:
    """Return the module-level default ``QueryPlanner`` singleton."""
    return _DEFAULT_PLANNER


# ---------------------------------------------------------------------------
# AST-aware analysis data models
# ---------------------------------------------------------------------------

#: Default selectivity factor for WHERE predicates when no statistics are
#: available.  Assumes an equality filter keeps ~33% of rows; inequality
#: keeps ~50%.  The geometric mean is ~0.4, rounded down for safety.
_DEFAULT_FILTER_SELECTIVITY: float = 0.33

#: Average bytes per cell for memory estimation when the actual DataFrame
#: is not available.  This is a conservative estimate for mixed-type columns.
_AVG_BYTES_PER_CELL: int = 64


@dataclass
class PushdownOpportunity:
    """Describes a filter that can be pushed before a join.

    Attributes:
        variable: The Cypher variable whose predicate can be pushed.
        predicate_summary: Human-readable description of the filter.

    """

    variable: str
    predicate_summary: str = ""


@dataclass
class AnalysisResult:
    """Full AST-level analysis of a query.

    Attributes:
        clause_cardinalities: Estimated output rows per clause (index-aligned
            with ``query.clauses``).
        estimated_peak_bytes: Peak memory estimate across all clauses.
        join_plans: Join strategy recommendations (one per relationship hop).
        pushdown_opportunities: Filters that can be applied before joins.
        has_pushdown_opportunities: Convenience flag.

    """

    clause_cardinalities: list[int] = field(default_factory=list)
    estimated_peak_bytes: int = 0
    join_plans: list[JoinPlan] = field(default_factory=list)
    pushdown_opportunities: list[PushdownOpportunity] = field(
        default_factory=list,
    )
    has_pushdown_opportunities: bool = False

    def exceeds_budget(self, *, budget_bytes: int) -> bool:
        """Return True if estimated peak memory exceeds *budget_bytes*."""
        return self.estimated_peak_bytes > budget_bytes

    def summary(self) -> str:
        """Return a human-readable plan summary."""
        lines: list[str] = ["Query Plan Analysis", "=" * 40]

        lines.append(
            f"Estimated peak memory: {self.estimated_peak_bytes:,} bytes",
        )

        for i, card in enumerate(self.clause_cardinalities):
            lines.append(f"  Clause {i}: ~{card:,} rows")

        if self.join_plans:
            lines.append(f"Join plans: {len(self.join_plans)}")
            for jp in self.join_plans:
                lines.append(
                    f"  {jp.left_name} ⋈ {jp.right_name}: "
                    f"{jp.strategy.value} ({jp.estimated_rows:,} rows)",
                )

        if self.has_pushdown_opportunities:
            lines.append("Pushdown opportunities:")
            for p in self.pushdown_opportunities:
                lines.append(
                    f"  Filter on '{p.variable}' can be applied early",
                )

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# QueryPlanAnalyzer — AST-aware analysis
# ---------------------------------------------------------------------------


class QueryPlanAnalyzer:
    """Walks a Cypher AST and Context to produce cardinality, memory, and
    join strategy analysis.

    This is the Phase 2.1 integration point — called from
    ``Star._execute_query_binding_frame_inner()`` before execution begins
    to produce an :class:`AnalysisResult` that guides strategy selection.

    Args:
        query: Parsed Cypher query AST.
        context: The execution context with entity/relationship tables.

    """

    def __init__(self, query: Query, context: Context) -> None:
        """Initialize query plan analyzer.

        Args:
            query: Parsed Cypher query AST to analyze.
            context: Execution context with entity/relationship tables for
                cardinality estimation.
        """
        self.query = query
        self.context = context
        self._planner = get_default_planner()

    def entity_row_count(self, entity_type: str) -> int:
        """Return the row count for an entity type, or 0 if unknown."""
        mapping = self.context.entity_mapping.mapping
        if entity_type in mapping:
            src = mapping[entity_type].source_obj
            if hasattr(src, "__len__"):
                return len(src)
            LOGGER.warning(
                "Entity source for %r does not support size estimation; "
                "cardinality defaults to 0 (query plan may be suboptimal). "
                "Use a pandas DataFrame or other sized collection as the data source.",
                entity_type,
            )
            return 0
        return 0

    def relationship_row_count(self, rel_type: str) -> int:
        """Return the row count for a relationship type, or 0 if unknown."""
        mapping = self.context.relationship_mapping.mapping
        if rel_type in mapping:
            src = mapping[rel_type].source_obj
            if hasattr(src, "__len__"):
                return len(src)
            LOGGER.warning(
                "Relationship source for %r does not support size estimation; "
                "cardinality defaults to 0 (query plan may be suboptimal). "
                "Use a pandas DataFrame or other sized collection as the data source.",
                rel_type,
            )
            return 0
        return 0

    def analyze(self) -> AnalysisResult:
        """Walk the AST and produce a full analysis."""
        from pycypher.ast_models import Match, Return, With

        result = AnalysisResult()
        current_cardinality = 0

        for clause in self.query.clauses:
            if isinstance(clause, Match):
                card, joins, pushdowns = self._analyze_match(clause)
                current_cardinality = card
                result.join_plans.extend(joins)
                result.pushdown_opportunities.extend(pushdowns)

            elif isinstance(clause, (Return, With)):
                # Projection preserves cardinality unless LIMIT/SKIP/DISTINCT
                limit_val = getattr(clause, "limit", None)
                skip_val = getattr(clause, "skip", None)
                distinct = getattr(clause, "distinct", False)

                if limit_val is not None:
                    from pycypher.ast_models import IntegerLiteral

                    if isinstance(limit_val, IntegerLiteral):
                        current_cardinality = min(
                            current_cardinality,
                            limit_val.value,
                        )
                if skip_val is not None:
                    from pycypher.ast_models import IntegerLiteral

                    if isinstance(skip_val, IntegerLiteral):
                        current_cardinality = max(
                            0,
                            current_cardinality - skip_val.value,
                        )
                if distinct:
                    # Conservative: assume DISTINCT keeps 80% of rows
                    current_cardinality = int(current_cardinality * 0.8)

            else:
                # Other clauses (SET, CREATE, etc.) — cardinality unchanged
                pass

            result.clause_cardinalities.append(current_cardinality)

        # Memory estimation: sum of per-clause intermediate frame sizes
        n_variables = self._count_variables()
        peak_bytes = 0
        for card in result.clause_cardinalities:
            clause_bytes = card * n_variables * _AVG_BYTES_PER_CELL
            peak_bytes = max(peak_bytes, clause_bytes)
        # Add join overhead
        for jp in result.join_plans:
            peak_bytes += jp.estimated_memory_bytes
        result.estimated_peak_bytes = max(peak_bytes, 1)  # at least 1 byte

        result.has_pushdown_opportunities = (
            len(result.pushdown_opportunities) > 0
        )

        return result

    def _analyze_match(
        self,
        clause: Any,
    ) -> tuple[int, list[JoinPlan], list[PushdownOpportunity]]:
        """Analyze a MATCH clause for cardinality, joins, and pushdown.

        Returns:
            (estimated_cardinality, join_plans, pushdown_opportunities)

        """
        from pycypher.ast_models import NodePattern, RelationshipPattern

        joins: list[JoinPlan] = []
        pushdowns: list[PushdownOpportunity] = []
        cardinality = 1

        if clause.pattern is None:
            return cardinality, joins, pushdowns

        # Walk each path in the pattern
        has_relationship = False
        entity_types: list[str] = []
        rel_types: list[str] = []
        node_variables: list[str] = []

        for path in clause.pattern.paths:
            for element in path.elements:
                if isinstance(element, NodePattern):
                    if element.labels:
                        label = element.labels[0]
                        entity_types.append(label)
                        entity_count = self.entity_row_count(label)
                        if entity_count > 0:
                            if not has_relationship:
                                # First node scan — cardinality = entity count
                                cardinality = entity_count
                    if element.variable is not None:
                        node_variables.append(element.variable.name)

                elif isinstance(element, RelationshipPattern):
                    has_relationship = True
                    if element.labels:
                        rel_type = element.labels[0]
                        rel_types.append(rel_type)
                        rel_count = self.relationship_row_count(rel_type)
                        # Cardinality bounded by relationship count
                        cardinality = min(
                            cardinality * rel_count
                            if cardinality > 0
                            else rel_count,
                            rel_count,
                        )

        # Generate join plans for relationship hops
        if has_relationship and entity_types and rel_types:
            for rel_type in rel_types:
                rel_rows = self.relationship_row_count(rel_type)
                entity_rows = max(
                    self.entity_row_count(et) for et in entity_types
                )
                jp = self._planner.plan_join(
                    left_name=entity_types[0],
                    right_name=rel_type,
                    left_rows=entity_rows,
                    right_rows=rel_rows,
                    join_key="__ID__",
                )
                joins.append(jp)

        # Apply filter selectivity
        if clause.where is not None:
            cardinality = max(
                1,
                int(cardinality * _DEFAULT_FILTER_SELECTIVITY),
            )

            # Detect pushdown opportunities: WHERE predicates that reference
            # only one node variable can be pushed before the join.
            if has_relationship:
                where_vars = self._extract_variables(clause.where)
                for var in where_vars:
                    if var in node_variables:
                        pushdowns.append(
                            PushdownOpportunity(
                                variable=var,
                                predicate_summary=f"Filter on {var} before join",
                            ),
                        )

        return cardinality, joins, pushdowns

    def _extract_variables(self, expr: Any) -> set[str]:
        """Extract all variable names referenced in an expression."""
        from pycypher.ast_models import PropertyLookup, Variable

        variables: set[str] = set()
        if isinstance(expr, Variable):
            variables.add(expr.name)
        elif isinstance(expr, PropertyLookup):
            if isinstance(expr.expression, Variable):
                variables.add(expr.expression.name)
        # Recurse into binary expressions
        for attr in ("left", "right", "operand", "operands"):
            child = getattr(expr, attr, None)
            if child is None:
                continue
            if isinstance(child, list):
                for item in child:
                    variables.update(self._extract_variables(item))
            else:
                variables.update(self._extract_variables(child))
        return variables

    def _count_variables(self) -> int:
        """Count distinct variable names in the query for memory estimation."""
        from pycypher.ast_models import Match, NodePattern, RelationshipPattern

        variables: set[str] = set()
        for clause in self.query.clauses:
            if isinstance(clause, Match) and clause.pattern is not None:
                for path in clause.pattern.paths:
                    for element in path.elements:
                        if isinstance(
                            element,
                            (NodePattern, RelationshipPattern),
                        ):
                            if element.variable is not None:
                                variables.add(element.variable.name)
        return max(len(variables), 1)  # at least 1 column
