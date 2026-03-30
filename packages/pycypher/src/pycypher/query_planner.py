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

import pandas as pd
from shared.logger import LOGGER

if TYPE_CHECKING:
    from pycypher.ast_models import Query
    from pycypher.relational_models import Context

__all__ = [
    "AggStrategy",
    "ColumnStatistics",
    "JoinPlan",
    "JoinStrategy",
    "QueryPlan",
    "QueryPlanAnalyzer",
    "QueryPlanner",
    "TableStatistics",
]


# ---------------------------------------------------------------------------
# Column and table statistics for cardinality estimation
# ---------------------------------------------------------------------------

#: Maximum number of rows to sample when computing column statistics.
_STATS_SAMPLE_SIZE: int = 10_000


@dataclass(frozen=True)
class ColumnStatistics:
    """Statistics for a single column, used for selectivity estimation.

    Attributes:
        ndv: Number of distinct values (excluding nulls).
        null_fraction: Fraction of rows that are null (0.0–1.0).
        min_value: Minimum non-null value (numeric columns only).
        max_value: Maximum non-null value (numeric columns only).
        row_count: Total rows in the table at time of collection.

    """

    ndv: int
    null_fraction: float
    min_value: float | None = None
    max_value: float | None = None
    row_count: int = 0

    def equality_selectivity(self) -> float:
        """Selectivity for ``col = value``: 1/NDV, adjusted for nulls."""
        if self.ndv <= 0:
            return _DEFAULT_FILTER_SELECTIVITY
        return (1.0 - self.null_fraction) / self.ndv

    def range_selectivity(
        self,
        low: float | None = None,
        high: float | None = None,
    ) -> float:
        """Selectivity for range predicates (``col > low``, ``col < high``).

        Uses uniform distribution assumption over [min_value, max_value].
        """
        if self.min_value is None or self.max_value is None:
            return _DEFAULT_FILTER_SELECTIVITY
        span = self.max_value - self.min_value
        if span <= 0:
            return _DEFAULT_FILTER_SELECTIVITY

        lo = low if low is not None else self.min_value
        hi = high if high is not None else self.max_value
        lo = max(lo, self.min_value)
        hi = min(hi, self.max_value)

        if lo >= hi:
            return 1.0 / max(self.row_count, 1)

        sel = (hi - lo) / span * (1.0 - self.null_fraction)
        return max(sel, 1.0 / max(self.row_count, 1))


class TableStatistics:
    """Collects and caches column-level statistics for an entity or
    relationship table.

    Statistics are computed lazily on first access and cached.  For large
    tables, a random sample of ``_STATS_SAMPLE_SIZE`` rows is used.
    """

    def __init__(self, source_obj: Any) -> None:
        self._source = source_obj
        self._columns: dict[str, ColumnStatistics] = {}
        self._row_count: int | None = None

    @property
    def row_count(self) -> int:
        if self._row_count is None:
            if hasattr(self._source, "__len__"):
                self._row_count = len(self._source)
            else:
                self._row_count = 0
        return self._row_count

    def column_stats(self, column: str) -> ColumnStatistics | None:
        """Return cached statistics for *column*, computing on first call."""
        if column in self._columns:
            return self._columns[column]
        stats = self._compute_column_stats(column)
        if stats is not None:
            self._columns[column] = stats
        return stats

    def _compute_column_stats(self, column: str) -> ColumnStatistics | None:
        """Compute statistics for a single column from the source data."""
        try:
            if isinstance(self._source, pd.DataFrame):
                df = self._source
            elif hasattr(self._source, "to_pandas"):
                df = self._source.to_pandas()
            else:
                return None

            if column not in df.columns:
                return None

            # Sample for large tables
            n = len(df)
            if n > _STATS_SAMPLE_SIZE:
                sample = df[column].sample(
                    n=_STATS_SAMPLE_SIZE,
                    random_state=42,
                )
            else:
                sample = df[column]

            null_count = int(sample.isna().sum())
            null_fraction = null_count / max(len(sample), 1)
            non_null = sample.dropna()
            ndv = int(non_null.nunique())

            min_val: float | None = None
            max_val: float | None = None
            if len(non_null) > 0 and pd.api.types.is_numeric_dtype(non_null):
                min_val = float(non_null.min())
                max_val = float(non_null.max())

            return ColumnStatistics(
                ndv=ndv,
                null_fraction=null_fraction,
                min_value=min_val,
                max_value=max_val,
                row_count=n,
            )
        except Exception:
            LOGGER.debug(
                "Failed to compute statistics for column %r",
                column,
                exc_info=True,
            )
            return None


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

    #: LeapfrogTriejoin — worst-case optimal multi-way join.
    #: Best for 3+ relations sharing a common join variable (e.g. triangle
    #: queries).  O(N^{w/2}) vs O(N^{w-1}) for iterated binary joins.
    LEAPFROG = "leapfrog"


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

    def __init__(
        self,
        query: Query,
        context: Context,
        feedback_store: CardinalityFeedbackStore | None = None,
    ) -> None:
        """Initialize query plan analyzer.

        Args:
            query: Parsed Cypher query AST to analyze.
            context: Execution context with entity/relationship tables for
                cardinality estimation.
            feedback_store: Optional feedback store for correcting estimates
                based on historical execution data.

        """
        self.query = query
        self.context = context
        self._planner = get_default_planner()
        self._feedback = feedback_store
        self._table_stats: dict[str, TableStatistics] = {}
        self._build_table_stats()

    # -- statistics helpers --------------------------------------------------

    def _build_table_stats(self) -> None:
        """Pre-build ``TableStatistics`` for all registered tables."""
        for name, et in self.context.entity_mapping.mapping.items():
            self._table_stats[name] = TableStatistics(et.source_obj)
        for name, rt in self.context.relationship_mapping.mapping.items():
            self._table_stats[name] = TableStatistics(rt.source_obj)

    def _get_column_stats(
        self,
        entity_type: str,
        column: str,
    ) -> ColumnStatistics | None:
        """Look up column statistics for *entity_type.column*."""
        ts = self._table_stats.get(entity_type)
        if ts is None:
            return None
        return ts.column_stats(column)

    def estimate_predicate_selectivity(self, predicate: Any) -> float:
        """Estimate the selectivity of a WHERE predicate using column
        statistics when available, falling back to heuristic defaults.

        Supported predicate patterns:
        - ``p.age > 30`` → range selectivity from column stats
        - ``p.name = 'Alice'`` → equality selectivity (1/NDV)
        - ``p.age > 30 AND p.dept = 'Eng'`` → product of selectivities
        - ``p.age > 30 OR p.dept = 'Eng'`` → capped union
        - ``NOT pred`` → 1 - selectivity
        """
        from pycypher.ast_models import (
            And,
            Comparison,
            Not,
            Or,
        )

        if isinstance(predicate, And):
            # Independence assumption: multiply selectivities
            sel = 1.0
            for op in (predicate.left, predicate.right):
                if op is not None:
                    sel *= self.estimate_predicate_selectivity(op)
            # Also handle .operands list if present
            for op in getattr(predicate, "operands", []):
                sel *= self.estimate_predicate_selectivity(op)
            return sel

        if isinstance(predicate, Or):
            # Union: P(A or B) = P(A) + P(B) - P(A)*P(B), capped at 1.0
            parts = []
            for op in (predicate.left, predicate.right):
                if op is not None:
                    parts.append(self.estimate_predicate_selectivity(op))
            for op in getattr(predicate, "operands", []):
                parts.append(self.estimate_predicate_selectivity(op))
            if not parts:
                return _DEFAULT_FILTER_SELECTIVITY
            sel = parts[0]
            for p in parts[1:]:
                sel = sel + p - sel * p
            return min(sel, 1.0)

        if isinstance(predicate, Not):
            child = getattr(predicate, "operand", None)
            if child is not None:
                return 1.0 - self.estimate_predicate_selectivity(child)
            return 1.0 - _DEFAULT_FILTER_SELECTIVITY

        if isinstance(predicate, Comparison):
            return self._estimate_comparison_selectivity(predicate)

        # Unknown predicate type — fall back to default
        return _DEFAULT_FILTER_SELECTIVITY

    def _estimate_comparison_selectivity(self, comp: Any) -> float:
        """Estimate selectivity for a single comparison expression."""
        from pycypher.ast_models import (
            FloatLiteral,
            IntegerLiteral,
            PropertyLookup,
            StringLiteral,
            Variable,
        )

        operator = getattr(comp, "operator", None)
        left = getattr(comp, "left", None)
        right = getattr(comp, "right", None)

        # Try to extract entity_type and column from PropertyLookup
        entity_type: str | None = None
        column: str | None = None
        literal_value: float | str | None = None

        for side, other_side in [(left, right), (right, left)]:
            if isinstance(side, PropertyLookup):
                if isinstance(side.expression, Variable):
                    # We need to find which entity type this variable maps to.
                    # Walk the query's MATCH patterns to resolve var → entity.
                    var_name = side.expression.name
                    entity_type = self._resolve_variable_entity_type(var_name)
                    column = side.property
                # Check what the literal value is
                if isinstance(other_side, IntegerLiteral) or isinstance(
                    other_side, FloatLiteral
                ):
                    literal_value = float(other_side.value)
                elif isinstance(other_side, StringLiteral):
                    literal_value = other_side.value
                if entity_type is not None:
                    break

        if entity_type is None or column is None:
            return _DEFAULT_FILTER_SELECTIVITY

        stats = self._get_column_stats(entity_type, column)
        if stats is None:
            return _DEFAULT_FILTER_SELECTIVITY

        op = operator if isinstance(operator, str) else str(operator)
        # Normalize operator representations
        op = op.strip().upper()

        if op in ("=", "==", "EQ"):
            return stats.equality_selectivity()

        if op in ("<>", "!=", "NEQ"):
            return 1.0 - stats.equality_selectivity()

        if op in (">", "GT", ">=", "GTE"):
            if isinstance(literal_value, (int, float)):
                return stats.range_selectivity(low=literal_value)
            return _DEFAULT_FILTER_SELECTIVITY

        if op in ("<", "LT", "<=", "LTE"):
            if isinstance(literal_value, (int, float)):
                return stats.range_selectivity(high=literal_value)
            return _DEFAULT_FILTER_SELECTIVITY

        return _DEFAULT_FILTER_SELECTIVITY

    def _resolve_variable_entity_type(self, var_name: str) -> str | None:
        """Resolve a variable name to its entity type by walking MATCH
        patterns in the query.
        """
        from pycypher.ast_models import Match, NodePattern

        for clause in self.query.clauses:
            if isinstance(clause, Match) and clause.pattern is not None:
                for path in clause.pattern.paths:
                    for element in path.elements:
                        if (
                            isinstance(element, NodePattern)
                            and element.variable is not None
                            and element.variable.name == var_name
                            and element.labels
                        ):
                            return element.labels[0]
        return None

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

        # Apply cardinality correction from feedback history when available.
        if self._feedback is not None:
            all_types = entity_types + rel_types
            corrections = [
                self._feedback.correction_factor(t) for t in all_types
            ]
            if corrections:
                # Geometric mean of correction factors for all involved types.
                from functools import reduce
                import operator

                product = reduce(operator.mul, corrections, 1.0)
                geo_mean = product ** (1.0 / len(corrections))
                if geo_mean != 1.0:
                    cardinality = max(1, int(cardinality * geo_mean))
                    LOGGER.debug(
                        "Cardinality feedback correction: types=%s factor=%.3f adjusted=%d",
                        all_types,
                        geo_mean,
                        cardinality,
                    )

        # Apply filter selectivity using column statistics when available
        if clause.where is not None:
            selectivity = self.estimate_predicate_selectivity(clause.where)
            cardinality = max(1, int(cardinality * selectivity))

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
        """Extract all variable names referenced in an expression.

        Delegates to :func:`~pycypher.ast_models.extract_referenced_variables`,
        the canonical variable extraction function.
        """
        from pycypher.ast_models import ASTNode, extract_referenced_variables

        if isinstance(expr, ASTNode):
            return extract_referenced_variables(expr)
        return set()

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

    def log_cardinality_feedback(
        self,
        analysis: AnalysisResult,
        actual_rows: list[int],
    ) -> None:
        """Log estimated vs actual cardinality for post-execution analysis.

        Args:
            analysis: The pre-execution analysis result.
            actual_rows: Actual row counts per clause (same order as
                ``analysis.clause_cardinalities``).

        """
        for i, (est, act) in enumerate(
            zip(analysis.clause_cardinalities, actual_rows),
        ):
            if est == 0 and act == 0:
                continue
            ratio = act / max(est, 1)
            level = "DEBUG" if 0.5 <= ratio <= 2.0 else "WARNING"
            LOGGER.log(
                getattr(__import__("logging"), level),
                "Cardinality feedback clause %d: estimated=%d actual=%d ratio=%.2f",
                i,
                est,
                act,
                ratio,
            )


# ---------------------------------------------------------------------------
# CardinalityFeedbackStore — learns from execution history
# ---------------------------------------------------------------------------

_MAX_HISTORY = 32  # rolling window per entity type


class CardinalityFeedbackStore:
    """Accumulates actual vs estimated cardinality ratios per entity type.

    After each query execution, call :meth:`record` with the entity types
    involved and the (estimated, actual) row counts.  Before a future
    estimate, call :meth:`correction_factor` to get a multiplicative
    adjustment derived from historical accuracy.

    Thread-safe via a simple lock.  History is bounded to the most recent
    ``_MAX_HISTORY`` observations per entity type.
    """

    def __init__(self) -> None:
        import threading
        from collections import deque

        self._lock = threading.Lock()
        # entity_type → deque of (estimated, actual) tuples
        self._history: dict[str, deque[tuple[int, int]]] = {}

    def record(
        self,
        entity_type: str,
        estimated: int,
        actual: int,
    ) -> None:
        """Record an (estimated, actual) observation for *entity_type*."""
        if estimated <= 0 and actual <= 0:
            return
        from collections import deque

        with self._lock:
            if entity_type not in self._history:
                self._history[entity_type] = deque(maxlen=_MAX_HISTORY)
            self._history[entity_type].append((estimated, actual))

    def correction_factor(self, entity_type: str) -> float:
        """Return a multiplicative correction for *entity_type*.

        If the estimator consistently overestimates by 2x, this returns
        ~0.5 so the caller can multiply the heuristic estimate by it.
        Returns 1.0 when no history is available.
        """
        with self._lock:
            history = self._history.get(entity_type)
            if not history:
                return 1.0

        # Compute mean(actual / estimated) with clamp to avoid division by zero.
        ratios = [act / max(est, 1) for est, act in history]
        avg_ratio = sum(ratios) / len(ratios)
        # Clamp to [0.01, 100] to prevent runaway corrections.
        return max(0.01, min(100.0, avg_ratio))

    @property
    def entity_types_tracked(self) -> list[str]:
        """Return entity types with recorded history."""
        with self._lock:
            return list(self._history.keys())

    def clear(self) -> None:
        """Drop all recorded history."""
        with self._lock:
            self._history.clear()
