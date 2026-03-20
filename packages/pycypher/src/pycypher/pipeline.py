"""Query execution pipeline abstraction.

Formalises the implicit execution stages in :class:`~pycypher.star.Star` into
a composable, extensible pipeline architecture.  Think of it as the Holtzman
engine of the query processor — each stage is a precisely calibrated fold in
space-time that transforms the query from raw text to final results.

Architecture
------------

::

    Pipeline
    ├── Stage (abstract)           — one step in the execution flow
    │   ├── ParseStage             — string → AST
    │   ├── ValidateStage          — semantic checks + complexity scoring
    │   ├── PlanStage              — query analysis + memory estimation
    │   ├── ExecuteStage           — clause-by-clause evaluation
    │   └── FormatStage            — projection modifiers + result shaping
    ├── PipelineContext            — shared state flowing through stages
    └── PipelineResult             — final output with metadata

Each stage receives a :class:`PipelineContext`, transforms it, and passes it
to the next stage.  Stages may short-circuit (e.g. cache hit) by setting
``context.result`` early.

Usage::

    # Default pipeline with all built-in stages
    pipeline = Pipeline.default()

    # Custom pipeline with extra validation
    pipeline = Pipeline([
        ParseStage(),
        my_custom_lint_stage,
        ValidateStage(),
        PlanStage(),
        ExecuteStage(),
        FormatStage(),
    ])

    result = pipeline.run(query="MATCH (n) RETURN n", star=star)

Extending the pipeline::

    class MyCustomStage(Stage):
        name = "custom_lint"

        def execute(self, ctx: PipelineContext) -> PipelineContext:
            # Inspect ctx.ast, modify ctx.metadata, etc.
            return ctx

    pipeline = Pipeline.default()
    pipeline.insert_after("parse", MyCustomStage())
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from shared.logger import LOGGER

if TYPE_CHECKING:
    import pandas as pd

    from pycypher.ast_models import ASTNode

__all__ = [
    # Public API (re-exported by pycypher.__init__)
    "Pipeline",
    "PipelineContext",
    "PipelineResult",
    "Stage",
    # Built-in stages (extensible by users)
    "ExecuteStage",
    "ParseStage",
    "PlanStage",
    "ValidateStage",
]


# ---------------------------------------------------------------------------
# Pipeline context — shared state flowing through stages
# ---------------------------------------------------------------------------


@dataclass
class PipelineContext:
    """Mutable state that flows through pipeline stages.

    Each stage reads from and writes to this context.  The context
    accumulates parsed AST, plan analysis, execution results, timing
    metrics, and any custom metadata injected by user stages.

    Attributes:
        query_string: Original Cypher query string (if provided).
        ast: Parsed AST node (populated by ParseStage).
        parameters: Named query parameters ($name placeholders).
        result: Final DataFrame result (populated by ExecuteStage).
        metadata: Arbitrary key-value metadata for custom stages.
        stage_timings: Timing in seconds for each completed stage.
        short_circuited: If True, remaining stages are skipped.
        short_circuit_reason: Human-readable reason for short-circuit.

    """

    # Input
    query_string: str | None = None
    query_input: str | Any = None  # Raw input (string or AST)
    parameters: dict[str, Any] = field(default_factory=dict)

    # Execution options
    timeout_seconds: float | None = None
    memory_budget_bytes: int | None = None
    max_complexity_score: int | None = None

    # Stage outputs
    ast: ASTNode | None = None
    plan_analysis: Any | None = None  # AnalysisResult from QueryPlanAnalyzer
    result: pd.DataFrame | None = None

    # Metadata
    metadata: dict[str, Any] = field(default_factory=dict)
    stage_timings: dict[str, float] = field(default_factory=dict)
    short_circuited: bool = False
    short_circuit_reason: str = ""

    # Internal — set by Pipeline.run()
    star: Any = None  # Star instance (avoids circular import)


# ---------------------------------------------------------------------------
# Stage — abstract base for pipeline steps
# ---------------------------------------------------------------------------


class Stage(ABC):
    """Abstract base class for a pipeline stage.

    Subclasses must define :attr:`name` and implement :meth:`execute`.
    The ``name`` is used for logging, timing, and insertion ordering.

    Attributes:
        name: Unique identifier for this stage (e.g. ``"parse"``).

    """

    name: str = "unnamed"

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Transform the pipeline context and return it.

        Args:
            ctx: Current pipeline context with accumulated state.

        Returns:
            The (possibly modified) context.  Return ``ctx`` directly
            unless you need to replace it entirely.

        Raises:
            Any exception propagates to the caller via
            :meth:`Pipeline.run`.

        """
        ...


# ---------------------------------------------------------------------------
# Pipeline — ordered stage runner
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Output of a pipeline run.

    Attributes:
        result: The DataFrame produced by the pipeline.
        stage_timings: Wall-clock time per stage in seconds.
        metadata: Accumulated metadata from all stages.

    """

    result: pd.DataFrame | None
    stage_timings: dict[str, float]
    metadata: dict[str, Any]


class Pipeline:
    """Ordered sequence of :class:`Stage` instances.

    Runs each stage in order, passing the shared :class:`PipelineContext`.
    Stages may short-circuit by setting ``ctx.short_circuited = True``.

    Args:
        stages: Ordered list of stages to execute.

    """

    def __init__(self, stages: list[Stage] | None = None) -> None:
        self._stages: list[Stage] = list(stages or [])

    # --- Mutation API ---

    def append(self, stage: Stage) -> Pipeline:
        """Append a stage to the end of the pipeline.

        Args:
            stage: Stage to add.

        Returns:
            Self for fluent chaining.

        """
        self._stages.append(stage)
        return self

    def insert_before(self, target_name: str, stage: Stage) -> Pipeline:
        """Insert a stage before the named target stage.

        Args:
            target_name: Name of the existing stage.
            stage: Stage to insert.

        Returns:
            Self for fluent chaining.

        Raises:
            ValueError: If *target_name* is not found.

        """
        for i, s in enumerate(self._stages):
            if s.name == target_name:
                self._stages.insert(i, stage)
                return self
        available = [s.name for s in self._stages]
        msg = (
            f"Stage '{target_name}' not found in pipeline. "
            f"Available stages: {available}"
        )
        raise ValueError(msg)

    def insert_after(self, target_name: str, stage: Stage) -> Pipeline:
        """Insert a stage after the named target stage.

        Args:
            target_name: Name of the existing stage.
            stage: Stage to insert.

        Returns:
            Self for fluent chaining.

        Raises:
            ValueError: If *target_name* is not found.

        """
        for i, s in enumerate(self._stages):
            if s.name == target_name:
                self._stages.insert(i + 1, stage)
                return self
        available = [s.name for s in self._stages]
        msg = (
            f"Stage '{target_name}' not found in pipeline. "
            f"Available stages: {available}"
        )
        raise ValueError(msg)

    def remove(self, stage_name: str) -> Pipeline:
        """Remove a stage by name.

        Args:
            stage_name: Name of the stage to remove.

        Returns:
            Self for fluent chaining.

        Raises:
            ValueError: If *stage_name* is not found.

        """
        for i, s in enumerate(self._stages):
            if s.name == stage_name:
                self._stages.pop(i)
                return self
        available = [s.name for s in self._stages]
        msg = (
            f"Stage '{stage_name}' not found in pipeline. "
            f"Available stages: {available}"
        )
        raise ValueError(msg)

    @property
    def stage_names(self) -> list[str]:
        """Return ordered list of stage names."""
        return [s.name for s in self._stages]

    # --- Execution ---

    def run(
        self,
        *,
        query: str | Any,
        star: Any,
        parameters: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
        memory_budget_bytes: int | None = None,
        max_complexity_score: int | None = None,
    ) -> PipelineResult:
        """Execute the pipeline and return results.

        Args:
            query: Cypher query string or pre-parsed AST node.
            star: :class:`~pycypher.star.Star` instance for execution.
            parameters: Optional named query parameters.
            timeout_seconds: Optional wall-clock timeout.
            memory_budget_bytes: Optional memory budget.
            max_complexity_score: Optional complexity ceiling.

        Returns:
            :class:`PipelineResult` with DataFrame and timing metadata.

        """
        ctx = PipelineContext(
            query_input=query,
            query_string=query if isinstance(query, str) else None,
            parameters=dict(parameters or {}),
            timeout_seconds=timeout_seconds,
            memory_budget_bytes=memory_budget_bytes,
            max_complexity_score=max_complexity_score,
            star=star,
        )

        for stage in self._stages:
            if ctx.short_circuited:
                LOGGER.debug(
                    "Pipeline short-circuited at '%s': %s",
                    stage.name,
                    ctx.short_circuit_reason,
                )
                break

            t0 = time.perf_counter()
            ctx = stage.execute(ctx)
            elapsed = time.perf_counter() - t0
            ctx.stage_timings[stage.name] = elapsed

            LOGGER.debug(
                "Pipeline stage '%s' completed in %.4fs",
                stage.name,
                elapsed,
            )

        return PipelineResult(
            result=ctx.result,
            stage_timings=ctx.stage_timings,
            metadata=ctx.metadata,
        )

    # --- Factory ---

    @classmethod
    def default(cls) -> Pipeline:
        """Create a pipeline with the standard built-in stages.

        Returns a pipeline with: parse → validate → plan → execute → format.

        """
        return cls(
            [
                ParseStage(),
                ValidateStage(),
                PlanStage(),
                ExecuteStage(),
            ],
        )


# ---------------------------------------------------------------------------
# Built-in stages
# ---------------------------------------------------------------------------


class ParseStage(Stage):
    """Parse a Cypher query string into a typed AST.

    If the input is already an AST node, this stage is a no-op.

    Populates ``ctx.ast`` with the parsed :class:`ASTNode`.
    """

    name: str = "parse"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Parse query string to AST."""
        from pycypher.ast_converter import ASTConverter
        from pycypher.ast_models import ASTNode

        if isinstance(ctx.query_input, ASTNode):
            ctx.ast = ctx.query_input
            return ctx

        if not isinstance(ctx.query_input, str) or not ctx.query_input.strip():
            msg = "Query must be a non-empty string or ASTNode"
            raise ValueError(msg)

        ctx.ast = ASTConverter.from_cypher(ctx.query_input)
        return ctx


class ValidateStage(Stage):
    """Run semantic validation and complexity scoring.

    Checks query complexity against ``ctx.max_complexity_score`` if set.
    Populates ``ctx.metadata["complexity_score"]`` when scoring runs.
    """

    name: str = "validate"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Validate parsed AST."""
        if ctx.ast is None:
            return ctx

        if ctx.max_complexity_score is not None:
            from pycypher.query_complexity import score_query

            score_result = score_query(ctx.ast)
            ctx.metadata["complexity_score"] = score_result.total
            ctx.metadata["complexity_details"] = score_result.breakdown
            # score_query raises QueryComplexityError internally if exceeded

        return ctx


class PlanStage(Stage):
    """Analyse query structure and estimate resource requirements.

    Runs :class:`QueryPlanAnalyzer` to produce cardinality estimates,
    join strategies, and memory projections.  Populates
    ``ctx.plan_analysis`` and ``ctx.metadata["estimated_peak_bytes"]``.
    """

    name: str = "plan"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Plan query execution."""
        if ctx.ast is None or ctx.star is None:
            return ctx

        from pycypher.ast_models import Query

        if not isinstance(ctx.ast, Query):
            return ctx

        try:
            from pycypher.query_planner import QueryPlanAnalyzer

            analyzer = QueryPlanAnalyzer(ctx.ast, ctx.star.context)
            analysis = analyzer.analyze()
            ctx.plan_analysis = analysis
            ctx.metadata["estimated_peak_bytes"] = (
                analysis.estimated_peak_bytes
            )
            ctx.metadata["clause_cardinalities"] = (
                analysis.clause_cardinalities
            )

            # Memory budget check
            budget = ctx.memory_budget_bytes
            if budget is not None and analysis.exceeds_budget(
                budget_bytes=budget
            ):
                from pycypher.exceptions import QueryMemoryBudgetError

                raise QueryMemoryBudgetError(
                    estimated_bytes=analysis.estimated_peak_bytes,
                    budget_bytes=budget,
                )
        except ImportError:
            LOGGER.debug(
                "QueryPlanAnalyzer not available, skipping plan stage"
            )

        return ctx


class ExecuteStage(Stage):
    """Execute the parsed query against the Star engine.

    Delegates to :meth:`Star.execute_query` with the parsed AST.
    Populates ``ctx.result`` with the output DataFrame.
    """

    name: str = "execute"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        """Execute query and populate result."""
        if ctx.star is None:
            msg = (
                "Star instance required for execution. "
                "Pass a Star instance via Pipeline.run(star=...) or set "
                "ctx.star before running the ExecuteStage."
            )
            raise ValueError(msg)

        # Delegate to Star.execute_query with the original input
        # This preserves all existing behavior (caching, timeout, metrics)
        ctx.result = ctx.star.execute_query(
            ctx.query_input,
            parameters=ctx.parameters or None,
            timeout_seconds=ctx.timeout_seconds,
            memory_budget_bytes=ctx.memory_budget_bytes,
            max_complexity_score=ctx.max_complexity_score,
        )
        return ctx
