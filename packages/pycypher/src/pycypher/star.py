"""Translation layer from Cypher AST to BindingFrame execution.

The main entry point for users is the ``Star`` class, which accepts a
``Context`` (containing ``EntityTable`` and ``RelationshipTable`` objects),
parses a Cypher query string, and executes it against the registered
DataFrames via the BindingFrame execution path.

``Star`` is a thin **facade** that coordinates specialized components:

- :class:`~pycypher.clause_executor.ClauseExecutor` — clause dispatch and execution
- :class:`~pycypher.query_analyzer.QueryAnalyzer` — pre-execution planning and optimization
- :class:`~pycypher.query_explainer.QueryExplainer` — EXPLAIN-style text plans
- :class:`~pycypher.pattern_matcher.PatternMatcher` — MATCH clause translation
- :class:`~pycypher.mutation_engine.MutationEngine` — CREATE/SET/DELETE execution
- :class:`~pycypher.projection_planner.ProjectionPlanner` — RETURN/WITH evaluation
- :class:`~pycypher.frame_joiner.FrameJoiner` — frame merging and joining
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Any

import pandas as pd
from shared.logger import LOGGER, reset_query_id, set_query_id
from shared.metrics import QUERY_METRICS, get_rss_mb
from shared.otel import trace_query

from pycypher.ast_models import (
    ASTConverter,
    Variable,
)
from pycypher.audit import audit_query_error, audit_query_success
from pycypher.binding_frame import BindingFrame
from pycypher.clause_executor import ClauseExecutor
from pycypher.config import COMPLEXITY_WARN_THRESHOLD as _COMPLEXITY_WARN
from pycypher.config import MAX_COMPLEXITY_SCORE as _DEFAULT_MAX_COMPLEXITY
from pycypher.config import QUERY_TIMEOUT_S as _DEFAULT_TIMEOUT_S
from pycypher.config import RESULT_CACHE_MAX_MB as _DEFAULT_RESULT_CACHE_MAX_MB
from pycypher.config import RESULT_CACHE_TTL_S as _DEFAULT_RESULT_CACHE_TTL_S
from pycypher.expression_renderer import ExpressionRenderer
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.mutation_engine import MutationEngine
from pycypher.path_expander import PathExpander
from pycypher.pattern_matcher import PatternMatcher
from pycypher.query_analyzer import QueryAnalyzer
from pycypher.query_explainer import QueryExplainer
from pycypher.relational_models import Context
from pycypher.result_cache import ResultCache
from pycypher.timeout_handler import TimeoutHandler

__all__ = [
    "ResultCache",
    "Star",
    "get_cache_stats",
]

# ---------------------------------------------------------------------------
# Internal column-name constants
# ---------------------------------------------------------------------------

#: Temporary column used during variable-length path BFS to track the frontier.
_VL_TIP_COL: str = "_vl_tip"

#: Synthetic variable name prefix for anonymous nodes.
_ANON_NODE_PREFIX: str = "_anon_node_"

#: Synthetic variable name prefix for anonymous relationships.
_ANON_REL_PREFIX: str = "_anon_rel_"


def _literal_from_python_value(val: Any) -> Any:
    """Convert a Python scalar to the matching AST Literal node."""
    from pycypher.ast_models import (
        BooleanLiteral,
        FloatLiteral,
        IntegerLiteral,
        StringLiteral,
    )

    if isinstance(val, bool):
        return BooleanLiteral(value=val)
    if isinstance(val, int):
        return IntegerLiteral(value=val)
    if isinstance(val, float):
        return FloatLiteral(value=val)
    return StringLiteral(value=str(val))


def get_cache_stats(star: Star | None = None) -> dict[str, Any]:
    """Return combined cache statistics from all PyCypher caches."""
    from pycypher.ast_models import _parse_cypher_cached
    from pycypher.grammar_parser import GrammarParser

    info = _parse_cypher_cached.cache_info()
    lru_total = info.hits + info.misses
    at_capacity = info.maxsize is not None and info.currsize == info.maxsize
    eviction_estimate = (
        max(0, info.misses - (info.maxsize or 0)) if at_capacity else 0
    )
    result: dict[str, Any] = {
        "lru_hits": info.hits,
        "lru_misses": info.misses,
        "lru_size": info.currsize,
        "lru_maxsize": info.maxsize,
        "lru_hit_rate": info.hits / lru_total if lru_total > 0 else 0.0,
        "lark_cache_hits": GrammarParser._lark_cache_hits,
        "lark_cache_misses": GrammarParser._lark_cache_misses,
        "lru_at_capacity": at_capacity,
        "eviction_estimate": eviction_estimate,
    }
    if star is not None:
        result.update(star._result_cache.stats())
    return result


class Star:
    """Main entry point for PyCypher query execution.

    Accepts a :class:`~pycypher.relational_models.Context` (containing
    ``EntityTable`` and ``RelationshipTable`` objects), parses a Cypher query
    string, and executes it against the registered DataFrames via the
    BindingFrame execution path.

    Query lifecycle
    ~~~~~~~~~~~~~~~

    1. **Parse** — Cypher string → AST via :class:`~pycypher.grammar_parser.GrammarParser`.
    2. **Plan** — AST is analysed for complexity, memory budget, and timeout.
    3. **Execute** — clauses are processed sequentially, building up a
       :class:`~pycypher.binding_frame.BindingFrame` through MATCH, WHERE,
       WITH, and RETURN stages.  Mutations (CREATE/SET/DELETE) are staged
       in a shadow layer and committed on success.
    4. **Return** — the final BindingFrame is projected to a pandas DataFrame.

    Example::

        import pandas as pd
        from pycypher import ContextBuilder, Star

        people = pd.DataFrame({
            "__ID__": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 35],
        })
        star = Star(ContextBuilder().add_entity("Person", people).build())

        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name ORDER BY p.name"
        )

    Internally, Star delegates to specialized engines:

    - :class:`~pycypher.pattern_matcher.PatternMatcher` — MATCH clause translation
    - :class:`~pycypher.path_expander.PathExpander` — variable-length path BFS
    - :class:`~pycypher.mutation_engine.MutationEngine` — CREATE/SET/DELETE execution
    - :class:`~pycypher.clause_executor.ClauseExecutor` — clause dispatch and execution loop
    - :class:`~pycypher.query_analyzer.QueryAnalyzer` — pre-execution planning
    - :class:`~pycypher.query_explainer.QueryExplainer` — EXPLAIN text plans
    """

    def __init__(
        self,
        context: Context | ContextBuilder | None = None,
        *,
        result_cache_max_mb: int | None = None,
        result_cache_ttl_seconds: float | None = None,
    ) -> None:
        """Initialize Star with a data context."""
        if context is None:
            context = Context()
        elif isinstance(context, ContextBuilder):
            context = context.build()
        self.context: Context = context

        # Track variable-to-type mappings during pattern processing
        self.variable_type_registry: dict[Variable, str] = {}

        # Delegate all mutation operations to the focused MutationEngine.
        self._mutations: MutationEngine = MutationEngine(context=context)

        # Delegate BFS path expansion to the focused PathExpander.
        self._path_expander: PathExpander = PathExpander(context=context)

        # Delegate frame joining (OPTIONAL MATCH, multi-MATCH merging, seed frames).
        from pycypher.frame_joiner import FrameJoiner

        self._frame_joiner: FrameJoiner = FrameJoiner(
            context=context,
            match_fn=lambda clause, **kw: (
                self._pattern_matcher.match_to_binding_frame(clause, **kw)
            ),
            where_fn=ClauseExecutor.apply_where_filter,
        )

        # Delegate pattern matching to the focused PatternMatcher.
        self._pattern_matcher: PatternMatcher = PatternMatcher(
            context=context,
            path_expander=self._path_expander,
            coerce_join_fn=self._frame_joiner.coerce_join,
            apply_where_fn=ClauseExecutor.apply_where_filter,
            multi_way_join_fn=self._frame_joiner.multi_way_join,
        )

        # Delegate expression rendering.
        self._renderer: ExpressionRenderer = ExpressionRenderer()

        # Delegate aggregation detection and planning.
        from pycypher.aggregation_planner import AggregationPlanner

        self._agg_planner: AggregationPlanner = AggregationPlanner()

        # Delegate RETURN/WITH clause evaluation and projection modifiers.
        from pycypher.projection_planner import ProjectionPlanner

        self._projection_planner: ProjectionPlanner = ProjectionPlanner(
            agg_planner=self._agg_planner,
            renderer=self._renderer,
            where_fn=ClauseExecutor.apply_where_filter,
        )

        # Cardinality feedback store.
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        self._cardinality_feedback = CardinalityFeedbackStore()

        # Query analyzer — pre-execution planning and optimization.
        self._query_analyzer: QueryAnalyzer = QueryAnalyzer(
            context=context,
            cardinality_feedback=self._cardinality_feedback,
            frame_joiner=self._frame_joiner,
            agg_planner=self._agg_planner,
        )

        # Clause executor — clause dispatch and execution loop.
        self._clause_executor: ClauseExecutor = ClauseExecutor(
            context=context,
            pattern_matcher=self._pattern_matcher,
            mutations=self._mutations,
            frame_joiner=self._frame_joiner,
            projection_planner=self._projection_planner,
            query_analyzer=self._query_analyzer,
        )

        # Query explainer — EXPLAIN text plans.
        self._query_explainer: QueryExplainer = QueryExplainer(
            context=context,
            cardinality_feedback=self._cardinality_feedback,
        )

        # Query result cache — LRU with size-bounded eviction.
        _cache_mb = (
            result_cache_max_mb
            if result_cache_max_mb is not None
            else _DEFAULT_RESULT_CACHE_MAX_MB
        )
        _cache_ttl = (
            result_cache_ttl_seconds
            if result_cache_ttl_seconds is not None
            else _DEFAULT_RESULT_CACHE_TTL_S
        )
        self._result_cache: ResultCache = ResultCache(
            max_size_bytes=_cache_mb * 1024 * 1024,
            ttl_seconds=_cache_ttl or 0.0,
        )

        # Last optimization plan — populated by _analyze_and_plan().
        self._last_optimization_plan: Any = None
        self._last_analysis: Any = None

        # Pre-warm the AST cache with common query templates.
        self._warmup_thread: threading.Thread | None = None
        entity_types = list(self.context.entity_mapping.mapping.keys())
        rel_types = list(self.context.relationship_mapping.mapping.keys())
        if entity_types:
            self._warmup_thread = threading.Thread(
                target=self._warmup_ast_cache,
                args=(entity_types, rel_types),
                name="pycypher-ast-warmup",
                daemon=True,
            )
            self._warmup_thread.start()

    @staticmethod
    def _warmup_ast_cache(
        entity_types: list[str],
        rel_types: list[str],
    ) -> None:
        """Pre-parse common query templates to warm the AST cache.

        Runs in a background thread — failures are silently logged and
        never propagate to the caller.
        """
        try:
            converter = ASTConverter()
            for etype in entity_types[:5]:
                try:
                    converter.from_cypher(
                        f"MATCH (n:{etype}) RETURN n"
                    )
                except Exception:  # noqa: BLE001 — best-effort warmup
                    LOGGER.debug("Warmup parse failed for entity %r", etype, exc_info=True)
            for rtype in rel_types[:3]:
                for etype in entity_types[:2]:
                    try:
                        converter.from_cypher(
                            f"MATCH (a:{etype})-[r:{rtype}]->(b) RETURN a, r, b"
                        )
                    except Exception:  # noqa: BLE001 — best-effort warmup
                        LOGGER.debug("Warmup parse failed for rel %r/%r", rtype, etype, exc_info=True)
        except Exception:  # noqa: BLE001 — best-effort warmup
            LOGGER.debug("AST cache warmup failed", exc_info=True)

    def __repr__(self) -> str:
        """Return an informative summary for REPL/notebook display."""
        entity_counts: dict[str, int] = {}
        for name in sorted(self.context.entity_mapping.mapping):
            table = self.context.entity_mapping.mapping[name]
            src = getattr(table, "source_obj", None)
            entity_counts[name] = len(src) if src is not None else 0

        rel_counts: dict[str, int] = {}
        for name in sorted(self.context.relationship_mapping.mapping):
            table = self.context.relationship_mapping.mapping[name]
            src = getattr(table, "source_obj", None)
            rel_counts[name] = len(src) if src is not None else 0

        parts = [f"Star(backend={self.context.backend_name!r}"]
        parts.append(
            f"entities={entity_counts}" if entity_counts else "entities={}",
        )
        if rel_counts:
            parts.append(f"relationships={rel_counts}")
        return ", ".join(parts) + ")"

    @staticmethod
    def _query_has_mutations(parsed: Any) -> bool:
        """Return True if the parsed query contains mutation clauses."""
        from pycypher.ast_models import (
            Create,
            Delete,
            Foreach,
            Merge,
            Remove,
            Set,
            UnionQuery,
        )

        _MUTATION_TYPES = (Create, Delete, Set, Merge, Remove, Foreach)

        if isinstance(parsed, UnionQuery):
            return any(
                Star._query_has_mutations(stmt) for stmt in parsed.statements
            )
        clauses = getattr(parsed, "clauses", None)
        if clauses is None:
            return False
        return any(isinstance(c, _MUTATION_TYPES) for c in clauses)

    def available_functions(self) -> list[str]:
        """Return a sorted list of registered scalar function names."""
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        return registry.list_functions()

    # ------------------------------------------------------------------
    # Delegation to extracted components
    # ------------------------------------------------------------------

    def explain_query(self, query: str) -> str:
        """Return a text execution plan without running the query."""
        return self._query_explainer.explain_query(query)

    # Backward-compatible private method aliases for any internal callers
    def _apply_where_filter(
        self,
        where_expr: Any,
        result_frame: BindingFrame,
        fallback_frame: BindingFrame | None = None,
    ) -> BindingFrame:
        """Apply a WHERE predicate — delegates to :class:`ClauseExecutor`."""
        return ClauseExecutor.apply_where_filter(
            where_expr, result_frame, fallback_frame,
        )

    def _apply_projection_modifiers(
        self,
        df: pd.DataFrame,
        clause: Any,
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Apply DISTINCT/ORDER BY/SKIP/LIMIT — delegates to :class:`ProjectionPlanner`."""
        return self._projection_planner.apply_projection_modifiers(
            df, clause, frame,
        )

    def _return_from_frame(
        self,
        return_clause: Any,
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Evaluate a RETURN clause — delegates to :class:`ProjectionPlanner`."""
        return self._projection_planner.return_from_frame(return_clause, frame)

    def _aggregate_items(
        self,
        items: list[Any],
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Evaluate projection items — delegates to :class:`AggregationPlanner`."""
        return self._agg_planner.aggregate_items(items, frame)

    def _contains_aggregation(self, expression: Any) -> bool:
        """Check for aggregate functions — delegates to :class:`AggregationPlanner`."""
        return self._agg_planner.contains_aggregation(expression)

    def _with_to_binding_frame(
        self,
        with_clause: Any,
        frame: BindingFrame,
    ) -> BindingFrame:
        """Translate a WITH clause — delegates to :class:`ProjectionPlanner`."""
        return self._projection_planner.with_to_binding_frame(
            with_clause, frame,
        )

    def _process_optional_match(self, clause: Any, current_frame: Any) -> Any:
        """Execute an OPTIONAL MATCH — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.process_optional_match(clause, current_frame)

    def _merge_frames_for_match(
        self,
        current_frame: Any,
        match_frame: Any,
        where_clause: Any = None,
    ) -> Any:
        """Merge a new MATCH frame — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.merge_frames_for_match(
            current_frame, match_frame, where_clause,
        )

    def _coerce_join(self, frame_a: Any, frame_b: Any) -> Any:
        """Join two BindingFrames — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.coerce_join(frame_a, frame_b)

    def _make_seed_frame(self) -> Any:
        """Create a seed frame — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.make_seed_frame()

    def _execute_query_binding_frame(self, query: Any) -> pd.DataFrame:
        """Execute with query-scoped shadow write atomicity."""
        result = self._clause_executor.execute_query_binding_frame(query)
        self._sync_executor_state()
        return result

    def _execute_query_binding_frame_inner(
        self, query: Any, initial_frame: Any = None,
    ) -> pd.DataFrame:
        """Execute a Cypher query using the BindingFrame IR."""
        result = self._clause_executor.execute_query_inner(query, initial_frame)
        self._sync_executor_state()
        return result

    def _sync_executor_state(self) -> None:
        """Copy execution metrics back from ClauseExecutor/QueryAnalyzer."""
        self._last_clause_timings = getattr(
            self._clause_executor, "last_clause_timings", {},
        )
        self._last_clause_memory = getattr(
            self._clause_executor, "last_clause_memory", {},
        )
        self._last_estimated_memory_bytes = (
            self._query_analyzer.last_estimated_memory_bytes
        )
        self._last_plan_time_ms = self._query_analyzer.last_plan_time_ms
        self._last_optimization_plan = (
            self._query_analyzer.last_optimization_plan
        )
        self._last_analysis = self._query_analyzer.last_analysis

    def _execute_union_query(self, union_query: Any) -> pd.DataFrame:
        """Execute a UNION [ALL] query."""
        return self._clause_executor.execute_union_query(union_query)

    def _plan_query(self, query: Any) -> dict[str, Any]:
        """Build computation graph — delegates to :class:`QueryAnalyzer`."""
        return self._query_analyzer.plan_query(query)

    def _extract_limit_hint(self, query: Any) -> int | None:
        """Extract LIMIT pushdown hint — delegates to :class:`QueryAnalyzer`."""
        return self._query_analyzer.extract_limit_hint(query)

    def _apply_match_reordering(self, query: Any) -> None:
        """Reorder consecutive MATCH clauses — delegates to :class:`QueryAnalyzer`."""
        return self._query_analyzer.apply_match_reordering(query)

    def _analyze_and_plan(self, query: Any) -> int | None:
        """Run query planning — delegates to :class:`QueryAnalyzer`."""
        return self._query_analyzer.analyze_and_plan(query)

    @staticmethod
    def _require_bound_frame(current_frame: Any, clause_name: str) -> None:
        """Raise ``ValueError`` if *current_frame* is ``None``."""
        ClauseExecutor.require_bound_frame(current_frame, clause_name)

    @staticmethod
    def _frame_size(frame: Any) -> str:
        """Return row count string for a BindingFrame, or ``'(none)'``."""
        return ClauseExecutor.frame_size(frame)

    # ------------------------------------------------------------------
    # Main public API
    # ------------------------------------------------------------------

    def execute_query(
        self,
        query: str | Any,
        *,
        parameters: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
        memory_budget_bytes: int | None = None,
        max_complexity_score: int | None = None,
    ) -> pd.DataFrame:
        """Execute a complete Cypher query and return results as DataFrame.

        Args:
            query: Cypher query string or Query AST node.
            parameters: Optional dict of named query parameters.
            timeout_seconds: Optional wall-clock timeout in seconds.
            memory_budget_bytes: Optional peak-memory budget in bytes.
            max_complexity_score: Optional ceiling for query complexity score.

        Returns:
            DataFrame with columns matching the RETURN clause aliases.

        Raises:
            ValueError: If query structure is invalid.
            NotImplementedError: For unsupported clause types.
            QueryTimeoutError: If execution exceeds *timeout_seconds*.
            QueryMemoryBudgetError: If estimated memory exceeds budget.
            QueryComplexityError: If complexity score exceeds threshold.

        """
        # --- Rate limiting (pre-execution gate) ---
        from pycypher.rate_limiter import get_global_limiter

        get_global_limiter().acquire()

        # --- Input validation ---
        if isinstance(query, str) and not query.strip():
            msg = "Query string must not be empty or whitespace-only."
            raise ValueError(msg)

        if parameters is not None and not isinstance(parameters, dict):
            from pycypher.exceptions import WrongCypherTypeError

            raise WrongCypherTypeError(
                f"parameters must be a dict, got {type(parameters).__name__}",
            )

        _effective_timeout = (
            timeout_seconds
            if timeout_seconds is not None
            else _DEFAULT_TIMEOUT_S
        )

        if _effective_timeout is not None and _effective_timeout < 0:
            msg = f"timeout_seconds must be non-negative, got {_effective_timeout}"
            raise ValueError(msg)

        # Inject parameters into the shared context.
        self.context._parameters = dict(parameters) if parameters else {}

        _qid = uuid.uuid4().hex[:12]
        _qid_token = set_query_id(_qid)
        _query_str = query if isinstance(query, str) else repr(query)
        _param_keys = list(self.context._parameters.keys())
        LOGGER.debug(
            "execute_query: start  query=%r  parameters=%r",
            _query_str[:120],
            _param_keys,
        )
        _t0 = time.perf_counter()
        _rss_before = get_rss_mb()
        self._last_clause_timings: dict[str, float] = {}
        self._last_clause_memory: dict[str, float] = {}
        self.context._memory_budget_bytes = memory_budget_bytes
        _parse_elapsed_ms: float | None = None

        # --- Timeout management ---
        _timeout_handler = TimeoutHandler(
            self.context,
            timeout_seconds=_effective_timeout,
            query_str=_query_str,
            start_time=_t0,
        )
        _timeout_handler.__enter__()

        # --- OpenTelemetry span ---
        _otel_cm = trace_query(
            _query_str,
            query_id=_qid,
            parameters=parameters,
        )
        _otel_span = _otel_cm.__enter__()

        # --- Result cache: fast path for repeated read-only queries ---
        _NON_DETERMINISTIC = {"rand(", "randomuuid(", "timestamp("}
        _cache_params = dict(parameters) if parameters else None
        _from_cache = False
        _query_lower = _query_str.lower() if isinstance(query, str) else ""
        _cache_eligible = (
            isinstance(query, str)
            and self._result_cache.enabled
            and not any(fn in _query_lower for fn in _NON_DETERMINISTIC)
        )
        if _cache_eligible:
            cached = self._result_cache.get(query, _cache_params)
            if cached is not None:
                _elapsed = time.perf_counter() - _t0
                _cached_rows = (
                    len(cached) if isinstance(cached, pd.DataFrame) else 0
                )
                LOGGER.debug(
                    "execute_query: cache hit  elapsed=%.3fs  query=%r",
                    _elapsed,
                    _query_str[:80],
                )
                audit_query_success(
                    query_id=_qid,
                    query=_query_str,
                    elapsed_s=_elapsed,
                    rows=_cached_rows,
                    parameter_keys=_param_keys,
                    cached=True,
                )
                _otel_span.set_attribute("pycypher.cached", True)
                _otel_span.set_attribute("result.rows", _cached_rows)
                _otel_span.set_attribute(
                    "pycypher.elapsed_ms", round(_elapsed * 1000.0, 2)
                )
                _otel_cm.__exit__(None, None, None)
                return cached

        try:
            # --- Pipeline execution: parse → validate → execute ---
            from pycypher.pipeline import (
                ExecuteStage,
                ParseStage,
                Pipeline,
                ValidateStage,
            )

            _effective_max_complexity = (
                max_complexity_score
                if max_complexity_score is not None
                else _DEFAULT_MAX_COMPLEXITY
            )

            _pipeline = Pipeline(
                [
                    ParseStage(),
                    ValidateStage(),
                    ExecuteStage(),
                ]
            )
            _pipeline_result = _pipeline.run(
                query=query,
                star=self,
                parameters=parameters,
                max_complexity_score=_effective_max_complexity,
                memory_budget_bytes=memory_budget_bytes,
            )

            result = _pipeline_result.result
            parsed_query = _pipeline_result.metadata.get("parsed_ast", query)
            _is_mutation = _pipeline_result.metadata.get("is_mutation", False)

            _parse_stage_time = _pipeline_result.stage_timings.get(
                "parse", 0.0
            )
            _parse_elapsed_ms = _parse_stage_time * 1000.0
            self._last_parse_time_ms = _parse_elapsed_ms

            _effective_warn_complexity = _COMPLEXITY_WARN
            _complexity_score = _pipeline_result.metadata.get(
                "complexity_score"
            )
            if _complexity_score is not None:
                LOGGER.debug(
                    "execute_query: complexity score=%d  breakdown=%s",
                    _complexity_score,
                    _pipeline_result.metadata.get("complexity_details", {}),
                )
                for _cw in _pipeline_result.metadata.get(
                    "complexity_warnings", []
                ):
                    LOGGER.warning("query complexity: %s", _cw)
                if (
                    _effective_warn_complexity is not None
                    and _complexity_score > _effective_warn_complexity
                ):
                    LOGGER.warning(
                        "Query complexity score %d exceeds warning "
                        "threshold %d. Top contributors: %s",
                        _complexity_score,
                        _effective_warn_complexity,
                        ", ".join(
                            f"{k}={v}"
                            for k, v in sorted(
                                _pipeline_result.metadata.get(
                                    "complexity_details", {}
                                ).items(),
                                key=lambda x: x[1],
                                reverse=True,
                            )[:3]
                        ),
                    )

            _elapsed = time.perf_counter() - _t0
            _rss_after = get_rss_mb()
            _mem_delta = _rss_after - _rss_before
            _nrows = len(result) if isinstance(result, pd.DataFrame) else 0
            LOGGER.info(
                "execute_query: done  rows=%d  elapsed=%.3fs  rss_delta=%.1fMB  query=%r",
                _nrows,
                _elapsed,
                _mem_delta,
                _query_str[:80],
            )
            audit_query_success(
                query_id=_qid,
                query=_query_str,
                elapsed_s=_elapsed,
                rows=_nrows,
                parameter_keys=_param_keys,
            )
            _clause_names: list[str] = (
                [type(c).__name__ for c in parsed_query.clauses]
                if hasattr(parsed_query, "clauses")
                else []
            )
            QUERY_METRICS.record_query(
                query_id=_qid,
                elapsed_s=_elapsed,
                rows=_nrows,
                clauses=_clause_names,
                memory_delta_mb=_mem_delta,
                clause_timings_ms=self._last_clause_timings or None,
                parse_time_ms=_parse_elapsed_ms,
                estimated_memory_mb=self._query_analyzer.last_estimated_memory_bytes
                / (1024.0 * 1024.0)
                if self._query_analyzer.last_estimated_memory_bytes
                else None,
                plan_time_ms=self._query_analyzer.last_plan_time_ms
                if self._query_analyzer.last_plan_time_ms
                else None,
            )
            QUERY_METRICS.update_cache_stats(self._result_cache.stats())
            if isinstance(result, pd.DataFrame) and result.empty:
                LOGGER.debug(
                    "execute_query: result is empty (0 rows) for query %r",
                    _query_str[:80],
                )

            _otel_span.set_attribute("result.rows", _nrows)
            _otel_span.set_attribute(
                "pycypher.elapsed_ms", round(_elapsed * 1000.0, 2)
            )
            _otel_span.set_attribute(
                "pycypher.memory_delta_mb", round(_mem_delta, 2)
            )
            _otel_span.set_attribute("pycypher.cached", False)
            if _parse_elapsed_ms is not None:
                _otel_span.set_attribute(
                    "pycypher.parse_time_ms", round(_parse_elapsed_ms, 2)
                )

            if _is_mutation:
                self._result_cache.invalidate()
            elif _cache_eligible and isinstance(result, pd.DataFrame):
                self._result_cache.put(query, _cache_params, result)

            return result
        except Exception as _exc:  # noqa: BLE001 — broad catch for metrics; re-raised below
            _elapsed = time.perf_counter() - _t0
            LOGGER.error(
                "execute_query: failed  elapsed=%.3fs  query=%r  error=%s",
                _elapsed,
                _query_str[:80],
                _exc,
                exc_info=True,
            )
            QUERY_METRICS.record_error(
                query_id=_qid,
                error_type=type(_exc).__name__,
                elapsed_s=_elapsed,
            )
            audit_query_error(
                query_id=_qid,
                query=_query_str,
                elapsed_s=_elapsed,
                error_type=type(_exc).__name__,
                parameter_keys=_param_keys,
            )
            raise
        finally:
            _timeout_handler.__exit__(None, None, None)
            import sys as _sys

            _ei = _sys.exc_info()
            _otel_cm.__exit__(_ei[0], _ei[1], _ei[2])
            reset_query_id(_qid_token)
            self.context._parameters = {}
            if self.context._shadow or self.context._shadow_rels:
                self.context.rollback_query()

    async def execute_query_async(
        self,
        query: str | Any,
        *,
        parameters: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
        memory_budget_bytes: int | None = None,
        max_complexity_score: int | None = None,
    ) -> pd.DataFrame:
        """Async wrapper around :meth:`execute_query`."""
        import asyncio

        return await asyncio.to_thread(
            self.execute_query,
            query,
            parameters=parameters,
            timeout_seconds=timeout_seconds,
            memory_budget_bytes=memory_budget_bytes,
            max_complexity_score=max_complexity_score,
        )
