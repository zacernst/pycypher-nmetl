"""Translation layer from Cypher AST to BindingFrame execution.

The main entry point for users is the ``Star`` class, which accepts a
``Context`` (containing ``EntityTable`` and ``RelationshipTable`` objects),
parses a Cypher query string, and executes it against the registered
DataFrames via the BindingFrame execution path.

The ``Star.execute_query()`` method drives the full pipeline: parsing,
clause-by-clause translation, and execution.

Supported Cypher Features
--------------------------

Pattern Matching
~~~~~~~~~~~~~~~~
* **MATCH** — node and relationship patterns; anonymous and named variables;
  inline node-property filters (``{key: val}``); unlabeled node traversal;
  multiple MATCH clauses in the same query.
* **OPTIONAL MATCH** — left-outer-join semantics; unmatched rows produce
  ``null`` for all variables introduced by the OPTIONAL MATCH.
* **Relationship direction** — directed (``-->``, ``<--``) and undirected
  (``--``) traversal; undirected is executed as a union of both directed
  traversals with duplicates removed.
* **Variable-length paths** — ``[*m..n]`` hop-bounded BFS; unbounded ``[*]``
  capped at :data:`_MAX_UNBOUNDED_PATH_HOPS` hops.
* **Relationship labels** — typed (``[:KNOWS]``), typed union
  (``[:KNOWS|:LIKES]`` or ``[:KNOWS|LIKES]``), and untyped (``[]``)
  relationship patterns; untyped scans every registered relationship type.
* **Relationship properties** — inline property filters on relationship
  patterns (``-[r {since: 2020}]->``) and post-hoc access via ``r.prop``.

Filtering and Predicates
~~~~~~~~~~~~~~~~~~~~~~~~~
* **WHERE** — full expression predicate support including scalar functions,
  boolean operators (``AND``, ``OR``, ``NOT``), comparisons, ``IS NULL``,
  ``IS NOT NULL``, and string predicates.
* **String predicates** — ``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``,
  regex match (``=~``), ``IN``, and ``NOT IN``.
* **Null literals** — ``null`` is parsed and propagated correctly through
  comparisons and predicates.

Expressions
~~~~~~~~~~~
* **Arithmetic** — ``+``, ``-``, ``*``, ``/``, ``%``, ``^`` with operator
  precedence.
* **CASE** — both searched (``CASE WHEN … THEN … ELSE … END``) and simple
  (``CASE expr WHEN val THEN … END``) forms.
* **List comprehensions** — ``[x IN list WHERE pred | expr]``.
* **Quantifier predicates** — ``all()``, ``any()``, ``none()``, ``single()``
  over lists.
* **reduce()** — ``reduce(acc = init, var IN list | step)`` accumulation.
* **Scalar functions** — full registry of built-in functions; see
  :meth:`Star.available_functions` for the complete list.
* **UNWIND** — expands a list expression into individual rows.

Projection and Aggregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~
* **WITH** — simple column projection, full-table aggregation, and
  grouped aggregation; supports DISTINCT, ORDER BY, SKIP, and LIMIT.
* **WITH *** — pass-through projection; all non-internal bindings are
  forwarded unchanged (ORDER BY / SKIP / LIMIT still apply).
* **RETURN** — expression projection with DISTINCT, ORDER BY (ASC / DESC),
  SKIP, and LIMIT.
* **RETURN *** — return all non-internal in-scope variables.
* **Aggregation functions** — ``count()``, ``sum()``, ``avg()``, ``min()``,
  ``max()``, ``collect()`` (produces Python lists), ``stdev()`` (sample,
  ddof=1), ``stdevp()`` (population, ddof=0), ``percentileCont(expr, p)``
  (linear interpolation), ``percentileDisc(expr, p)`` (lower/discrete).

Mutation
~~~~~~~~
* **CREATE** — node and relationship insertion.  New entity types are
  automatically registered in the context at commit time.
* **SET** — property write-back via shadow-write atomicity; supports
  computed expressions on the right-hand side.
* **REMOVE p.property** — remove a single property from matched nodes.
  ``REMOVE p:Label`` is accepted but is a no-op (label membership is
  implicit in entity-table identity and cannot be detached per-row).
* **DELETE** — remove matched entity rows from the entity table.
* **DETACH DELETE** — remove matched entities *and* all relationship rows
  that reference them (outgoing or incoming).
* **MERGE** — upsert primitive: attempts to match the supplied node
  pattern; if no match exists, creates the node.  Idempotent.
  Supports ``ON CREATE SET`` (fires only when the node was created) and
  ``ON MATCH SET`` (fires only when the node already existed); both may
  appear together in the same MERGE clause.

All mutations are staged in a per-query shadow layer and committed
atomically when the query succeeds; a failed query is automatically
rolled back to leave the context unchanged.

Procedure Calls
~~~~~~~~~~~~~~~
* **CALL procedure(args) YIELD col1, col2** — invokes a registered
  procedure and introduces the YIELDed columns into the binding frame.
  Built-in ``db.*`` procedures:

  - ``db.labels()`` → ``label`` (one row per registered entity type)
  - ``db.relationshipTypes()`` → ``relationshipType`` (one per relationship type)
  - ``db.propertyKeys()`` → ``propertyKey`` (one per unique user-visible property)

  Custom procedures can be registered via
  :data:`~pycypher.relational_models.PROCEDURE_REGISTRY`.

Set Operations
~~~~~~~~~~~~~~
* **UNION / UNION ALL** — combine the results of multiple queries;
  ``UNION`` deduplicates, ``UNION ALL`` preserves all rows.

Utility
-------
* :meth:`Star.available_functions` — return sorted list of registered scalar
  function names from the :class:`~pycypher.scalar_functions.ScalarFunctionRegistry`.
* :data:`~pycypher.relational_models.PROCEDURE_REGISTRY` — module-level
  singleton for registering and invoking CALL procedures.
"""

from __future__ import annotations

import hashlib
import json
import signal
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any

# BindingFrame is needed at runtime, not just for type checking
import pandas as pd
from shared.logger import LOGGER, reset_query_id, set_query_id
from shared.metrics import QUERY_METRICS, get_rss_mb
from shared.otel import trace_phase, trace_query

from pycypher.ast_models import (
    ASTConverter,
    Variable,
)
from pycypher.audit import audit_query_error, audit_query_success
from pycypher.binding_frame import BindingFrame
from pycypher.config import COMPLEXITY_WARN_THRESHOLD as _COMPLEXITY_WARN
from pycypher.config import MAX_COMPLEXITY_SCORE as _DEFAULT_MAX_COMPLEXITY
from pycypher.config import QUERY_TIMEOUT_S as _DEFAULT_TIMEOUT_S
from pycypher.config import RESULT_CACHE_MAX_MB as _DEFAULT_RESULT_CACHE_MAX_MB
from pycypher.config import RESULT_CACHE_TTL_S as _DEFAULT_RESULT_CACHE_TTL_S
from pycypher.exceptions import QueryTimeoutError
from pycypher.expression_renderer import ExpressionRenderer
from pycypher.ingestion.context_builder import ContextBuilder
from pycypher.mutation_engine import MutationEngine
from pycypher.path_expander import PathExpander
from pycypher.pattern_matcher import PatternMatcher
from pycypher.relational_models import Context

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


class ResultCache:
    """LRU cache for query results with size-bounded eviction and TTL support.

    Keys are derived from the normalised query string and parameters.
    Values are copied DataFrames so mutations to the returned frame do not
    corrupt the cached entry.

    The cache is automatically invalidated when the underlying ``Context``
    commits a mutation (SET / CREATE / DELETE / MERGE / REMOVE).

    Thread-safety: all public methods acquire ``_lock``.
    """

    def __init__(
        self,
        max_size_bytes: int = _DEFAULT_RESULT_CACHE_MAX_MB * 1024 * 1024,
        ttl_seconds: float = _DEFAULT_RESULT_CACHE_TTL_S or 0.0,
    ) -> None:
        """Initialize the result cache.

        Args:
            max_size_bytes: Maximum total memory for cached DataFrames.
                ``0`` disables caching entirely.
            ttl_seconds: Time-to-live per entry in seconds.
                ``0`` means entries never expire by time (only by LRU eviction
                or mutation-based invalidation).

        """
        self._max_size_bytes: int = max_size_bytes
        self._ttl_seconds: float = ttl_seconds
        # OrderedDict for LRU: most-recently-used entries move to the end.
        self._entries: OrderedDict[str, tuple[pd.DataFrame, float, int]] = (
            OrderedDict()
        )
        self._current_size_bytes: int = 0
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0
        self._lock: threading.Lock = threading.Lock()
        # Monotonically increasing counter — bumped on every mutation commit.
        # Cache entries store the generation at insertion time; a mismatch
        # means the underlying data has changed.
        self._generation: int = 0

    @property
    def enabled(self) -> bool:
        """Whether caching is active (max_size_bytes > 0)."""
        return self._max_size_bytes > 0

    @staticmethod
    def _make_key(query: str, parameters: dict[str, Any] | None) -> str:
        """Produce a deterministic cache key from query + parameters.

        Args:
            query: The Cypher query string.
            parameters: Optional query parameters dict.

        Returns:
            A hex-digest string suitable as a dict key.

        """
        # blake2b is ~3x faster than SHA-256 for short inputs and equally
        # collision-resistant for cache-key purposes (not cryptographic).
        h = hashlib.blake2b(query.encode("utf-8"), digest_size=16)
        if parameters:
            # Sort keys for deterministic ordering.
            h.update(
                json.dumps(parameters, sort_keys=True, default=str).encode(
                    "utf-8",
                ),
            )
        return h.hexdigest()

    @staticmethod
    def _estimate_df_bytes(df: pd.DataFrame) -> int:
        """Estimate the in-memory size of a DataFrame in bytes.

        Args:
            df: The DataFrame to measure.

        Returns:
            Estimated size in bytes.

        """
        return int(df.memory_usage(deep=True).sum())

    def get(
        self,
        query: str,
        parameters: dict[str, Any] | None,
    ) -> pd.DataFrame | None:
        """Look up a cached result.

        Args:
            query: The Cypher query string.
            parameters: Optional query parameters dict.

        Returns:
            A **copy** of the cached DataFrame, or ``None`` on miss.

        """
        if not self.enabled:
            self._misses += 1
            return None

        key = self._make_key(query, parameters)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._misses += 1
                return None

            df, timestamp, generation = entry

            # Stale generation — data has been mutated since caching.
            if generation != self._generation:
                del self._entries[key]
                self._current_size_bytes -= self._estimate_df_bytes(df)
                self._misses += 1
                return None

            # TTL expiry.
            if self._ttl_seconds > 0:
                age = time.monotonic() - timestamp
                if age > self._ttl_seconds:
                    del self._entries[key]
                    self._current_size_bytes -= self._estimate_df_bytes(df)
                    self._misses += 1
                    return None

            # Move to end (most-recently-used).
            self._entries.move_to_end(key)
            self._hits += 1
            # Lazy copy (O(1) with pandas 3.0+ CoW) — ensures callers get
            # a distinct object so inplace mutations cannot corrupt the cache.
            return df.copy()

    def put(
        self,
        query: str,
        parameters: dict[str, Any] | None,
        result: pd.DataFrame,
    ) -> None:
        """Store a query result in the cache.

        Args:
            query: The Cypher query string.
            parameters: Optional query parameters dict.
            result: The DataFrame to cache (a lazy copy is stored).

        """
        if not self.enabled:
            return

        key = self._make_key(query, parameters)
        # Lazy copy (O(1) with pandas 3.0+ CoW) — protects the cache from
        # inplace mutations by the caller on the original *result*.
        df_copy = result.copy()
        entry_bytes = self._estimate_df_bytes(df_copy)

        # Don't cache entries larger than the entire budget.
        if entry_bytes > self._max_size_bytes:
            return

        with self._lock:
            # If key already exists, remove old entry first.
            if key in self._entries:
                old_df, _, _ = self._entries.pop(key)
                self._current_size_bytes -= self._estimate_df_bytes(old_df)

            # Evict LRU entries until there is room.
            while (
                self._entries
                and self._current_size_bytes + entry_bytes
                > self._max_size_bytes
            ):
                _, (evicted_df, _, _) = self._entries.popitem(last=False)
                self._current_size_bytes -= self._estimate_df_bytes(evicted_df)
                self._evictions += 1

            self._entries[key] = (
                df_copy,
                time.monotonic(),
                self._generation,
            )
            self._current_size_bytes += entry_bytes

    def invalidate(self) -> None:
        """Bump the generation counter, lazily invalidating all entries.

        Called when a mutation is committed to the Context.  Existing entries
        are not deleted immediately — they are evicted on the next ``get()``
        that detects the stale generation, or during LRU eviction.
        """
        with self._lock:
            self._generation += 1

    def clear(self) -> None:
        """Remove all cached entries immediately."""
        with self._lock:
            self._entries.clear()
            self._current_size_bytes = 0

    def stats(self) -> dict[str, Any]:
        """Return cache statistics.

        Returns:
            Dict with keys: result_cache_hits, result_cache_misses,
            result_cache_hit_rate, result_cache_size_bytes,
            result_cache_size_mb, result_cache_entries,
            result_cache_evictions, result_cache_max_mb.

        """
        with self._lock:
            total = self._hits + self._misses
            return {
                "result_cache_hits": self._hits,
                "result_cache_misses": self._misses,
                "result_cache_hit_rate": (
                    self._hits / total if total > 0 else 0.0
                ),
                "result_cache_size_bytes": self._current_size_bytes,
                "result_cache_size_mb": round(
                    self._current_size_bytes / (1024 * 1024),
                    2,
                ),
                "result_cache_entries": len(self._entries),
                "result_cache_evictions": self._evictions,
                "result_cache_max_mb": round(
                    self._max_size_bytes / (1024 * 1024),
                    2,
                ),
            }


def _literal_from_python_value(val: Any) -> Any:
    """Convert a Python scalar to the matching AST Literal node.

    Args:
        val: A Python ``bool``, ``int``, ``float``, or any other value
             (converted to string).

    Returns:
        An AST ``Literal`` subclass instance suitable for use in a
        :class:`~pycypher.ast_models.Comparison` predicate.

    Note:
        ``bool`` must be checked before ``int`` because ``bool`` is a
        subclass of ``int`` in Python.

    """
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
    """Return combined cache statistics from all PyCypher caches.

    Combines metrics from:
    - ``_parse_cypher_cached`` (functools.lru_cache in ast_models)
    - ``GrammarParser._lark_cache`` (class-level Lark instance cache)
    - ``Star._result_cache`` (query result LRU cache, if *star* is provided)

    Args:
        star: Optional Star instance to include result cache stats.  When
            ``None``, result cache metrics are omitted.

    Returns:
        Dict with keys: lru_hits, lru_misses, lru_size, lru_maxsize,
        lru_hit_rate, lark_cache_hits, lark_cache_misses,
        lru_at_capacity, eviction_estimate, and (when *star* is given)
        result_cache_* keys.

    """
    from pycypher.ast_models import _parse_cypher_cached
    from pycypher.grammar_parser import GrammarParser

    info = _parse_cypher_cached.cache_info()
    lru_total = info.hits + info.misses
    at_capacity = info.maxsize is not None and info.currsize == info.maxsize
    # Eviction estimate: misses beyond maxsize indicate evictions occurred.
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

        # Simple query
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > 28 RETURN p.name ORDER BY p.name"
        )
        # Returns DataFrame: name = ["Alice", "Carol"]

        # Parameterized query with timeout
        result = star.execute_query(
            "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
            parameters={"min_age": 28},
            timeout_seconds=5.0,
        )

    Internally, Star delegates to specialized engines:

    - :class:`~pycypher.pattern_matcher.PatternMatcher` — MATCH clause translation
    - :class:`~pycypher.path_expander.PathExpander` — variable-length path BFS
    - :class:`~pycypher.mutation_engine.MutationEngine` — CREATE/SET/DELETE execution
    """

    def __init__(
        self,
        context: Context | ContextBuilder | None = None,
        *,
        result_cache_max_mb: int | None = None,
        result_cache_ttl_seconds: float | None = None,
    ) -> None:
        """Initialize Star with a data context.

        Args:
            context: PyCypher Context or ContextBuilder.  A ContextBuilder is
                automatically finalised via ``.build()``.
            result_cache_max_mb: Maximum memory (MB) for the query result
                cache.  Defaults to ``PYCYPHER_RESULT_CACHE_MAX_MB`` env var
                (100 MB).  Set to ``0`` to disable result caching.
            result_cache_ttl_seconds: Time-to-live per cached result in
                seconds.  Defaults to ``PYCYPHER_RESULT_CACHE_TTL_S`` env var
                (0 = no TTL).

        """
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
            where_fn=self._apply_where_filter,
        )

        # Delegate pattern matching to the focused PatternMatcher.
        # Injected with coerce_join and apply_where_filter from this class.
        self._pattern_matcher: PatternMatcher = PatternMatcher(
            context=context,
            path_expander=self._path_expander,
            coerce_join_fn=self._frame_joiner.coerce_join,
            apply_where_fn=self._apply_where_filter,
            multi_way_join_fn=self._frame_joiner.multi_way_join,
        )

        # Delegate expression rendering to the focused ExpressionRenderer.
        self._renderer: ExpressionRenderer = ExpressionRenderer()

        # Delegate aggregation detection and planning.
        from pycypher.aggregation_planner import AggregationPlanner

        self._agg_planner: AggregationPlanner = AggregationPlanner()

        # Delegate RETURN/WITH clause evaluation and projection modifiers.
        from pycypher.projection_planner import ProjectionPlanner

        self._projection_planner: ProjectionPlanner = ProjectionPlanner(
            agg_planner=self._agg_planner,
            renderer=self._renderer,
            where_fn=self._apply_where_filter,
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

        # Cardinality feedback store — learns from execution history to
        # correct heuristic estimates in future queries.
        from pycypher.query_planner import CardinalityFeedbackStore

        self._cardinality_feedback = CardinalityFeedbackStore()

        # Last analysis result — stored for post-execution feedback.
        self._last_analysis: Any = None

        # Pre-warm the AST cache with common query templates for the
        # entity/relationship types in this context.  This runs in a
        # background thread so Star.__init__ returns immediately.
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
        """Pre-parse common query templates to populate the AST LRU cache.

        By pre-parsing queries for the actual entity/relationship types in
        the context, subsequent user queries that match these patterns hit
        the cache instead of paying the ~10-20ms Earley parsing cost.
        """
        from pycypher.ast_converter import _parse_cypher_cached

        templates = []
        for etype in entity_types[:5]:  # cap to avoid excessive warm-up
            templates.extend(
                [
                    f"MATCH (n:{etype}) RETURN n",
                    f"MATCH (n:{etype}) RETURN count(n)",
                    f"MATCH (n:{etype}) RETURN n LIMIT 10",
                ]
            )
        for rtype in rel_types[:5]:
            for etype in entity_types[:3]:
                templates.append(
                    f"MATCH (a:{etype})-[:{rtype}]->(b) RETURN a, b",
                )

        for q in templates:
            try:
                _parse_cypher_cached(q)
            except Exception:
                pass  # Template may fail for unusual type names; ignore.

    def __repr__(self) -> str:
        """Return an informative summary for REPL/notebook display.

        Shows backend, entity types with row counts, and relationship
        types with row counts.  Example::

            Star(backend='pandas', entities={'Person': 4}, relationships={'KNOWS': 3})
        """
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
        """Return True if the parsed query contains mutation clauses.

        Args:
            parsed: A Query or UnionQuery AST node.

        Returns:
            True if the query contains CREATE, SET, DELETE, MERGE, REMOVE,
            or FOREACH clauses (i.e. it modifies data).

        """
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
        """Return a sorted list of registered scalar function names.

        Queries the :class:`~pycypher.scalar_functions.ScalarFunctionRegistry`
        singleton for all currently registered function names.  Names are
        lowercased (matching Cypher case-insensitive semantics).

        Returns:
            Sorted list of function name strings, e.g.
            ``["abs", "ceil", "cos", ..., "toupper", "trim"]``.

        """
        from pycypher.scalar_functions import ScalarFunctionRegistry

        registry = ScalarFunctionRegistry.get_instance()
        return registry.list_functions()

    def explain_query(self, query: str) -> str:
        """Return a text execution plan without running the query.

        Parses the Cypher query, walks the AST clauses, and runs the query
        planner to produce cardinality estimates, join strategies, and memory
        projections.  Similar to SQL ``EXPLAIN``.

        Args:
            query: Cypher query string.

        Returns:
            Human-readable execution plan as a multi-line string.

        Example::

            print(star.explain_query(
                "MATCH (p:Person)-[:KNOWS]->(q:Person) RETURN p.name"
            ))

        """
        from pycypher.ast_models import ASTConverter, Query, UnionQuery
        from pycypher.query_planner import QueryPlanAnalyzer

        if not query.strip():
            return "Error: empty query"

        converter = ASTConverter()
        parsed = converter.from_cypher(query)

        lines: list[str] = [
            "Execution Plan",
            "=" * 60,
            f"Query: {query.strip()!r}",
            f"Backend: {self.context.backend_name}",
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
        entities = self.context.entity_mapping.mapping
        rels = self.context.relationship_mapping.mapping
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
            parsed, self.context, feedback_store=self._cardinality_feedback,
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
        except Exception:
            LOGGER.debug("PROFILE complexity analysis failed", exc_info=True)

        # Rule-based optimizer analysis
        from pycypher.query_optimizer import QueryOptimizer

        opt_plan = QueryOptimizer.default().optimize(parsed, self.context)
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
            parameters: Optional dict of named query parameters.  Each key
                corresponds to a ``$name`` placeholder in the query.  Example::

                    star.execute_query(
                        "MATCH (p:Person) WHERE p.name = $name RETURN p.age",
                        parameters={"name": "Alice"},
                    )
            timeout_seconds: Optional wall-clock timeout in seconds.  If the
                query takes longer than this, a
                :class:`~pycypher.exceptions.QueryTimeoutError` is raised.
                ``None`` (default) means no timeout.  Falls back to the
                ``PYCYPHER_QUERY_TIMEOUT_S`` environment variable when set.
            memory_budget_bytes: Optional peak-memory budget in bytes.  The
                query planner estimates memory requirements before execution
                and raises
                :class:`~pycypher.exceptions.QueryMemoryBudgetError` if the
                estimate exceeds this value.  ``None`` (default) logs a
                warning when the estimate exceeds 2 GB but does not raise.
                Set explicitly to enforce a hard limit, e.g.
                ``memory_budget_bytes=500 * 1024 * 1024`` for 500 MB.
            max_complexity_score: Optional ceiling for the query complexity
                score computed by :func:`~pycypher.query_complexity.score_query`.
                The scorer assigns weighted points for clauses, joins,
                variable-length paths, aggregations, cross-product risk,
                etc.  If the total exceeds this value, a
                :class:`~pycypher.query_complexity.QueryComplexityError` is
                raised **before** execution begins.  ``None`` (default) skips
                complexity checking entirely.  Typical production values
                range from 50 to 200.

        Returns:
            DataFrame with columns matching the RETURN clause aliases.

        Raises:
            ValueError: If query structure is invalid, or if a query
                parameter referenced in the query (e.g. ``$min_age``) is
                not present in *parameters*.
            NotImplementedError: For unsupported clause types.
            QueryTimeoutError: If execution exceeds *timeout_seconds*.
            QueryMemoryBudgetError: If estimated memory exceeds
                *memory_budget_bytes*.
            QueryComplexityError: If complexity score exceeds
                *max_complexity_score*.

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

        # Resolve effective timeout: explicit parameter > env var > None.
        _effective_timeout = (
            timeout_seconds
            if timeout_seconds is not None
            else _DEFAULT_TIMEOUT_S
        )

        if _effective_timeout is not None and _effective_timeout < 0:
            msg = f"timeout_seconds must be non-negative, got {_effective_timeout}"
            raise ValueError(msg)

        # Inject parameters into the shared context so evaluators can access them.
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
        self.context._memory_budget_bytes = memory_budget_bytes
        _parse_elapsed_ms: float | None = None

        # Arm the per-query timeout before entering the execution path.
        self.context.set_deadline(_effective_timeout)

        # --- SIGALRM hard stop (Unix main-thread only) ---
        # The cooperative check_timeout() runs between clauses, but a single
        # clause stuck in a C extension (pandas merge, numpy sort) won't yield.
        # SIGALRM ensures the process isn't stuck forever.
        _old_alarm_handler = None
        _alarm_set = False
        if (
            _effective_timeout is not None
            and _effective_timeout >= 0
            and (
                hasattr(signal, "SIGALRM")
                and threading.current_thread() is threading.main_thread()
            )
        ):

            def _alarm_handler(signum: int, frame: Any) -> None:
                elapsed = time.perf_counter() - _t0
                raise QueryTimeoutError(
                    timeout_seconds=_effective_timeout or 0.0,
                    elapsed_seconds=elapsed,
                    query_fragment=_query_str,
                )

            _old_alarm_handler = signal.signal(
                signal.SIGALRM,
                _alarm_handler,
            )
            signal.alarm(max(1, int(_effective_timeout + 1)))
            _alarm_set = True

        # --- OpenTelemetry: wrap the entire execution in a trace span ---
        _otel_cm = trace_query(
            _query_str,
            query_id=_qid,
            parameters=parameters,
        )
        _otel_span = _otel_cm.__enter__()

        # --- Result cache: fast path for repeated read-only queries ---
        # Skip cache for queries containing non-deterministic functions.
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
            # The Pipeline abstraction decomposes the inline execution flow
            # into composable stages that users can extend with custom stages
            # (e.g. insert a lint stage between parse and validate).
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

            _pipeline = Pipeline([
                ParseStage(),
                ValidateStage(),
                ExecuteStage(),
            ])
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

            # Extract parse timing from pipeline stage timings.
            _parse_stage_time = _pipeline_result.stage_timings.get("parse", 0.0)
            _parse_elapsed_ms = _parse_stage_time * 1000.0
            self._last_parse_time_ms = _parse_elapsed_ms

            # Log complexity warnings from the validate stage.
            _effective_warn_complexity = _COMPLEXITY_WARN
            _complexity_score = _pipeline_result.metadata.get("complexity_score")
            if _complexity_score is not None:
                LOGGER.debug(
                    "execute_query: complexity score=%d  breakdown=%s",
                    _complexity_score,
                    _pipeline_result.metadata.get("complexity_details", {}),
                )
                for _cw in _pipeline_result.metadata.get("complexity_warnings", []):
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
            # Record metrics for the successful query.
            _clause_names: list[str] = (
                [
                    type(c).__name__ for c in parsed_query.clauses
                ]  # guarded by hasattr below
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
                estimated_memory_mb=self._last_estimated_memory_bytes
                / (1024.0 * 1024.0)
                if hasattr(self, "_last_estimated_memory_bytes")
                else None,
                plan_time_ms=self._last_plan_time_ms
                if hasattr(self, "_last_plan_time_ms")
                else None,
            )
            QUERY_METRICS.update_cache_stats(self._result_cache.stats())
            if isinstance(result, pd.DataFrame) and result.empty:
                LOGGER.debug(
                    "execute_query: result is empty (0 rows) for query %r",
                    _query_str[:80],
                )

            # Record OTel span attributes for successful execution.
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

            # Cache read-only results; invalidate cache after mutations.
            if _is_mutation:
                self._result_cache.invalidate()
            elif _cache_eligible and isinstance(result, pd.DataFrame):
                self._result_cache.put(query, _cache_params, result)

            return result
        except Exception as _exc:
            _elapsed = time.perf_counter() - _t0
            LOGGER.error(
                "execute_query: failed  elapsed=%.3fs  query=%r",
                _elapsed,
                _query_str[:80],
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
            # Close the OpenTelemetry trace span.
            import sys as _sys

            _ei = _sys.exc_info()
            _otel_cm.__exit__(_ei[0], _ei[1], _ei[2])
            # Restore the query-ID contextvar so it doesn't leak into
            # subsequent queries or unrelated log output.
            reset_query_id(_qid_token)
            # Cancel SIGALRM and restore previous handler.
            if _alarm_set:
                signal.alarm(0)
                if _old_alarm_handler is not None:
                    signal.signal(signal.SIGALRM, _old_alarm_handler)
            # Clear parameters after execution to avoid leaking them.
            self.context._parameters = {}
            # Disarm the timeout so it doesn't bleed into subsequent queries.
            self.context.clear_deadline()
            # Defensive: ensure shadow state is clean even if an unexpected
            # code path bypassed the normal begin/commit/rollback cycle
            # (e.g. BaseException during transaction handling).
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
        """Async wrapper around :meth:`execute_query`.

        Runs the synchronous query execution in a thread via
        :func:`asyncio.to_thread`, allowing callers to ``await`` query
        results without blocking the event loop.

        All parameters are forwarded to :meth:`execute_query`.

        Example::

            result = await star.execute_query_async(
                "MATCH (p:Person) RETURN p.name AS name"
            )

        Returns:
            DataFrame with columns matching the RETURN clause aliases.

        """
        import asyncio

        return await asyncio.to_thread(
            self.execute_query,
            query,
            parameters=parameters,
            timeout_seconds=timeout_seconds,
            memory_budget_bytes=memory_budget_bytes,
            max_complexity_score=max_complexity_score,
        )

    # =========================================================================
    # BindingFrame execution path (Phases 5–7)
    # =========================================================================

    def _apply_projection_modifiers(
        self,
        df: pd.DataFrame,
        clause: Any,
        frame: BindingFrame,
    ) -> pd.DataFrame:
        """Apply DISTINCT/ORDER BY/SKIP/LIMIT — delegates to :class:`ProjectionPlanner`."""
        return self._projection_planner.apply_projection_modifiers(
            df,
            clause,
            frame,
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

    def _plan_query(self, query: Any) -> dict[str, Any]:
        """Build a computation graph from the query AST and run optimisation passes.

        Returns execution hints derived from the lazy evaluation optimizer's
        analysis of the query structure.  These hints are used for logging,
        memory estimation, and future predicate pushdown.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            A dict with keys:

            - ``estimated_memory_bytes`` — peak memory estimate from the graph.
            - ``node_count`` — number of operation nodes in the graph.
            - ``has_filter`` — whether the query contains filter operations.
            - ``has_join`` — whether the query contains join operations.
            - ``optimized_graph`` — the graph after optimization passes.

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

    def _extract_limit_hint(self, query: Any) -> int | None:
        """Extract a LIMIT value for pushdown if the query pattern is safe.

        Returns the integer LIMIT value when the query has the simple pattern
        ``MATCH ... RETURN ... LIMIT N`` (no aggregation, no DISTINCT, no
        ORDER BY, no SKIP, no WITH).  Returns ``None`` when pushdown is
        unsafe.

        This is a conservative heuristic — it only enables pushdown when we
        are *certain* that limiting rows during MATCH will produce the same
        final result as computing all rows and truncating at the end.

        Args:
            query: Parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            Integer limit for pushdown, or ``None`` if pushdown is unsafe.

        """
        from pycypher.ast_models import Match, Return, With

        clauses = query.clauses
        if not clauses:
            return None

        # Only push down for simple MATCH → RETURN (possibly with WHERE)
        # Reject if there's a WITH clause (changes row semantics)
        match_count = sum(1 for c in clauses if isinstance(c, Match))
        with_count = sum(1 for c in clauses if isinstance(c, With))
        return_clauses = [c for c in clauses if isinstance(c, Return)]

        if match_count != 1 or with_count > 0 or len(return_clauses) != 1:
            return None

        ret = return_clauses[0]

        # Reject if DISTINCT, ORDER BY, or SKIP are present
        if ret.distinct or ret.order_by or ret.skip is not None:
            return None

        # Reject if LIMIT is absent
        if ret.limit is None:
            return None

        # Reject if any ReturnItem contains an aggregation
        for item in ret.items:
            if item.expression is not None and self._contains_aggregation(
                item.expression,
            ):
                return None

        # Extract the integer limit value
        limit_val = ret.limit
        if isinstance(limit_val, int):
            return limit_val

        # If it's an expression (e.g. parameter), we can't resolve it at
        # planning time — skip pushdown
        return None

    def _unwind_binding_frame(
        self,
        clause: Any,
        frame: BindingFrame,
    ) -> BindingFrame:
        """Evaluate an UNWIND clause and return the exploded BindingFrame.

        Each row in *frame* that contains a list value in the expression is
        expanded into one row per list element, with the element bound to
        ``clause.alias``.  All other columns from *frame* are preserved via
        pandas :meth:`~pandas.DataFrame.explode`.

        Args:
            clause: AST :class:`~pycypher.ast_models.Unwind` node.
            frame: Current :class:`~pycypher.binding_frame.BindingFrame`.

        Returns:
            A new :class:`~pycypher.binding_frame.BindingFrame` whose rows
            are the Cartesian expansion of *frame* × list elements.

        """
        from pycypher.binding_evaluator import BindingExpressionEvaluator

        alias: str = clause.alias or "_unwind_col"
        evaluator = BindingExpressionEvaluator(frame)
        list_series = evaluator.evaluate(clause.expression).reset_index(
            drop=True,
        )

        # Guard against memory exhaustion: check max list size before explode.
        from pycypher.config import MAX_COLLECTION_SIZE
        from pycypher.exceptions import SecurityError

        max_list_len = 0
        for val in list_series:
            if isinstance(val, (list, tuple)):
                max_list_len = max(max_list_len, len(val))
        if max_list_len > MAX_COLLECTION_SIZE:
            msg = (
                f"UNWIND list contains {max_list_len:,} elements, "
                f"exceeding limit of {MAX_COLLECTION_SIZE:,}. "
                f"Adjust PYCYPHER_MAX_COLLECTION_SIZE to increase."
            )
            raise SecurityError(msg)

        # PERFORMANCE: Use assign() instead of copy() for adding single column.
        # Skip reset_index when already contiguous — avoids a full copy.
        bindings = frame.bindings
        idx = bindings.index
        if not (
            isinstance(idx, pd.RangeIndex)
            and idx.start == 0
            and idx.step == 1
            and idx.stop == len(bindings)
        ):
            bindings = bindings.reset_index(drop=True)
        df = bindings.assign(**{alias: list_series})

        # explode with ignore_index=True already produces clean 0-based index.
        df = df.explode(alias, ignore_index=True)

        # pandas explode() converts empty lists to a single NaN row; drop those.
        df = df.dropna(subset=[alias])
        idx = df.index
        if not (
            isinstance(idx, pd.RangeIndex)
            and idx.start == 0
            and idx.step == 1
            and idx.stop == len(df)
        ):
            df = df.reset_index(drop=True)

        return BindingFrame(
            bindings=df,
            type_registry=frame.type_registry,
            context=frame.context,
        )

    def _apply_where_filter(
        self,
        where_expr: Any,
        result_frame: BindingFrame,
        fallback_frame: BindingFrame | None = None,
    ) -> BindingFrame:
        """Apply a WHERE predicate to *result_frame*, with optional fallback.

        First tries evaluating *where_expr* against *result_frame* (handles
        projected aliases).  If that raises ``ValueError`` or ``KeyError``
        (expression references variables not in the projected frame), falls
        back to evaluating against *fallback_frame* and applying the boolean
        mask positionally to *result_frame*.

        When *fallback_frame* is ``None``, uses
        :class:`~pycypher.binding_frame.BindingFilter` directly (no fallback).

        Args:
            where_expr: AST expression node for the WHERE predicate.
            result_frame: The frame to filter.
            fallback_frame: Optional pre-projection frame for variable lookup.

        Returns:
            A new filtered :class:`~pycypher.binding_frame.BindingFrame`.

        """
        from pycypher.binding_evaluator import (
            BindingExpressionEvaluator as _BEE,
        )
        from pycypher.binding_frame import BindingFilter

        if fallback_frame is None:
            return BindingFilter(predicate=where_expr).apply(result_frame)

        try:
            _mask = _BEE(result_frame).evaluate(where_expr).fillna(False)
            return result_frame.filter(_mask)
        except (ValueError, KeyError):
            LOGGER.debug(
                "WHERE: evaluation failed on result frame, falling back to pre-projection frame",
            )
            _pre_mask = _BEE(fallback_frame).evaluate(where_expr).fillna(False)
            return result_frame.filter(_pre_mask)

    def _with_to_binding_frame(
        self,
        with_clause: Any,
        frame: BindingFrame,
    ) -> BindingFrame:
        """Translate a WITH clause — delegates to :class:`ProjectionPlanner`."""
        return self._projection_planner.with_to_binding_frame(
            with_clause,
            frame,
        )

    # Mutation clause wrappers removed — calls inlined directly in the
    # clause loop via self._mutations.* methods.

    def _execute_union_query(self, union_query: Any) -> pd.DataFrame:
        """Execute a UNION [ALL] query — run each component query and combine.

        For ``UNION`` (without ALL), duplicate rows are removed from the final
        result.  For ``UNION ALL``, all rows from all sub-queries are kept.

        The entire UNION is wrapped in a single transaction so that a
        failure in any sub-query rolls back mutations from *all* earlier
        sub-queries, preserving atomicity.

        Args:
            union_query: A :class:`~pycypher.ast_models.UnionQuery` AST node.

        Returns:
            A ``pd.DataFrame`` combining all sub-query results.

        """
        self.context.begin_query()
        _committed = False
        try:
            frames: list[pd.DataFrame] = []
            for stmt in union_query.statements:
                frames.append(
                    self._execute_query_binding_frame_inner(stmt),
                )

            if not frames:
                self.context.commit_query()
                _committed = True
                return pd.DataFrame()

            combined = pd.concat(frames, ignore_index=True)

            # Determine whether any join was "ALL" — if any flag is False, we need
            # to deduplicate.  Simplest correct behaviour: if ALL flags are True
            # this is a pure UNION ALL; otherwise deduplicate the full result.
            any_union = any(not flag for flag in union_query.all_flags)
            if any_union:
                combined = combined.drop_duplicates().reset_index(drop=True)

            self.context.commit_query()
            _committed = True
            return combined
        finally:
            if not _committed:
                self.context.rollback_query()

    def _execute_query_binding_frame(
        self,
        query: Any,
    ) -> pd.DataFrame:
        """Execute a Cypher query with query-scoped shadow write atomicity.

        Wraps :meth:`_execute_query_binding_frame_inner` in a
        begin / commit / rollback transaction so that a failed query never
        leaves the context in a partially-mutated state.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            A ``pd.DataFrame`` with columns matching the RETURN aliases.

        """
        self.context.begin_query()
        _committed = False
        try:
            result = self._execute_query_binding_frame_inner(
                query,
            )
            self.context.commit_query()
            _committed = True
            return result
        finally:
            if not _committed:
                self.context.rollback_query()

    # ------------------------------------------------------------------
    # Private helpers for _execute_query_binding_frame_inner
    # ------------------------------------------------------------------

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
            current_frame,
            match_frame,
            where_clause,
        )

    def _coerce_join(self, frame_a: Any, frame_b: Any) -> Any:
        """Join two BindingFrames — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.coerce_join(frame_a, frame_b)

    def _make_seed_frame(self) -> Any:
        """Create a seed frame — delegates to :class:`FrameJoiner`."""
        return self._frame_joiner.make_seed_frame()

    def _process_unwind_clause(self, clause: Any, current_frame: Any) -> Any:
        """Execute an UNWIND clause, seeding a synthetic frame if needed.

        When UNWIND appears without a preceding MATCH (standalone), there is
        no existing frame.  A single-row seed frame is created so the
        expression evaluator has something to iterate over.  The synthetic
        ``_seed`` column is removed after unwinding.

        Args:
            clause: The :class:`~pycypher.ast_models.Unwind` clause.
            current_frame: The preceding
                :class:`~pycypher.binding_frame.BindingFrame`, or ``None`` if
                UNWIND is the first clause.

        Returns:
            A new :class:`~pycypher.binding_frame.BindingFrame` with the
            unwound variable bound and the seed column (if any) stripped.

        """
        if current_frame is None:
            # Standalone UNWIND (no preceding MATCH) — seed with a
            # single-row empty frame so the expression evaluator has
            # something to operate against.
            current_frame = self._make_seed_frame()

        result = self._unwind_binding_frame(clause, current_frame)

        # Strip the synthetic seed column if it survived into the result.
        if "_row" in result.bindings.columns:
            result = BindingFrame(
                bindings=result.bindings.drop(columns=["_row"]),
                type_registry=result.type_registry,
                context=result.context,
            )
        return result

    def _log_clause_performance(
        self,
        clause_name: str,
        elapsed: float,
        size_before: str,
        size_after: str,
    ) -> None:
        """Log debug information for clause execution performance."""
        LOGGER.debug(
            "clause %s  elapsed=%.3fs  frame_before=%s  frame_after=%s",
            clause_name,
            elapsed,
            size_before,
            size_after,
        )

    def _handle_clause_result(
        self,
        result: Any,
        clause_name: str,
        elapsed: float,
        size_before: str,
        clause_timings: dict[str, float],
    ) -> pd.DataFrame | None:
        """Handle clause execution result, returning DataFrame if early exit needed.

        Returns:
            pd.DataFrame if the clause result triggers early return (RETURN clause),
            None if execution should continue with the next clause.

        """
        if isinstance(result, pd.DataFrame):
            self._log_clause_performance(
                clause_name,
                elapsed,
                size_before,
                str(len(result)),
            )
            self._last_clause_timings = clause_timings
            self._record_cardinality_feedback(len(result))
            return result

        return None

    def _record_cardinality_feedback(self, actual_rows: int) -> None:
        """Record actual row count into the cardinality feedback store.

        Uses the last analysis result to compare estimated vs actual
        cardinality per entity type, building up correction factors for
        future queries.
        """
        analysis = self._last_analysis
        if analysis is None:
            return

        # Record overall estimated vs actual for each entity type in the
        # last analysis.  clause_cardinalities[-1] is the final estimate.
        if analysis.clause_cardinalities:
            final_estimate = analysis.clause_cardinalities[-1]
            # Record feedback for each entity/relationship type seen in
            # the analysis join plans.
            entity_types_seen: set[str] = set()
            for jp in analysis.join_plans:
                entity_types_seen.add(jp.left_name)
                entity_types_seen.add(jp.right_name)
            for et in entity_types_seen:
                self._cardinality_feedback.record(
                    et, final_estimate, actual_rows,
                )

        self._last_analysis = None

    # ------------------------------------------------------------------
    # Query planning and clause dispatch helpers
    # ------------------------------------------------------------------

    def _analyze_and_plan(self, query: Any) -> int | None:
        """Run query planning, memory budget enforcement, and LIMIT pushdown.

        Performs all pre-execution analysis:

        * Builds a lazy computation graph via :meth:`_plan_query` for
          memory estimates and structural analysis.
        * Runs :class:`~pycypher.query_optimizer.QueryOptimizer` to
          produce an :class:`~pycypher.query_optimizer.OptimizationPlan`
          with actionable hints (filter pushdown, limit pushdown, join
          reordering, predicate simplification).
        * Runs :class:`~pycypher.query_planner.QueryPlanAnalyzer` for
          cardinality estimates, join strategies, and pushdown opportunities.
        * Enforces the memory budget (hard error with explicit budget,
          warning otherwise).
        * Extracts a LIMIT pushdown hint when the query pattern is safe.

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.

        Returns:
            An optional row-limit hint for MATCH pushdown, or ``None``.

        Raises:
            QueryMemoryBudgetError: If an explicit memory budget is exceeded.

        """
        # --- Lazy evaluation planning phase ---
        _plan_t0 = time.perf_counter()
        _plan_hints = self._plan_query(query)
        _plan_elapsed_ms = (time.perf_counter() - _plan_t0) * 1000.0
        self._last_plan_time_ms = _plan_elapsed_ms
        self._last_estimated_memory_bytes = _plan_hints.get(
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

        _opt_plan = QueryOptimizer.default().optimize(query, self.context)
        self._last_optimization_plan = _opt_plan
        if _opt_plan.applied_rules:
            LOGGER.debug(
                "optimizer: %d rule(s) applied (%s), estimated speedup %.2fx in %.2fms",
                len(_opt_plan.applied_rules),
                ", ".join(_opt_plan.applied_rules),
                _opt_plan.total_estimated_speedup,
                _opt_plan.elapsed_ms,
            )

        # --- Query planner analysis ---
        from pycypher.query_planner import QueryPlanAnalyzer

        _analysis = QueryPlanAnalyzer(
            query, self.context, feedback_store=self._cardinality_feedback,
        ).analyze()
        self._last_analysis = _analysis
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

        # Pass pre-computed join plans to FrameJoiner so BindingFrame.join()
        # uses them directly instead of re-planning each join.
        if _analysis.join_plans:
            self._frame_joiner.set_join_plans(_analysis.join_plans)

        # Memory budget enforcement.
        _budget = (
            self.context._memory_budget_bytes
            if self.context._memory_budget_bytes is not None
            else 2 * 1024 * 1024 * 1024
        )
        if _analysis.exceeds_budget(budget_bytes=_budget):
            if self.context._memory_budget_bytes is not None:
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
        self._apply_match_reordering(query)

        # --- LIMIT pushdown hint ---
        # Use the optimizer's limit_pushdown_value hint if it's a concrete
        # integer (parameterized LIMIT produces a Parameter AST node, not int).
        # Fall back to the manual extraction for safety.
        _opt_limit = _opt_plan.hints.get("limit_pushdown_value")
        _limit_hint = _opt_limit if isinstance(_opt_limit, int) else None
        if _limit_hint is None:
            _limit_hint = self._extract_limit_hint(query)
        if _limit_hint is not None:
            LOGGER.debug(
                "LIMIT pushdown hint: %d rows",
                _limit_hint,
            )
        return _limit_hint

    def _apply_match_reordering(self, query: Any) -> None:
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

        # Find runs of consecutive MATCH clauses (non-OPTIONAL only).
        i = 0
        reordered = False
        while i < len(clauses):
            # Start of a consecutive MATCH run?
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
                run_end = i  # exclusive

                if run_end - run_start >= 2:
                    run = clauses[run_start:run_end]

                    # Skip reordering if any MATCH in the run uses
                    # shortestPath / allShortestPaths — those patterns
                    # rely on variables pre-bound by preceding MATCHes.
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

                    # Collect variables defined by each MATCH in the run.

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
                        """Variable names referenced in an expression tree.

                        Delegates to the canonical
                        :func:`~pycypher.ast_models.extract_referenced_variables`.
                        """
                        from pycypher.ast_models import (
                            ASTNode,
                            extract_referenced_variables,
                        )

                        if expr is None:
                            return set()
                        if isinstance(expr, ASTNode):
                            return extract_referenced_variables(expr)
                        return set()

                    # Skip reordering if any WHERE clause references
                    # variables defined by *other* MATCHes in the run.
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

                    # Estimate cardinalities for each MATCH in the run.
                    estimates = []
                    for idx, match in enumerate(run):
                        est = JoinReorderingRule._estimate_match_cardinality(
                            match,
                            self.context,
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

    def _handle_match_clause(
        self,
        clause: Any,
        current_frame: Any,
        limit_hint: int | None,
    ) -> Any | pd.DataFrame:
        """Execute a MATCH or OPTIONAL MATCH clause.

        Handles three sub-cases:

        * **OPTIONAL MATCH with existing frame** — delegates to
          :meth:`_process_optional_match` for left-join semantics.
        * **Regular MATCH (first clause)** — creates the initial
          BindingFrame from pattern matching.
        * **Regular MATCH (subsequent)** — merges with the existing
          frame via :meth:`_merge_frames_for_match`.

        Args:
            clause: The :class:`~pycypher.ast_models.Match` clause.
            current_frame: The preceding BindingFrame, or ``None``.
            limit_hint: Optional row limit for MATCH pushdown.

        Returns:
            The updated BindingFrame, or an empty ``pd.DataFrame`` when
            an OPTIONAL MATCH as first clause finds no matches.

        """
        if clause.optional and current_frame is not None:
            return self._process_optional_match(clause, current_frame)

        try:
            match_frame = self._pattern_matcher.match_to_binding_frame(
                clause,
                context_frame=current_frame,
                row_limit=limit_hint,
            )
        except ValueError:
            LOGGER.debug(
                "MATCH: ValueError during pattern matching (optional=%s)",
                clause.optional,
            )
            if clause.optional:
                return pd.DataFrame()
            raise

        if current_frame is None:
            return match_frame
        return self._merge_frames_for_match(
            current_frame,
            match_frame,
            clause.where,
        )

    @staticmethod
    def _require_bound_frame(current_frame: Any, clause_name: str) -> None:
        """Raise ``ValueError`` if *current_frame* is ``None``.

        Used by mutation clauses (SET, REMOVE, DELETE) that require a
        preceding MATCH or CREATE to bind variables.

        Args:
            current_frame: The current BindingFrame (or None).
            clause_name: Human-readable clause name for the error message.

        Raises:
            ValueError: When *current_frame* is None.

        """
        if current_frame is None:
            msg = (
                f"{clause_name} clause requires a preceding MATCH or CREATE clause "
                f"to bind variables."
            )
            raise ValueError(msg)

    @staticmethod
    def _frame_size(frame: Any) -> str:
        """Return row count string for a BindingFrame, or ``'(none)'``."""
        if frame is None:
            return "(none)"
        try:
            return str(len(frame.bindings))
        except AttributeError:
            return "(unknown)"

    def _dispatch_clause(
        self,
        clause: Any,
        current_frame: Any,
        limit_hint: int | None,
    ) -> Any:
        """Dispatch a single clause and return the updated frame.

        Returns a ``pd.DataFrame`` for RETURN clauses and early-exit
        OPTIONAL MATCH, or a BindingFrame / ``None`` for all other clauses.

        """
        from pycypher.ast_models import (
            Call,
            Create,
            Delete,
            Foreach,
            Match,
            Merge,
            Remove,
            Return,
            Set,
            Unwind,
            With,
        )

        if isinstance(clause, Match):
            result = self._handle_match_clause(
                clause,
                current_frame,
                limit_hint,
            )
            # _handle_match_clause returns pd.DataFrame for OPTIONAL
            # MATCH-as-first-clause with no matches — signal early exit.
            if isinstance(result, pd.DataFrame):
                return result
            return result

        if isinstance(clause, With):
            if current_frame is None:
                current_frame = self._make_seed_frame()
            return self._with_to_binding_frame(clause, current_frame)

        if isinstance(clause, Return):
            if current_frame is None:
                current_frame = self._make_seed_frame()
            return self._return_from_frame(clause, current_frame)

        if isinstance(clause, Set):
            self._require_bound_frame(current_frame, "SET")
            self._mutations.set_properties(clause, current_frame)
            return current_frame

        if isinstance(clause, Remove):
            self._require_bound_frame(current_frame, "REMOVE")
            self._mutations.remove_properties(clause, current_frame)
            return current_frame

        if isinstance(clause, Delete):
            self._require_bound_frame(current_frame, "DELETE")
            self._mutations.process_delete(clause, current_frame)
            return current_frame

        if isinstance(clause, Unwind):
            return self._process_unwind_clause(clause, current_frame)

        if isinstance(clause, Create):
            return self._mutations.process_create(
                clause,
                current_frame,
                make_seed_frame=self._make_seed_frame,
            )

        if isinstance(clause, Call):
            return self._mutations.process_call(clause, current_frame)

        if isinstance(clause, Merge):
            return self._mutations.process_merge(
                clause,
                current_frame,
                match_to_binding_frame=self._pattern_matcher.match_to_binding_frame,
                merge_frames_for_match=self._merge_frames_for_match,
                make_seed_frame=self._make_seed_frame,
            )

        if isinstance(clause, Foreach):
            return self._mutations.process_foreach(
                clause,
                current_frame,
                make_seed_frame=self._make_seed_frame,
            )

        msg = (
            f"Clause type '{type(clause).__name__}' is not yet supported "
            "in the BindingFrame execution path."
        )
        raise NotImplementedError(msg)

    def _execute_query_binding_frame_inner(
        self,
        query: Any,
        initial_frame: Any = None,
    ) -> pd.DataFrame:
        """Execute a Cypher query using the BindingFrame IR.

        This is the new execution path that eliminates the PREFIXED_ENTITY /
        HASH_ID / ALIAS column-name fragility of the legacy pipeline.  It
        handles:

        * MATCH + WHERE (with anonymous and named relationships)
        * WITH (simple projection, full aggregation, grouped aggregation;
          plus DISTINCT, ORDER BY, SKIP, LIMIT modifiers)
        * SET (property write-back via ``BindingFrame.mutate``)
        * RETURN (expression projection including ``CaseExpression``;
          plus DISTINCT, ORDER BY, SKIP, LIMIT modifiers)

        Unsupported patterns raise :exc:`NotImplementedError` so the caller
        can fall back to the legacy path.

        Clause-specific details are delegated to focused private helpers:

        * :meth:`_analyze_and_plan` — query planning, memory budget, LIMIT pushdown
        * :meth:`_handle_match_clause` — MATCH / OPTIONAL MATCH dispatch
        * :meth:`_require_bound_frame` — null-frame guard for mutation clauses
        * :meth:`_process_optional_match` — OPTIONAL MATCH left-join semantics
        * :meth:`_merge_frames_for_match` — keyed or cross-join for subsequent MATCH clauses
        * :meth:`_process_unwind_clause` — UNWIND with seed-frame management

        Args:
            query: A parsed :class:`~pycypher.ast_models.Query` AST node.
            initial_frame: Optional pre-seeded BindingFrame for correlated
                subqueries.

        Returns:
            A ``pd.DataFrame`` with columns matching the RETURN aliases.

        Raises:
            NotImplementedError: For variable-length paths, cross-product MATCH,
                or other unsupported patterns.
            ValueError: For structural problems (no MATCH, no RETURN, etc.).

        """
        if not query.clauses:
            msg = (
                "Query must have at least one clause (e.g. MATCH, RETURN, CREATE). "
                "Example: MATCH (n:Person) RETURN n.name"
            )
            raise ValueError(msg)

        _clause_timings: dict[str, float] = {}
        _limit_hint = self._analyze_and_plan(query)
        current_frame: Any = initial_frame

        # --- Dead column elimination: compute live columns per clause ---
        from pycypher.lazy_eval import compute_live_columns

        _live_columns = compute_live_columns(query.clauses)

        for clause_idx, clause in enumerate(query.clauses):
            self.context.check_timeout()

            _clause_name = type(clause).__name__
            _size_before = self._frame_size(current_frame)
            _clause_t0 = time.perf_counter()

            result = self._dispatch_clause(
                clause,
                current_frame,
                _limit_hint,
            )

            _clause_elapsed = time.perf_counter() - _clause_t0
            _clause_timings[_clause_name] = (
                _clause_timings.get(_clause_name, 0.0)
                + _clause_elapsed * 1000.0
            )

            # Check if this is a final result (DataFrame) requiring early return
            early_result = self._handle_clause_result(
                result,
                _clause_name,
                _clause_elapsed,
                _size_before,
                _clause_timings,
            )
            if early_result is not None:
                return early_result

            # --- Dead column elimination ---
            # Drop columns not needed by any subsequent clause.
            _live = _live_columns[clause_idx]
            if _live is not None and hasattr(result, "bindings"):
                from pycypher.binding_frame import PATH_HOP_COLUMN_PREFIX

                def _is_live(col: str) -> bool:
                    if col in _live:
                        return True
                    # Keep _path_hop_X columns when path var X is live
                    if col.startswith(PATH_HOP_COLUMN_PREFIX):
                        path_var = col[len(PATH_HOP_COLUMN_PREFIX):]
                        return path_var in _live
                    return False

                _dead_cols = [
                    c for c in result.bindings.columns if not _is_live(c)
                ]
                if _dead_cols:
                    LOGGER.debug(
                        "dead column elimination after %s: dropping %s",
                        _clause_name,
                        _dead_cols,
                    )
                    result = BindingFrame(
                        bindings=result.bindings.drop(columns=_dead_cols),
                        type_registry={
                            k: v
                            for k, v in result.type_registry.items()
                            if k not in _dead_cols
                        },
                        context=result.context,
                    )

            # Continue with next clause - log performance and update current frame
            current_frame = result
            self._log_clause_performance(
                _clause_name,
                _clause_elapsed,
                _size_before,
                self._frame_size(current_frame),
            )

        # Mutation queries (DELETE, SET, CREATE without RETURN) are valid — return
        # an empty DataFrame indicating successful execution with no output rows.
        self._last_clause_timings = _clause_timings
        self._record_cardinality_feedback(0)
        return pd.DataFrame()
