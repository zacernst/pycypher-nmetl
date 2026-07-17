Constants and Magic Values Reference
====================================

This document explains every configurable constant and internal magic value
in PyCypher, including the rationale for each default.

.. contents:: On this page
   :local:
   :depth: 2


Environment-Configurable Constants
-----------------------------------

These constants are read from environment variables at import time via
``pycypher.config``.  All can be overridden without code changes.

Query Execution Limits
~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 10 10 60
   :header-rows: 1

   * - Env Variable
     - Default
     - Module
     - Rationale
   * - ``PYCYPHER_QUERY_TIMEOUT_S``
     - None
     - ``config``
     - Wall-clock budget for a single query (seconds).  None means no limit.
       Uses ``signal.SIGALRM`` on Unix.  Set this in production to prevent
       runaway queries from holding resources indefinitely.
   * - ``PYCYPHER_MAX_CROSS_JOIN_ROWS``
     - 1,000,000
     - ``config``
     - Hard ceiling on cross-join (Cartesian product) result rows.  Prevents
       accidental memory exhaustion from ``MATCH (a), (b)`` patterns.
       1M rows is ~8 MB for ID-only frames; well within typical memory budgets.
   * - ``PYCYPHER_MAX_UNBOUNDED_PATH_HOPS``
     - 20
     - ``config``
     - Maximum BFS depth for unbounded variable-length paths (``[*]``).
       20 hops prevents runaway expansion on cyclic graphs while allowing
       traversal of most practical graph structures.  Social networks
       (diameter ~6) and supply chains (depth ~10) are well within this limit.
   * - ``PYCYPHER_MAX_COMPLEXITY_SCORE``
     - None (0)
     - ``config``
     - Hard gate on query complexity score.  Queries exceeding this are
       rejected before execution.  None disables the gate.  Typical
       production values: 50-200.
   * - ``PYCYPHER_COMPLEXITY_WARN_THRESHOLD``
     - None (0)
     - ``config``
     - Soft threshold for complexity warnings.  Queries scoring above this
       emit a warning but still execute.  Set lower than
       ``MAX_COMPLEXITY_SCORE`` for early alerts.

Security Limits
~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 10 10 60
   :header-rows: 1

   * - Env Variable
     - Default
     - Module
     - Rationale
   * - ``PYCYPHER_MAX_QUERY_SIZE_BYTES``
     - 1,048,576 (1 MiB)
     - ``config``
     - Rejects queries exceeding this size before parsing.  Prevents DoS
       via oversized query strings.  1 MiB accommodates even very large
       Cypher queries while protecting against adversarial payloads.
   * - ``PYCYPHER_MAX_QUERY_NESTING_DEPTH``
     - 200
     - ``config``
     - Maximum AST traversal depth.  Prevents stack exhaustion on deeply
       nested WHERE clauses or subqueries.  200 is generous for legitimate
       queries (typical nesting: 5-20 levels).
   * - ``PYCYPHER_MAX_COLLECTION_SIZE``
     - 1,000,000
     - ``config``
     - Ceiling on generated collection/string sizes from ``range()``,
       ``repeat()``, ``lpad()``, ``rpad()``, and ``UNWIND``.  Prevents
       memory exhaustion from adversarial inputs like ``range(0, 999999999)``.
   * - ``PYCYPHER_RATE_LIMIT_QPS``
     - 0 (disabled)
     - ``config``
     - Maximum sustained queries per second.  When enabled, excess queries
       receive a ``RateLimitError``.
   * - ``PYCYPHER_RATE_LIMIT_BURST``
     - 10
     - ``config``
     - Burst allowance for rate limiting.  Allows short bursts above the
       sustained QPS rate.  Only meaningful when rate limiting is enabled.

Caching
~~~~~~~

.. list-table::
   :widths: 20 10 10 60
   :header-rows: 1

   * - Env Variable
     - Default
     - Module
     - Rationale
   * - ``PYCYPHER_RESULT_CACHE_MAX_MB``
     - 100
     - ``config``
     - Maximum in-memory size for the query result cache.  100 MB balances
       cache hit rates against memory pressure.  LRU eviction removes
       least-recently-used entries when the limit is reached.
   * - ``PYCYPHER_RESULT_CACHE_TTL_S``
     - 0 (no expiry)
     - ``config``
     - Time-to-live for cached results.  0 means entries are only evicted by
       size pressure.  Set a TTL when the underlying data changes between
       queries.
   * - ``PYCYPHER_AST_CACHE_MAX``
     - 1,024
     - ``config``
     - LRU cache size for parsed ASTs.  1,024 entries accommodate typical
       application workloads with repeated query patterns.  Set to 0 to
       disable caching (useful during grammar development).


Internal Constants (Not Configurable)
--------------------------------------

These constants are defined in source code and require code changes to
modify.

Cardinality Estimator
~~~~~~~~~~~~~~~~~~~~~

Constants defined in ``cardinality_estimator.py`` (extracted from
``query_planner.py`` in 2026-03, see :doc:`../adr/adr-007-cardinality-estimator-extraction`).

.. list-table::
   :widths: 25 10 65
   :header-rows: 1

   * - Constant
     - Value
     - Rationale
   * - ``STATS_SAMPLE_SIZE``
     - 10,000
     - Maximum rows sampled when computing column statistics.  10K rows
       provides statistically meaningful NDV and null-fraction estimates
       while keeping computation fast on large tables.
   * - ``HISTOGRAM_BINS``
     - 64
     - Number of equi-width bins for histogram-based range selectivity.
       64 bins balances granularity against memory overhead (~512 bytes
       per histogram).
   * - ``HISTOGRAM_MIN_ROWS``
     - 10
     - Minimum non-null rows required to build a histogram.  Below this
       count, histograms are unreliable and the estimator falls back to
       uniform distribution assumptions.
   * - ``DEFAULT_FILTER_SELECTIVITY``
     - 0.33
     - Default fraction of rows passing a WHERE filter when no statistics
       are available.  Same value as ``_DEFAULT_FILTER_SELECTIVITY`` in the
       query planner (see below).
   * - ``AVG_BYTES_PER_CELL``
     - 64
     - Conservative per-cell memory estimate for mixed-type columns when
       the actual DataFrame is unavailable.  Used for memory budgeting in
       query planning.

Query Planner
~~~~~~~~~~~~~

.. list-table::
   :widths: 25 10 65
   :header-rows: 1

   * - Constant
     - Value
     - Rationale
   * - ``_DEFAULT_FILTER_SELECTIVITY``
     - 0.33
     - Default fraction of rows that pass a WHERE filter when no statistics
       are available.  0.33 (one-third) is a standard database heuristic:
       conservative enough to avoid over-optimistic plan choices, while not
       so pessimistic that it prevents useful optimisations.  Used for
       equality checks, comparisons, string predicates, and IS NULL tests.
       Override by recording actual selectivity via the query learning system.
   * - ``_BROADCAST_THRESHOLD``
     - 10,000
     - Row count below which the smaller side of a join is broadcast to
       all partitions.  10K rows fit comfortably in L2 cache (~256 KB for
       8-byte IDs), making broadcast join faster than hash partitioning
       for small-vs-large joins.
   * - ``_MERGE_SORTED_THRESHOLD``
     - 0.8
     - Fraction of data that must be pre-sorted to trigger a merge-sort join
       instead of hash join.  0.8 means at least 80% sorted — merge join
       only wins when data is mostly ordered.
   * - ``_STREAMING_AGG_THRESHOLD``
     - 10,000,000
     - Row count above which aggregation switches to streaming (chunked)
       mode.  10M rows is approximately when a single-pass aggregation
       starts competing with available memory.
   * - ``_CROSS_JOIN_WARNING_THRESHOLD``
     - 100,000
     - Output row count above which the planner emits a warning for
       cross-join operations.  Separate from the hard ceiling
       (``MAX_CROSS_JOIN_ROWS``) — this is an advisory threshold.

Path Expansion (BFS)
~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 10 65
   :header-rows: 1

   * - Constant
     - Value
     - Rationale
   * - ``_MAX_UNBOUNDED_PATH_HOPS``
     - 20
     - Module-level default for the configurable env var.  See above.
   * - ``_MAX_FRONTIER_ROWS``
     - 1,000,000
     - Maximum BFS frontier size (rows) at any single hop level.
       Prevents memory explosion during breadth-first path expansion.
       1M rows is ~8 MB of IDs, keeping BFS within reasonable memory bounds.
   * - ``_MAX_BFS_TOTAL_ROWS``
     - 5,000,000
     - Maximum total accumulated result rows across all BFS hops.
       5M rows (~40 MB of IDs) is the overall safety valve for path
       expansion.

Query Learning
~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 10 65
   :header-rows: 1

   * - Constant
     - Value
     - Rationale
   * - ``_MAX_SELECTIVITY_HISTORY``
     - 64
     - Rolling window size for selectivity observations per
       (entity_type, property, operator) triple.  64 provides enough
       samples for stable exponential moving averages without unbounded
       memory growth.
   * - ``_MAX_JOIN_HISTORY``
     - 64
     - Rolling window size for join performance observations per
       (strategy, size_bucket) pair.  Same rationale as selectivity history.
   * - ``_MAX_PLAN_CACHE``
     - 256
     - Maximum entries in the adaptive plan cache (keyed by query
       fingerprint).  256 covers typical application workloads with
       ~50-100 distinct query shapes.
   * - ``_CONFIDENCE_THRESHOLD``
     - 0.5
     - Minimum confidence score before learned selectivity overrides the
       default heuristic.  0.5 ensures at least moderate statistical
       evidence before changing plan decisions.
   * - ``_MAX_HISTORY``
     - 32
     - Cardinality estimator rolling window per entity type.  32
       observations balance recency with stability.

Other Internal Limits
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 10 15 50
   :header-rows: 1

   * - Constant
     - Value
     - Module
     - Rationale
   * - ``_MAX_REGEX_PATTERN_LENGTH``
     - 1,000
     - ``binding_evaluator``, ``string_predicate_evaluator``
     - Maximum regex pattern length for ``=~`` operator.  Prevents ReDoS
       (Regular Expression Denial of Service) attacks via adversarial
       patterns.
   * - ``_MAX_QUERIES``
     - 1,000
     - ``multi_query_analyzer``
     - Maximum queries in a multi-query dependency analysis batch.  Bounds
       the O(n^2) dependency inference cost.
   * - ``_MAX_NAME_GENERATION_ATTEMPTS``
     - 10,000
     - ``variable_manager``
     - Safety limit for generating unique variable names.  10K attempts
       ensures name generation succeeds even in heavily aliased queries.
   * - ``_MAX_CONTENT_LENGTH``
     - 10 MiB
     - ``cypher_lsp``
     - Maximum LSP JSON-RPC payload size.  Rejects oversized payloads
       before parsing to prevent memory exhaustion.
   * - ``_MAX_DOCUMENTS``
     - 128
     - ``cypher_lsp``
     - Maximum concurrent open documents in the LSP server.  Evicts
       oldest documents beyond this limit.
   * - ``_MAX_QUERY_LOG_LEN``
     - 200
     - ``grammar_parser``
     - Truncation limit for query strings in log messages.  Keeps logs
       readable without losing diagnostic value.
   * - ``_MAX_HISTORY``
     - 1,000
     - ``repl``
     - Maximum readline history entries in the REPL.
   * - ``_MAX_QUERY_LENGTH``
     - 2,048
     - ``audit``
     - Truncation limit for query strings in audit log records.  Longer
       than log truncation to preserve audit trail fidelity.
   * - ``CROSS_JOIN_WARN_THRESHOLDS``
     - (100K, 1M)
     - ``config``
     - Progressive warning thresholds for cross-join cardinality.
       Warnings at 100K and 1M rows give users early notice before
       hitting the hard ceiling.

Display Defaults
~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 25 10 15 50
   :header-rows: 1

   * - Constant
     - Value
     - Module
     - Rationale
   * - ``PYCYPHER_REPL_MAX_ROWS``
     - 50
     - ``repl`` (env var)
     - Maximum rows displayed in REPL output.  Prevents terminal flooding.
   * - ``_DEFAULT_LOCK_TIMEOUT``
     - 5.0s
     - ``star`` (ResultCache)
     - Timeout for acquiring cache locks.  5 seconds is long enough for
       normal contention, short enough to detect deadlocks.
