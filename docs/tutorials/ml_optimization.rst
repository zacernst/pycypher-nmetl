ML-Powered Query Optimization
==============================

Learn how PyCypher's online learning system adaptively improves query plans
over time without heavyweight ML dependencies.

.. contents:: On this page
   :local:
   :depth: 2

Learning Objectives
-------------------

After this tutorial you will be able to:

* Use ``QueryLearningStore`` to access all learning components
* Understand how query fingerprinting enables plan reuse
* Measure cache hit rates and selectivity convergence
* Use correction factors to improve cardinality estimates
* Configure cache TTL and capacity for your workload

Prerequisites
-------------

* Completed :doc:`basic_query_parsing` tutorial
* Basic familiarity with query planning concepts

Architecture Overview
---------------------

The learning system has five components coordinated by a single facade:

::

    QueryFingerprinter          structural query similarity detection
    PredicateSelectivityTracker learns actual selectivity per predicate
    JoinPerformanceTracker      records strategy performance per size bucket
    AdaptivePlanCache           caches plans for structurally similar queries
    QueryLearningStore          unified facade coordinating all components

All components use exponential moving averages (EMA) and bounded rolling
windows rather than heavyweight ML frameworks.  Thread-safe via fine-grained
locks.

Getting Started
---------------

.. code-block:: python

   from pycypher.query_learning import QueryLearningStore

   # Create a store (or use the module-level singleton)
   store = QueryLearningStore()

   # Or access the global singleton
   from pycypher.query_learning import get_learning_store
   store = get_learning_store()

Query Fingerprinting
--------------------

The fingerprinter extracts a query's structural skeleton, stripping literal
values so that structurally identical queries share the same fingerprint:

.. code-block:: python

   from pycypher.ast_converter import ASTConverter
   from pycypher.query_learning import QueryLearningStore

   store = QueryLearningStore()

   q1 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
   q2 = ASTConverter.from_cypher("MATCH (p:Person) WHERE p.age > 50 RETURN p.name")

   fp1 = store.fingerprint(q1)
   fp2 = store.fingerprint(q2)

   assert fp1.digest == fp2.digest  # Same structure, different literals

Each fingerprint contains:

- ``digest`` — SHA-256 hex digest (first 16 chars) of the structural representation
- ``clause_signature`` — Human-readable clause sequence (e.g. ``"Match -> Return"``)
- ``entity_types`` — Sorted tuple of entity types (e.g. ``("Person",)``)
- ``relationship_types`` — Sorted tuple of relationship types

Adaptive Plan Caching
---------------------

Once a query is analyzed, cache the result for structurally similar queries:

.. code-block:: python

   from pycypher.ast_converter import ASTConverter
   from pycypher.query_learning import QueryLearningStore
   from pycypher.query_planner import AnalysisResult

   store = QueryLearningStore()
   query = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
   fp = store.fingerprint(query)

   # First execution: cache miss
   cached = store.get_cached_plan(fp)
   assert cached is None

   # Cache the analysis result
   analysis = AnalysisResult(clause_cardinalities=[100])
   store.cache_plan(fp, analysis)

   # Subsequent queries with same structure: cache hit
   cached = store.get_cached_plan(fp)
   assert cached is not None

Measuring Cache Effectiveness
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   stats = store.plan_cache.stats
   print(f"Entries: {stats['entries']}")
   print(f"Hits: {stats['hits']}")
   print(f"Misses: {stats['misses']}")
   print(f"Hit rate: {stats['hit_rate']:.1%}")

Cache Configuration
~~~~~~~~~~~~~~~~~~~

The cache uses LRU eviction with configurable capacity and TTL:

.. code-block:: python

   from pycypher.query_learning import AdaptivePlanCache

   # Custom cache: 128 entries, 60-second TTL
   cache = AdaptivePlanCache(max_entries=128, ttl_seconds=60.0)

**Default settings:**

- ``max_entries=256`` — Maximum cached plans before LRU eviction
- ``ttl_seconds=300.0`` — Plans expire after 5 minutes

**LRU eviction** — When the cache is full, the least-recently-used entry is
evicted.  Accessing an entry (via ``get``) updates its recency.

**TTL expiry** — Entries older than ``ttl_seconds`` are not returned (treated
as cache misses) and are removed on access.

Mutation Invalidation
~~~~~~~~~~~~~~~~~~~~~

After data mutations (CREATE, SET, DELETE), cached plans may be stale:

.. code-block:: python

   # After executing a mutation
   store.invalidate_on_mutation()

   # Plan cache is cleared; selectivity and join data are preserved

Predicate Selectivity Learning
------------------------------

The selectivity tracker learns the actual filtering ratio of predicates using
exponential moving averages, giving more weight to recent observations:

.. code-block:: python

   from pycypher.query_learning import PredicateSelectivityTracker

   tracker = PredicateSelectivityTracker()

   # Record observations after each query execution
   for actual in [0.12, 0.18, 0.14, 0.16, 0.13, 0.15]:
       tracker.record("Person", "age", ">", estimated=0.33, actual=actual)

   # After >= 3 observations, learned value is available
   learned = tracker.get_learned_selectivity("Person", "age", ">")
   print(f"Learned selectivity: {learned:.3f}")  # ~0.14, near actual mean

Using Correction Factors
~~~~~~~~~~~~~~~~~~~~~~~~

Correction factors let you adjust heuristic estimates toward learned values:

.. code-block:: python

   # Record enough observations
   for _ in range(5):
       tracker.record("Person", "age", ">", estimated=0.33, actual=0.12)

   # Get multiplicative correction: learned / heuristic
   factor = tracker.correction_factor("Person", "age", ">", heuristic=0.33)
   # factor ~ 0.12 / 0.33 ~ 0.36

   # Apply: improved_estimate = heuristic * factor
   improved = 0.33 * factor  # ~0.12, much closer to reality

Key behaviors:

- Returns ``1.0`` when insufficient data (< 3 observations)
- Clamped to ``[0.01, 100.0]`` to prevent extreme corrections
- Operators are normalized (case-insensitive, whitespace-stripped)

Join Strategy Learning
----------------------

The join tracker records execution time per strategy and size bucket,
enabling the planner to select the historically fastest approach:

.. code-block:: python

   from pycypher.query_learning import QueryLearningStore

   store = QueryLearningStore()

   # Record: hash join is slower for this size pair
   for _ in range(5):
       store.record_join_performance(
           strategy="hash", left_rows=5000, right_rows=3000,
           actual_output_rows=2500, elapsed_ms=30.0,
       )

   # Record: merge join is faster
   for _ in range(5):
       store.record_join_performance(
           strategy="merge", left_rows=5000, right_rows=3000,
           actual_output_rows=2500, elapsed_ms=10.0,
       )

   best = store.get_best_join_strategy(5000, 3000)
   assert best == "merge"

Size Buckets
~~~~~~~~~~~~

Row counts are bucketed for strategy lookup:

======== ============ ===========
Bucket   Row Range    Example
======== ============ ===========
tiny     <= 100       In-memory lookups
small    <= 10,000    Small tables
medium   <= 1,000,000 Mid-size datasets
large    > 1,000,000  Warehouse-scale
======== ============ ===========

Strategy Statistics
~~~~~~~~~~~~~~~~~~~

Get detailed per-strategy metrics for a size bucket:

.. code-block:: python

   stats = store.join_tracker.strategy_stats(5000, 3000)
   for strategy, metrics in stats.items():
       print(f"{strategy}: avg={metrics['avg_ms']:.1f}ms, "
             f"count={int(metrics['count'])}, "
             f"accuracy={metrics['output_accuracy']:.2f}")

Diagnostics
-----------

Get a snapshot of all learning component state:

.. code-block:: python

   diag = store.diagnostics()
   print(diag)
   # {
   #   'plan_cache': {'entries': 12, 'max_entries': 256, 'hits': 45,
   #                  'misses': 12, 'hit_rate': 0.789},
   #   'selectivity_patterns': 8,
   #   'join_buckets_tracked': 3,
   # }

Resetting State
~~~~~~~~~~~~~~~

Clear all learning data (useful for testing or after schema changes):

.. code-block:: python

   store.clear()

Configuration Constants
-----------------------

The learning system uses these tuning constants (defined at module level):

========================== ======= ==========================================
Constant                   Default Description
========================== ======= ==========================================
``_EMA_ALPHA``             0.3     EMA smoothing factor (higher = more recent)
``_MAX_SELECTIVITY_HISTORY`` 64    Max observations per predicate pattern
``_MAX_JOIN_HISTORY``      64      Max observations per join bucket
``_MAX_PLAN_CACHE``        256     Max cached plans
``_PLAN_CACHE_TTL_S``      300.0   Plan cache TTL in seconds
``_MIN_OBSERVATIONS``      3       Min observations before learning activates
``_CONFIDENCE_THRESHOLD``  0.5     EMA confidence threshold
========================== ======= ==========================================

Try It Yourself
---------------

1. Create a ``QueryLearningStore`` and fingerprint several queries with
   different literal values but the same structure.  Verify they share
   a fingerprint.

2. Record 10 selectivity observations with varying actual values and
   watch the EMA converge.  Plot the convergence if you have matplotlib.

3. Record join performance for two strategies at different size buckets.
   Verify ``best_strategy`` returns the faster one.

4. Set up a cache with ``max_entries=2`` and observe LRU eviction behavior.

Next Steps
----------

* :doc:`../user_guide/performance_tuning` — Environment variables and runtime tuning
* :doc:`production_deployment` — Backend selection for production workloads
* :doc:`../api/pycypher` — Full API reference for ``query_learning`` module
