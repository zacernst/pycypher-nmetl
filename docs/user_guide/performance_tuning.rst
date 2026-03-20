Performance Tuning
==================

Practical strategies for optimising PyCypher query execution in production
workloads — covering timeouts, memory budgets, caching, cross-join limits,
and query structure best practices.

.. contents:: In this guide
   :local:
   :depth: 2

Environment Variables at a Glance
----------------------------------

All runtime tuning knobs are read from environment variables at import time
and centralised in :mod:`pycypher.config`.

+---------------------------------------+-----------+------------------------------------------+
| Variable                              | Default   | Purpose                                  |
+=======================================+===========+==========================================+
| ``PYCYPHER_QUERY_TIMEOUT_S``          | *None*    | Wall-clock timeout per query (seconds)   |
+---------------------------------------+-----------+------------------------------------------+
| ``PYCYPHER_MAX_CROSS_JOIN_ROWS``      | 10 000 000| Hard ceiling on cross-join result size    |
+---------------------------------------+-----------+------------------------------------------+
| ``PYCYPHER_RESULT_CACHE_MAX_MB``      | 100       | Query result cache size (MB)             |
+---------------------------------------+-----------+------------------------------------------+
| ``PYCYPHER_RESULT_CACHE_TTL_S``       | 0         | Cache entry TTL (0 = no expiry)          |
+---------------------------------------+-----------+------------------------------------------+
| ``PYCYPHER_MAX_UNBOUNDED_PATH_HOPS``  | 20        | BFS hop cap for unbounded ``[*]`` paths  |
+---------------------------------------+-----------+------------------------------------------+

Query Timeouts
--------------

Prevent runaway queries from consuming resources indefinitely.

**Per-query timeout** (programmatic):

.. code-block:: python

   result = star.execute_query(
       "MATCH (a)-[*]->(b) RETURN a, b",
       timeout_seconds=5.0,
   )

**Default timeout** (environment):

.. code-block:: bash

   export PYCYPHER_QUERY_TIMEOUT_S=10

When a query exceeds its timeout, PyCypher raises
:class:`~pycypher.exceptions.QueryTimeoutError` with the elapsed time and
a query fragment for diagnostics:

.. code-block:: python

   from pycypher import QueryTimeoutError

   try:
       result = star.execute_query(slow_query, timeout_seconds=2.0)
   except QueryTimeoutError as e:
       print(f"Timed out after {e.elapsed_seconds:.1f}s")

The timeout is enforced cooperatively between clauses **and** via
``SIGALRM`` on Unix (main thread only) so that queries stuck inside C
extensions (e.g. a large pandas merge) are still interrupted.

Memory Budget
-------------

Cap the peak memory a single query is allowed to consume.  The query planner
estimates memory requirements before execution begins and rejects queries
that exceed the budget:

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person), (q:Person) RETURN p.name, q.name",
       memory_budget_bytes=500 * 1024 * 1024,  # 500 MB
   )

If the planner's estimate exceeds the budget, a
:class:`~pycypher.exceptions.QueryMemoryBudgetError` is raised **before**
any execution begins, avoiding wasted work:

.. code-block:: python

   from pycypher import QueryMemoryBudgetError

   try:
       result = star.execute_query(big_query, memory_budget_bytes=100_000_000)
   except QueryMemoryBudgetError as e:
       print(f"Estimated {e.estimated_bytes / 1e6:.0f} MB exceeds budget")

Cross-Join Limits
-----------------

Cartesian products (``MATCH (a:X), (b:Y)``) can produce explosive row counts.
PyCypher enforces a configurable hard ceiling:

.. code-block:: bash

   # Reject cross-joins exceeding 1 million rows
   export PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000

The default is 10 million rows.  When exceeded, the query fails immediately
with a clear error rather than consuming all available memory.

**Best practice**: avoid unfiltered cross-joins.  Push WHERE predicates as
early as possible:

.. code-block:: python

   # BAD: cross-join first, filter later (produces N×M rows)
   star.execute_query(
       "MATCH (p:Person), (c:Company) "
       "WHERE p.company_id = c.id "
       "RETURN p.name, c.name"
   )

   # BETTER: use a relationship to join directly
   star.execute_query(
       "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
       "RETURN p.name, c.name"
   )

Result Caching
--------------

PyCypher caches query results in an LRU cache keyed by query string +
parameters.  Repeated identical read-only queries skip parsing and execution
entirely.

**Configuration**:

.. code-block:: python

   # Programmatic: set cache size and TTL per Star instance
   star = Star(
       context=context,
       result_cache_max_mb=200,          # 200 MB cache
       result_cache_ttl_seconds=300.0,   # 5-minute TTL
   )

   # Or via environment variables (affects all Star instances)
   # export PYCYPHER_RESULT_CACHE_MAX_MB=200
   # export PYCYPHER_RESULT_CACHE_TTL_S=300

**Disable caching** (useful for mutation-heavy workloads):

.. code-block:: python

   star = Star(context=context, result_cache_max_mb=0)

**Smart invalidation**: the cache is automatically invalidated whenever a
mutation query (SET, CREATE, DELETE, MERGE, REMOVE, FOREACH) commits.
Mutation queries are never cached.

**Monitoring cache effectiveness**:

.. code-block:: python

   from pycypher import get_cache_stats

   stats = get_cache_stats(star=star)
   print(f"Hit rate: {stats['result_cache_hit_rate']:.0%}")
   print(f"Entries:  {stats['result_cache_entries']}")
   print(f"Size:     {stats['result_cache_size_mb']:.1f} MB")
   print(f"Evictions: {stats['result_cache_evictions']}")

A hit rate below 50% on analytical workloads may indicate:

- Queries with unique literal values that prevent cache matches
- Frequent mutations invalidating the cache
- Cache too small for the working set — increase ``result_cache_max_mb``

Parse Caching
~~~~~~~~~~~~~

Independent of the result cache, PyCypher maintains an LRU cache (capacity
512) for parsed AST trees.  Identical query strings hit an O(1) lookup on
all calls after the first.  No configuration is needed — this is automatic.

For an ETL pipeline executing 5 queries across 1 000 batches, this means
roughly 5 cold parses (~56 ms each) rather than 5 000.

Query Structure Best Practices
------------------------------

Push Filters Early
~~~~~~~~~~~~~~~~~~

The most impactful optimisation is to filter rows as early as possible.
WHERE clauses immediately after MATCH reduce the working set before
subsequent joins and aggregations:

.. code-block:: python

   # GOOD: filter before aggregation
   star.execute_query(
       "MATCH (p:Person) WHERE p.active = true "
       "RETURN p.dept AS dept, count(p) AS n"
   )

   # LESS EFFICIENT: aggregate all rows, then filter
   star.execute_query(
       "MATCH (p:Person) "
       "WITH p.dept AS dept, count(p) AS n "
       "WHERE n > 5 RETURN dept, n"
   )

Bound Variable-Length Paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unbounded paths (``[*]``) are capped at 20 hops by default, but even this
can produce large intermediate results on dense graphs.  Always specify
explicit bounds when possible:

.. code-block:: python

   # GOOD: bounded path — predictable performance
   star.execute_query(
       "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name"
   )

   # RISKY: unbounded path — BFS explores up to 20 hops
   star.execute_query(
       "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name, b.name"
   )

Reduce the hop cap globally if your graph has long chains:

.. code-block:: bash

   export PYCYPHER_MAX_UNBOUNDED_PATH_HOPS=5

Use WITH to Pipeline Results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use WITH to narrow the working set between query stages.  Only project
the columns you need for subsequent clauses:

.. code-block:: python

   star.execute_query(
       """
       MATCH (p:Person)-[:WORKS_AT]->(c:Company)
       WITH c.name AS company, count(p) AS headcount
       WHERE headcount > 10
       RETURN company, headcount ORDER BY headcount DESC
       """
   )

Prefer Relationship Patterns over Cross-Joins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Relationship patterns (``-[:REL]->``) scan only related rows.  Cross-joins
(``MATCH (a), (b)``) produce the full Cartesian product.  The difference is
orders of magnitude on large tables.

Monitoring and Diagnostics
--------------------------

Query Metrics
~~~~~~~~~~~~~

PyCypher collects execution metrics automatically:

.. code-block:: python

   from shared.metrics import QUERY_METRICS

   stats = QUERY_METRICS.snapshot()
   print(f"Total queries:     {stats.total_queries}")
   print(f"Timing p50:        {stats.timing_p50_ms:.1f} ms")
   print(f"Timing p99:        {stats.timing_p99_ms:.1f} ms")
   print(f"Slow queries:      {stats.slow_queries}")
   print(f"Error rate:        {stats.error_rate:.1%}")
   print(f"Memory delta p50:  {stats.memory_delta_p50_mb:.1f} MB")

The slow-query threshold is configurable:

.. code-block:: bash

   export PYCYPHER_SLOW_QUERY_MS=500   # 500ms threshold

Query Execution Logging
~~~~~~~~~~~~~~~~~~~~~~~~

PyCypher logs every query execution at the ``INFO`` level with row count,
elapsed time, and memory delta.  Enable debug logging for parse timing and
clause-by-clause breakdown:

.. code-block:: python

   import logging
   logging.getLogger("pycypher").setLevel(logging.DEBUG)

Common Bottlenecks
------------------

+------------------------------+-------------------------------------------+
| Symptom                      | Likely cause and fix                      |
+==============================+===========================================+
| Query takes seconds for      | Large cross-join or unbounded path.       |
| small result set             | Add WHERE filters or bound the path.      |
+------------------------------+-------------------------------------------+
| Memory spike during query    | Wide cross-join or large intermediate     |
|                              | frame.  Use ``memory_budget_bytes`` to    |
|                              | reject early.                             |
+------------------------------+-------------------------------------------+
| Low cache hit rate           | Unique literals in queries prevent cache  |
|                              | matching.  Use query parameters instead   |
|                              | (``$name`` → ``parameters={"name": …}``)|
+------------------------------+-------------------------------------------+
| Repeated identical queries   | Verify result cache is enabled            |
| not getting faster           | (``result_cache_max_mb > 0``).  Check     |
|                              | for interleaved mutations that invalidate.|
+------------------------------+-------------------------------------------+
| Timeout on first query only  | Cold parse of the Lark grammar (~50 ms).  |
|                              | Subsequent queries hit the AST cache.     |
|                              | This is normal one-time overhead.         |
+------------------------------+-------------------------------------------+

Production Deployment Checklist
-------------------------------

.. code-block:: bash

   # 1. Set a default timeout to prevent runaway queries
   export PYCYPHER_QUERY_TIMEOUT_S=30

   # 2. Limit cross-join size for safety
   export PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000

   # 3. Size the result cache for your working set
   export PYCYPHER_RESULT_CACHE_MAX_MB=200

   # 4. Set a TTL if data changes externally
   export PYCYPHER_RESULT_CACHE_TTL_S=60

   # 5. Bound variable-length paths for dense graphs
   export PYCYPHER_MAX_UNBOUNDED_PATH_HOPS=10

   # 6. Set slow-query threshold for alerting
   export PYCYPHER_SLOW_QUERY_MS=500

For More Information
--------------------

* :doc:`query_processing` — detailed query lifecycle and expression evaluation
* :doc:`../api/pycypher` — complete API reference
* :doc:`../tutorials/data_etl_pipeline` — ETL pipeline patterns
