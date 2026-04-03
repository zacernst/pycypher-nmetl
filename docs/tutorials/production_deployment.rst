Production Deployment Patterns
==============================

Configure PyCypher for production workloads — backend selection, rate
limiting, audit logging, and performance tuning for real-world systems.

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Completed :doc:`basic_query_parsing` and :doc:`integration_guide`
* Familiarity with environment variable configuration

Backend Selection
-----------------

PyCypher supports pluggable DataFrame backends.  Choose a backend based on
your workload characteristics:

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Backend
     - Best for
     - Trade-offs
   * - ``pandas`` (default)
     - Small-medium datasets, prototyping
     - Lowest startup cost, widest compatibility
   * - ``duckdb``
     - Large datasets, analytical queries
     - 2-10x faster joins/aggregations, higher memory efficiency
   * - ``polars``
     - CPU-bound transformations
     - Fast columnar operations, requires polars dependency

.. code-block:: python

   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   # Auto-select best available backend
   context = ContextBuilder.from_dict({"Person": people_df}).build(backend="auto")

   # Explicitly choose DuckDB for analytical workloads
   context = ContextBuilder.from_dict({"Person": people_df}).build(backend="duckdb")

   star = Star(context=context)
   print(f"Using backend: {context.backend_name}")

Rate Limiting
-------------

Protect shared deployments from query abuse with the built-in rate limiter.
Rate limiting is off by default and activated via environment variables:

.. code-block:: bash

   # Allow 50 queries/second with bursts up to 100
   export PYCYPHER_RATE_LIMIT_QPS=50
   export PYCYPHER_RATE_LIMIT_BURST=100

For programmatic control, create a custom limiter:

.. code-block:: python

   from pycypher.rate_limiter import QueryRateLimiter
   from pycypher import RateLimitError

   # Per-user rate limiting in a web application
   user_limiters = {}

   def get_user_limiter(user_id: str) -> QueryRateLimiter:
       if user_id not in user_limiters:
           user_limiters[user_id] = QueryRateLimiter(qps=10.0, burst=20)
       return user_limiters[user_id]

   def handle_query(user_id: str, query: str):
       try:
           get_user_limiter(user_id).acquire()
           return star.execute_query(query)
       except RateLimitError:
           raise HTTPError(429, "Too many queries — try again shortly")

Audit Logging
-------------

Enable structured query audit logging for compliance, debugging, and
performance monitoring.

**Quick start** — enable via environment variable:

.. code-block:: bash

   export PYCYPHER_AUDIT_LOG=1

Each query emits a JSON record to the ``pycypher.audit`` logger:

.. code-block:: json

   {
     "query_id": "a1b2c3d4",
     "timestamp": "2026-03-30T12:00:00Z",
     "query": "MATCH (p:Person) WHERE p.age > 25 RETURN p.name",
     "status": "ok",
     "elapsed_ms": 12.3,
     "rows": 42,
     "parameter_keys": []
   }

**Production configuration** — route audit logs to a file:

.. code-block:: python

   import logging

   from pycypher.audit import enable_audit_log

   # Enable audit logging
   enable_audit_log()

   # Route to a dedicated file
   audit_logger = logging.getLogger("pycypher.audit")
   handler = logging.FileHandler("/var/log/pycypher/audit.jsonl")
   audit_logger.addHandler(handler)
   audit_logger.setLevel(logging.INFO)

**Security note**: parameter *values* and result data are never logged.
Query text is truncated to prevent unbounded log growth.

Timeouts and Memory Budgets
----------------------------

Always set timeouts and memory budgets in production to prevent runaway
queries from consuming all resources:

.. code-block:: python

   from pycypher import Star, QueryTimeoutError, QueryMemoryBudgetError

   star = Star(context=context)

   try:
       result = star.execute_query(
           user_query,
           timeout_seconds=10.0,
           memory_budget_bytes=500 * 1024 * 1024,  # 500 MB
       )
   except QueryTimeoutError as e:
       log.warning(f"Query timed out after {e.elapsed_seconds:.1f}s")
   except QueryMemoryBudgetError as e:
       log.warning(f"Estimated {e.estimated_bytes / 1e6:.0f}MB exceeds budget")

Or set defaults via environment variables:

.. code-block:: bash

   export PYCYPHER_QUERY_TIMEOUT_S=30
   export PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000

Result Caching
--------------

PyCypher caches query results automatically.  Tune cache size and TTL
for your workload:

.. code-block:: python

   # Large cache with 5-minute TTL for analytical dashboards
   star = Star(
       context=context,
       result_cache_max_mb=500,
       result_cache_ttl_seconds=300.0,
   )

   # Disable caching for mutation-heavy workloads
   star = Star(context=context, result_cache_max_mb=0)

Monitor cache effectiveness:

.. code-block:: python

   from pycypher import get_cache_stats

   stats = get_cache_stats(star=star)
   print(f"Hit rate: {stats['result_cache_hit_rate']:.0%}")
   print(f"Size:     {stats['result_cache_size_mb']:.1f} MB")

Complete Production Checklist
------------------------------

.. code-block:: bash

   # Resource protection
   export PYCYPHER_QUERY_TIMEOUT_S=30
   export PYCYPHER_MAX_CROSS_JOIN_ROWS=1_000_000
   export PYCYPHER_MAX_UNBOUNDED_PATH_HOPS=10

   # Caching
   export PYCYPHER_RESULT_CACHE_MAX_MB=200
   export PYCYPHER_RESULT_CACHE_TTL_S=60

   # Monitoring
   export PYCYPHER_SLOW_QUERY_MS=500
   export PYCYPHER_AUDIT_LOG=1

   # Multi-tenant rate limiting
   export PYCYPHER_RATE_LIMIT_QPS=50
   export PYCYPHER_RATE_LIMIT_BURST=100

Next Steps
----------

* :doc:`../user_guide/performance_tuning` — detailed performance tuning reference
* :doc:`../user_guide/error_handling` — structured error handling patterns
* :doc:`../deployment/index` — deployment infrastructure (Docker, scaling)
