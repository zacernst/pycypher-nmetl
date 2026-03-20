Configuration Reference
=======================

PyCypher is configured entirely through environment variables and
constructor parameters.  All environment variables are read once at import
time from :mod:`pycypher.config`.

Query Execution
---------------

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Environment Variable
     - Default
     - Description
   * - ``PYCYPHER_QUERY_TIMEOUT_S``
     - None
     - Wall-clock timeout (seconds) for a single query.  ``None`` means no
       limit.  Can also be set per-query via ``execute_query(timeout_seconds=...)``.
   * - ``PYCYPHER_MAX_CROSS_JOIN_ROWS``
     - 10,000,000
     - Hard ceiling on cross-join result size.  Prevents accidental Cartesian
       explosions when MATCH patterns share no variables.
   * - ``PYCYPHER_MAX_UNBOUNDED_PATH_HOPS``
     - 20
     - Maximum BFS hops for unbounded variable-length paths (``[*]``).
       Higher values risk exponential memory growth in dense graphs.

Caching
-------

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Environment Variable
     - Default
     - Description
   * - ``PYCYPHER_RESULT_CACHE_MAX_MB``
     - 100
     - Maximum memory (MB) for the LRU query result cache.  Set to ``0``
       to disable result caching.
   * - ``PYCYPHER_RESULT_CACHE_TTL_S``
     - 0
     - Time-to-live (seconds) for cached results.  ``0`` means entries never
       expire — only evicted by size pressure.
   * - ``PYCYPHER_AST_CACHE_MAX``
     - 1024
     - Maximum parsed ASTs cached per ``GrammarParser`` instance.  LRU
       eviction when full.  ``0`` disables caching.

Logging and Metrics
-------------------

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Environment Variable
     - Default
     - Description
   * - ``PYCYPHER_LOG_LEVEL``
     - WARNING
     - Log level: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``.
   * - ``PYCYPHER_LOG_FORMAT``
     - rich
     - Log format: ``rich`` (human-readable) or ``json`` (machine-readable,
       suitable for ELK/Datadog/Splunk).
   * - ``PYCYPHER_METRICS_ENABLED``
     - 1
     - Enable query metrics collection.  Set to ``0`` or ``false`` to disable.
   * - ``PYCYPHER_SLOW_QUERY_MS``
     - 1000
     - Threshold (milliseconds) for slow-query warnings in the metrics
       collector.

Telemetry
---------

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Environment Variable
     - Default
     - Description
   * - ``PYROSCOPE_ENABLED``
     - 1
     - Enable Pyroscope continuous profiling.  Set to ``0`` or ``false`` to
       disable.  Requires ``pyroscope`` package.
   * - ``PYROSCOPE_SERVER``
     - \http://localhost:4040
     - Pyroscope server endpoint.
   * - ``PYROSCOPE_APP_NAME``
     - nmetl
     - Application name tag in Pyroscope.
   * - ``PYROSCOPE_SAMPLE_RATE``
     - 100
     - Samples per second.

Per-Query Overrides
-------------------

Many configuration options can be overridden per-query via
:meth:`~pycypher.star.Star.execute_query` parameters:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a)-[:KNOWS*]->(b) RETURN a, b",
       timeout_seconds=5.0,            # overrides PYCYPHER_QUERY_TIMEOUT_S
       memory_budget_bytes=256 * 1024 * 1024,  # 256 MB hard limit
       max_complexity_score=100,        # reject overly complex queries
   )

Production Configuration Example
---------------------------------

.. code-block:: bash

   # .env for production deployment
   export PYCYPHER_QUERY_TIMEOUT_S=30
   export PYCYPHER_MAX_CROSS_JOIN_ROWS=1000000
   export PYCYPHER_MAX_UNBOUNDED_PATH_HOPS=10
   export PYCYPHER_RESULT_CACHE_MAX_MB=500
   export PYCYPHER_RESULT_CACHE_TTL_S=300
   export PYCYPHER_LOG_LEVEL=INFO
   export PYCYPHER_LOG_FORMAT=json
   export PYCYPHER_SLOW_QUERY_MS=500
