Monitoring & Operations
=======================

PyCypher includes built-in metrics collection, structured logging, and
diagnostic tools for production observability.

Query Metrics
-------------

The ``shared.metrics`` module collects per-query execution metrics
automatically:

.. code-block:: python

   from shared.metrics import QUERY_METRICS

   # Execute some queries...
   star.execute_query("MATCH (p:Person) RETURN p.name")

   # Get a point-in-time snapshot
   snapshot = QUERY_METRICS.snapshot()
   print(f"Queries executed: {snapshot.total_queries}")
   print(f"Total time: {snapshot.total_time_ms:.1f} ms")
   print(f"Error count: {snapshot.error_count}")

Metrics Snapshot
~~~~~~~~~~~~~~~~

The ``MetricsSnapshot`` includes:

* **Query counts** -- total, successful, failed
* **Timing** -- total, mean, p50, p95, p99 latencies
* **Throughput** -- queries per second
* **Error rates** -- error count and error rate trend
* **Per-clause breakdown** -- time spent in MATCH, WHERE, RETURN, etc.

Diagnostic Summary
~~~~~~~~~~~~~~~~~~

For operator dashboards, use the diagnostic summary method:

.. code-block:: python

   summary = QUERY_METRICS.diagnostic_summary()
   # Returns a dict suitable for JSON serialisation or dashboard display

Cache Statistics
~~~~~~~~~~~~~~~~

Monitor AST parse cache and grammar cache pressure:

.. code-block:: python

   from pycypher.grammar_parser import get_cache_stats

   stats = get_cache_stats()
   print(f"Cache hits: {stats['hits']}")
   print(f"Cache misses: {stats['misses']}")
   print(f"Cache size: {stats['size']}")

Structured Logging
------------------

PyCypher uses the ``shared.logger`` module for structured output:

.. code-block:: python

   from shared.logger import LOGGER

   LOGGER.info("Query executed", query="MATCH (n) RETURN n", rows=42)
   LOGGER.debug("Pattern match", entity_type="Person", matches=1000)

Query execution is automatically logged with:

* Query text (truncated for security)
* Execution time
* Row count
* Error details (on failure)
* Query ID for correlation

Query ID Correlation
~~~~~~~~~~~~~~~~~~~~

Each query execution is assigned a unique ID for end-to-end tracing:

.. code-block:: python

   result = star.execute_query("MATCH (p:Person) RETURN p.name")
   # Log output: {"query_id": "abc-123", "time_ms": 12.3, "rows": 100}

Container Logs
--------------

View logs for any service:

.. code-block:: bash

   # Development container
   make dev-logs

   # Spark cluster
   make spark-logs

   # Neo4j
   make neo4j-logs

   # Nominatim
   make nominatim-logs

   # FastOpenData
   make fod-logs
   make fod-api-logs

Service Health
--------------

Check container status and health:

.. code-block:: bash

   # All services
   docker compose ps

   # Nominatim import/service status
   make nominatim-status

   # Neo4j health (via browser endpoint)
   curl -s http://localhost:7474

   # Spark cluster status
   # → open http://localhost:8090

Backend Health Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~

Monitor backend health programmatically:

.. code-block:: python

   from pycypher.backend_engine import (
       check_backend_health,
       get_circuit_breaker,
       PandasBackend,
   )

   # Probe a specific backend
   backend = PandasBackend()
   healthy = check_backend_health(backend)
   print(f"Pandas backend healthy: {healthy}")

   # Check circuit breaker state
   cb = get_circuit_breaker()
   for name in ["pandas", "duckdb", "polars"]:
       state = cb.state(name)
       available = cb.is_available(name)
       print(f"{name}: state={state.value}, available={available}")

Operational Runbook
-------------------

Restart a Stuck Container
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Restart just the dev container
   docker compose restart pycypher-dev

   # Full rebuild if state is corrupted
   make dev-rebuild

Clear Neo4j Data
~~~~~~~~~~~~~~~~

.. code-block:: bash

   # WARNING: deletes all graph data (3s abort window)
   make neo4j-reset

Reset Spark State
~~~~~~~~~~~~~~~~~

.. code-block:: bash

   make spark-down
   docker volume rm pycypher-nmetl_spark-events
   make spark-up

Check Nominatim Import Progress
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first Nominatim start imports US OSM data, which takes several hours:

.. code-block:: bash

   # Watch import progress
   make nominatim-logs

   # Check status endpoint
   make nominatim-status

   # Test geocoding
   make nominatim-search
