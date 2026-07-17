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

Diagnostic Report
~~~~~~~~~~~~~~~~~

For operator dashboards, use the diagnostic report on a metrics snapshot:

.. code-block:: python

   snapshot = QUERY_METRICS.snapshot()
   report = snapshot.diagnostic_report()
   # Returns a human-readable multi-section report with hotspot
   # identification, error analysis, and cache efficiency

Cache Statistics
~~~~~~~~~~~~~~~~

Monitor AST parse cache and result cache pressure:

.. code-block:: python

   from pycypher import get_cache_stats

   stats = get_cache_stats()
   print(f"AST cache hits: {stats['ast_hits']}")
   print(f"AST cache misses: {stats['ast_misses']}")
   print(f"AST cache size: {stats['ast_size']}")
   print(f"Result cache hit rate: {stats.get('result_cache_hit_rate', 0):.1%}")

Metrics Export
--------------

PyCypher includes pluggable metrics exporters for Prometheus, StatsD, and
JSON file output.  All exporters use stdlib only — no external dependencies.

Configure via the ``PYCYPHER_METRICS_EXPORT`` environment variable:

.. code-block:: bash

   # Enable one or more exporters (comma-separated)
   export PYCYPHER_METRICS_EXPORT=prometheus,statsd,json

Prometheus
~~~~~~~~~~

Generates ``text/plain; version=0.0.4`` output compatible with Prometheus,
VictoriaMetrics, or any OpenMetrics scraper.  The health server at
``/metrics`` uses this exporter automatically.

.. code-block:: python

   from shared.exporters import PrometheusExporter
   from shared.metrics import QUERY_METRICS

   exporter = PrometheusExporter(prefix="pycypher")
   text = exporter.render(QUERY_METRICS.snapshot())
   # Returns multi-line Prometheus text exposition format

Exported metrics include ``pycypher_queries_total``,
``pycypher_query_duration_p50_ms``, ``pycypher_cache_hit_rate``,
``pycypher_health_status``, per-clause timings, and per-error-type counters.

StatsD / Datadog
~~~~~~~~~~~~~~~~

Pushes metrics via UDP to any StatsD-compatible daemon (StatsD, Datadog Agent,
Telegraf).

.. code-block:: bash

   export PYCYPHER_METRICS_EXPORT=statsd
   export PYCYPHER_STATSD_HOST=127.0.0.1   # default
   export PYCYPHER_STATSD_PORT=8125         # default

JSON File
~~~~~~~~~

Appends JSON-lines snapshots for ingestion by ELK, Datadog Logs, or Loki.

.. code-block:: bash

   export PYCYPHER_METRICS_EXPORT=json
   export PYCYPHER_METRICS_JSON_PATH=metrics.jsonl   # default

Programmatic Export
~~~~~~~~~~~~~~~~~~~

Push metrics to all configured exporters in one call:

.. code-block:: python

   from shared.exporters import export_once
   from shared.metrics import QUERY_METRICS

   export_once(QUERY_METRICS.snapshot())

Additional configuration:

* ``PYCYPHER_METRICS_PREFIX`` — metric name prefix (default: ``pycypher``)
* ``PYCYPHER_METRICS_EXPORT_INTERVAL_S`` — push interval in seconds (default: ``60``)

OpenTelemetry Distributed Tracing
----------------------------------

PyCypher supports OpenTelemetry distributed tracing for end-to-end query
visibility across services.  If ``opentelemetry-api`` is not installed, all
tracing calls are silent no-ops with zero runtime cost.

.. code-block:: bash

   # Enable tracing
   export PYCYPHER_OTEL_ENABLED=1

   # Standard OTel configuration
   export OTEL_SERVICE_NAME=pycypher
   export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317

Wrap query execution in traced spans:

.. code-block:: python

   from shared.otel import trace_query, trace_phase

   # Trace an entire query
   with trace_query("MATCH (p:Person) RETURN p.name", query_id="q-123") as span:
       result = star.execute_query(query)
       span.set_attribute("result.rows", len(result))

   # Trace individual phases (parse, plan, execute)
   with trace_phase("parse", query_id="q-123") as span:
       ast = parser.parse(query)

Span attributes set automatically:

* ``db.system`` = ``"pycypher"``
* ``db.statement`` = query text (truncated to 500 chars)
* ``db.operation`` = first Cypher keyword (MATCH, CREATE, etc.)
* ``pycypher.query_id`` = correlation ID

Exceptions are recorded on the span and the span status is set to ERROR.

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
