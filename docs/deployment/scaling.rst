Scaling & Performance
=====================

PyCypher supports several strategies for scaling query execution from
single-machine development to multi-node production workloads.

Backend Selection
-----------------

PyCypher supports three computation backends, selectable at context creation
time:

.. code-block:: python

   from pycypher.ingestion import ContextBuilder

   # Auto-select best available backend
   context = ContextBuilder.from_dict(data, backend="auto")

   # Force a specific backend
   context = ContextBuilder.from_dict(data, backend="pandas")
   context = ContextBuilder.from_dict(data, backend="duckdb")
   context = ContextBuilder.from_dict(data, backend="polars")

.. list-table::
   :widths: 15 30 30 25
   :header-rows: 1

   * - Backend
     - Strengths
     - Best for
     - Memory profile
   * - Pandas
     - Widest compatibility, mature ecosystem
     - Small-medium datasets, prototyping
     - High (full materialisation)
   * - DuckDB
     - Columnar engine, SQL pushdown
     - Analytical queries, large scans
     - Medium (lazy evaluation)
   * - Polars
     - Rust-native, multi-threaded
     - CPU-bound transforms, large datasets
     - Low (streaming where possible)

The ``auto`` strategy probes backends in order (Polars, DuckDB, Pandas) and
selects the first that passes a health check.  The circuit breaker tracks
failures and automatically bypasses unhealthy backends.

Health Checks and Circuit Breaker
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The backend engine includes a health check probe that validates scan, filter,
join, and materialise operations before committing to a backend.  Enable
health checks during selection:

.. code-block:: python

   from pycypher.backend_engine import select_backend

   backend = select_backend("auto", run_health_check=True)

The circuit breaker uses a three-state model:

* **CLOSED** (normal) -- requests flow through; failures increment a counter
* **OPEN** (tripped) -- backend is skipped; checked again after a recovery timeout
* **HALF_OPEN** (probing) -- one test request; success resets, failure re-opens

Default thresholds: 3 consecutive failures to open, 60-second recovery timeout.

Spark Cluster Scaling
---------------------

For workloads that exceed single-machine capacity, scale the Spark cluster:

.. code-block:: bash

   # Start with default 1 worker
   make spark-up

   # Scale to 5 workers
   make spark-scale WORKERS=5

   # View the Spark UI
   make spark-ui
   # → http://localhost:8090

Worker Configuration
~~~~~~~~~~~~~~~~~~~~

Each Spark worker is configured with:

* **2 GB memory** (``SPARK_WORKER_MEMORY=2G``)
* **2 CPU cores** (``SPARK_WORKER_CORES=2``)
* **RPC authentication** enabled with shared secret
* **RPC encryption** and local storage encryption enabled

To change worker resources, modify ``docker-compose.yml``:

.. code-block:: yaml

   spark-worker:
     environment:
       - SPARK_WORKER_MEMORY=4G   # increase per-worker memory
       - SPARK_WORKER_CORES=4     # increase per-worker cores

Neo4j Tuning
~~~~~~~~~~~~~

Default Neo4j memory settings are conservative:

.. code-block:: yaml

   neo4j:
     environment:
       - NEO4J_dbms_memory_heap_max__size=1G
       - NEO4J_dbms_memory_pagecache_size=512m

For production workloads with larger graphs, increase these values.  A common
guideline: allocate 50% of available RAM to page cache and 25% to heap.

Query-Level Performance
-----------------------

Memory Budget
~~~~~~~~~~~~~

The query planner estimates memory usage and enforces a configurable budget:

.. code-block:: python

   from pycypher import Star

   star = Star(context=context)
   result = star.execute_query(
       "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
       memory_budget_mb=512,  # cap at 512 MB
   )

Cross-Join Limits
~~~~~~~~~~~~~~~~~

Unbounded cross-products (e.g., ``MATCH (a), (b)``) are capped by default to
prevent resource exhaustion.  The limit is configurable:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a:Person), (b:Person) RETURN count(*)",
       max_cross_join_rows=1_000_000,
   )

Query Timeout
~~~~~~~~~~~~~

Set a timeout to abort runaway queries:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a)-[:KNOWS*1..10]->(b) RETURN a, b",
       timeout_seconds=30,
   )

Lazy Evaluation
~~~~~~~~~~~~~~~

Variable-length path queries and large scans benefit from lazy evaluation,
which defers computation until results are materialised:

.. code-block:: python

   # Lazy evaluation is automatic for variable-length paths
   result = star.execute_query(
       "MATCH (a)-[:KNOWS*1..5]->(b) RETURN a.name, b.name LIMIT 100"
   )
   # Only paths that survive the LIMIT are fully materialised

Performance Testing
-------------------

Run the built-in performance benchmarks:

.. code-block:: bash

   # Large-dataset integration tests (120s timeout)
   make test-large-dataset

   # Backend compatibility tests
   make test-backends

   # Full test suite in parallel
   make test

Resource Planning
-----------------

.. list-table::
   :widths: 25 25 25 25
   :header-rows: 1

   * - Dataset size
     - Recommended backend
     - RAM per worker
     - Workers
   * - < 100K rows
     - Pandas
     - 2 GB
     - 1
   * - 100K -- 10M rows
     - DuckDB or Polars
     - 4 GB
     - 1--2
   * - 10M -- 100M rows
     - Polars + Spark
     - 8 GB
     - 2--4
   * - > 100M rows
     - Spark cluster
     - 16 GB
     - 4+

These are guidelines.  Actual requirements depend on query complexity (number
of joins, variable-length paths, aggregation cardinality).  Use the query
planner's memory estimation to calibrate for your workload.
