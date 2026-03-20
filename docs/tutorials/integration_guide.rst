Integration Guide
=================

Embed PyCypher in real-world applications — web services, data pipelines,
batch jobs, and interactive notebooks.

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Completed :doc:`basic_query_parsing` and :doc:`graph_modeling`
* Familiarity with your target integration (Flask/FastAPI, Jupyter, etc.)

Core Integration Pattern
-------------------------

Every PyCypher integration follows the same three steps:

1. **Build a Context** — load data once
2. **Create a Star executor** — reuse across queries
3. **Execute queries** — return DataFrames

.. code-block:: python

   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   # Step 1: Build context (once, at startup)
   context = ContextBuilder.from_dict({
       "Person": people_df,
       "KNOWS": knows_df,
   })

   # Step 2: Create executor (reusable)
   star = Star(context=context)

   # Step 3: Execute queries (as many as needed)
   result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")

The ``Star`` instance caches parsed ASTs and query results internally.
Create it once and reuse it.

Web API Integration
-------------------

Flask Example
~~~~~~~~~~~~~

.. code-block:: python

   from flask import Flask, jsonify, request
   from pycypher import Star, QueryTimeoutError, VariableNotFoundError
   from pycypher.ingestion import ContextBuilder

   app = Flask(__name__)

   # Build context at startup
   context = ContextBuilder.from_dict(load_your_data())
   star = Star(context=context)

   @app.route("/query", methods=["POST"])
   def run_query():
       query = request.json.get("query", "")
       params = request.json.get("parameters", {})

       try:
           result = star.execute_query(
               query,
               parameters=params,
               timeout_seconds=5.0,
           )
           return jsonify({
               "columns": result.columns.tolist(),
               "rows": result.values.tolist(),
               "count": len(result),
           })
       except QueryTimeoutError:
           return jsonify({"error": "Query timed out"}), 408
       except VariableNotFoundError as e:
           return jsonify({"error": str(e)}), 400
       except Exception as e:
           return jsonify({"error": str(e)}), 500

FastAPI Example
~~~~~~~~~~~~~~~

.. code-block:: python

   from fastapi import FastAPI, HTTPException
   from pydantic import BaseModel
   from pycypher import Star, QueryTimeoutError
   from pycypher.ingestion import ContextBuilder

   app = FastAPI()

   context = ContextBuilder.from_dict(load_your_data())
   star = Star(context=context)

   class QueryRequest(BaseModel):
       query: str
       parameters: dict = {}
       timeout: float = 5.0

   @app.post("/query")
   def run_query(req: QueryRequest):
       try:
           result = star.execute_query(
               req.query,
               parameters=req.parameters,
               timeout_seconds=req.timeout,
           )
           return {"columns": result.columns.tolist(), "rows": result.to_dict("records")}
       except QueryTimeoutError:
           raise HTTPException(408, "Query timed out")
       except Exception as e:
           raise HTTPException(400, str(e))

.. tip::

   Always set ``timeout_seconds`` on user-facing endpoints to prevent
   malicious or accidental resource exhaustion.

Query Parameters for Safety
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Never interpolate user input into query strings.  Use query parameters:

.. code-block:: python

   # SAFE: parameterized query
   result = star.execute_query(
       "MATCH (p:Person) WHERE p.name = $name RETURN p.age AS age",
       parameters={"name": user_input},
   )

   # UNSAFE: string interpolation — never do this
   # result = star.execute_query(f"... WHERE p.name = '{user_input}' ...")

Parameters are bound safely before execution and support all Cypher types.

Data Pipeline Integration
--------------------------

Batch Processing
~~~~~~~~~~~~~~~~

Process data in stages, refreshing the context between batches:

.. code-block:: python

   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   def process_batch(batch_df):
       """Process a single batch of incoming data."""
       context = (
           ContextBuilder()
           .add_entity("Event", batch_df, id_col="event_id")
           .add_entity("User", users_df)
           .add_relationship("TRIGGERED", triggered_df,
                             source_col="user_id", target_col="event_id")
           .build()
       )
       star = Star(context=context)

       # Aggregate and filter
       result = star.execute_query(
           """
           MATCH (u:User)-[:TRIGGERED]->(e:Event)
           WHERE e.severity = 'critical'
           RETURN u.name AS user, count(e) AS critical_events
           ORDER BY critical_events DESC
           """
       )
       return result

   # Process each batch
   for batch in read_batches("events/*.parquet"):
       alerts = process_batch(batch)
       if len(alerts) > 0:
           send_alerts(alerts)

Mutation Pipelines
~~~~~~~~~~~~~~~~~~

Use Cypher write operations to transform data in-place:

.. code-block:: python

   # Enrich nodes with computed properties
   star.execute_query(
       """
       MATCH (p:Person)-[:KNOWS]->(friend:Person)
       WITH p, count(friend) AS friend_count
       SET p.popularity = friend_count
       """
   )

   # Create derived relationships
   star.execute_query(
       """
       MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person)
       WHERE NOT EXISTS { MATCH (a)-[:KNOWS]->(c) }
       AND a <> c
       CREATE (a)-[:FRIEND_OF_FRIEND]->(c)
       """
   )

YAML Pipeline with nmetl
~~~~~~~~~~~~~~~~~~~~~~~~~

For declarative pipelines, use the ``nmetl`` CLI:

.. code-block:: yaml

   # pipeline.yaml
   sources:
     Person:
       type: csv
       path: data/people.csv
       id_col: user_id
     KNOWS:
       type: csv
       path: data/relationships.csv
       source_col: from_id
       target_col: to_id

   queries:
     - name: popular_people
       cypher: |
         MATCH (p:Person)-[:KNOWS]->(f:Person)
         RETURN p.name AS person, count(f) AS friends
         ORDER BY friends DESC LIMIT 10

.. code-block:: bash

   uv run nmetl run pipeline.yaml
   uv run nmetl query pipeline.yaml "MATCH (p:Person) RETURN count(p)"

See :doc:`data_etl_pipeline` for the full YAML pipeline tutorial.

Jupyter Notebook Integration
-----------------------------

PyCypher returns pandas DataFrames, making it naturally compatible with
Jupyter's display system:

.. code-block:: python

   # In a notebook cell:
   import pandas as pd
   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   context = ContextBuilder.from_dict({
       "Person": pd.read_csv("people.csv"),
       "KNOWS": pd.read_csv("relationships.csv"),
   })
   star = Star(context=context)

   # Display as a rich HTML table automatically
   star.execute_query("MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age")

Combine with plotting libraries:

.. code-block:: python

   import matplotlib.pyplot as plt

   result = star.execute_query(
       """
       MATCH (p:Person)
       RETURN p.dept AS department, count(p) AS headcount
       ORDER BY headcount DESC
       """
   )
   result.plot.bar(x="department", y="headcount", title="Headcount by Department")
   plt.tight_layout()
   plt.show()

Pre-Execution Validation
--------------------------

In multi-tenant or interactive systems, validate queries before execution
to provide fast user feedback:

.. code-block:: python

   from pycypher import validate_query
   from pycypher.semantic_validator import ErrorSeverity

   def safe_execute(star, query, **kwargs):
       """Validate, then execute.  Returns (result, errors)."""
       errors = validate_query(query)
       blocking = [e for e in errors if e.severity == ErrorSeverity.ERROR]
       if blocking:
           return None, [e.message for e in blocking]
       return star.execute_query(query, **kwargs), []

See :doc:`query_validation` for the full validation tutorial.

Error Handling Strategy
------------------------

PyCypher raises specific exception types.  Build your error handling
around them:

.. code-block:: python

   from pycypher import (
       Star,
       VariableNotFoundError,
       UnsupportedFunctionError,
       GraphTypeNotFoundError,
       QueryTimeoutError,
       QueryMemoryBudgetError,
   )

   def execute_safely(star, query, **kwargs):
       """Execute with structured error handling."""
       try:
           return star.execute_query(query, **kwargs)
       except QueryTimeoutError as e:
           log.warning("Timeout after %.1fs: %s", e.elapsed_seconds, query[:80])
           raise
       except QueryMemoryBudgetError as e:
           log.warning("Memory estimate %d MB exceeds budget", e.estimated_bytes // 1e6)
           raise
       except VariableNotFoundError as e:
           log.info("Undefined variable '%s' (available: %s)",
                    e.variable_name, e.available_variables)
           raise
       except GraphTypeNotFoundError as e:
           log.info("Unknown entity type '%s'", e.type_name)
           raise

Context Refresh
----------------

When your source data changes, rebuild the context.  The ``Star`` executor
itself is stateless (beyond caches):

.. code-block:: python

   # Periodic refresh
   def refresh_context():
       new_context = ContextBuilder.from_dict(load_latest_data())
       return Star(context=new_context)

   # Use in a long-running service
   star = refresh_context()
   schedule.every(10).minutes.do(lambda: star := refresh_context())

For append-only data, use mutation queries (``CREATE``, ``MERGE``) to
add data without rebuilding the full context.

Production Checklist
---------------------

Before deploying PyCypher in production:

.. code-block:: text

   [ ] Set PYCYPHER_QUERY_TIMEOUT_S to prevent runaway queries
   [ ] Set PYCYPHER_MAX_CROSS_JOIN_ROWS to limit Cartesian explosions
   [ ] Use query parameters (never string interpolation)
   [ ] Validate untrusted queries before execution
   [ ] Handle all exception types in your error handler
   [ ] Size the result cache for your workload
   [ ] Monitor query metrics via shared.metrics.QUERY_METRICS
   [ ] Bound variable-length paths in user-facing queries

See :doc:`../user_guide/performance_tuning` for detailed tuning guidance
and :doc:`../deployment/index` for container deployment.

Next Steps
----------

* :doc:`../user_guide/performance_tuning` — optimize for production workloads
* :doc:`data_etl_pipeline` — YAML-driven batch pipelines
* :doc:`query_validation` — pre-execution validation
* :doc:`../api/pycypher` — full API reference
