Security Hardening & Compliance
================================

This guide documents PyCypher's security architecture, threat mitigation
strategies, and best practices for secure deployment.

.. contents:: On this page
   :local:
   :depth: 2

Threat Model
------------

.. seealso::

   :doc:`threat_model` for the comprehensive trust boundary mapping,
   data flow analysis, and coordination requirements.

PyCypher processes user-supplied Cypher queries against in-process data.
The primary threat vectors are:

1. **Query injection** — malicious Cypher or SQL embedded in user input.
2. **Resource exhaustion** — unbounded cross-joins, runaway queries, or
   memory bombs that crash the host process.
3. **Path traversal** — data-source URIs or file paths that escape the
   intended directory.
4. **Data leakage** — returning internal columns (``__ID__``,
   ``__SOURCE__``, ``__TARGET__``) or properties from unintended entity
   types.

Input Validation
----------------

Cypher query parameters
~~~~~~~~~~~~~~~~~~~~~~~

Always use parameterised queries instead of string interpolation:

.. code-block:: python

   # GOOD — parameters are never interpolated into the query string
   star.execute_query(
       "MATCH (p:Person) WHERE p.age > $min_age RETURN p.name",
       parameters={"min_age": 18},
   )

   # BAD — opens the door to injection
   star.execute_query(
       f"MATCH (p:Person) WHERE p.age > {user_input} RETURN p.name"
   )

SQL data-source queries
~~~~~~~~~~~~~~~~~~~~~~~

The ingestion layer uses DuckDB for reading external files.  All SQL
passed to DuckDB is validated by
:func:`~pycypher.ingestion.security.validate_sql_query`, which rejects:

* Multiple statements (semicolon stacking).
* DDL keywords (``DROP``, ``ALTER``, ``CREATE``, ``TRUNCATE``).
* DML mutation keywords (``INSERT``, ``UPDATE``, ``DELETE``).
* Comment markers (``--``, ``/* */``) that could hide payloads.

Use :func:`~pycypher.ingestion.security.sanitize_sql_identifier` for any
user-supplied table or column names, and
:func:`~pycypher.ingestion.security.parameterize_duckdb_query` for safe
value substitution.

File path sanitisation
~~~~~~~~~~~~~~~~~~~~~~

:func:`~pycypher.ingestion.security.sanitize_file_path` prevents path
traversal by rejecting ``..`` components and blocking access to sensitive
system directories (``/etc/``, ``/root/``, ``/proc/``).

URI scheme validation
~~~~~~~~~~~~~~~~~~~~~

:func:`~pycypher.ingestion.security.validate_uri_scheme` restricts data
source URIs to an allow-list of safe schemes: ``file``, ``http``,
``https``, ``s3``, ``gs``, ``postgresql``, ``mysql``, ``sqlite``,
``duckdb``.

Resource Limits
---------------

Cross-join row limit
~~~~~~~~~~~~~~~~~~~~

Unbounded ``MATCH (a), (b), (c)`` patterns can produce Cartesian
explosions.  PyCypher enforces a configurable row ceiling:

.. code-block:: python

   # Default: 10,000,000 rows
   # Override via environment variable:
   import os
   os.environ["PYCYPHER_MAX_CROSS_JOIN_ROWS"] = "1000000"

Exceeding the limit raises ``MemoryError`` with a descriptive message.

Query timeout
~~~~~~~~~~~~~

Arm a per-query wall-clock budget to prevent runaway execution:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a)-[*1..10]->(b) RETURN a, b",
       timeout_seconds=5.0,
   )

Exceeding the timeout raises ``QueryTimeoutError``.

Memory budget
~~~~~~~~~~~~~

The query planner estimates memory usage before execution begins.  Set
an explicit budget to reject queries that would exceed it:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a)-[:KNOWS]->(b) RETURN a, b",
       memory_budget_bytes=512 * 1024 * 1024,  # 512 MB
   )

Exceeding the budget raises ``QueryMemoryBudgetError``.

Internal Column Protection
--------------------------

PyCypher's data model uses internal columns (``__ID__``, ``__SOURCE__``,
``__TARGET__``) to track entity identity and relationship endpoints.
These are automatically excluded from user-visible results:

* ``properties(n)`` omits internal columns.
* ``keys(n)`` omits internal columns.
* ``RETURN *`` omits internal columns.
* ``MapProjection`` with ``.*`` omits internal columns.

If you build custom functions that access raw DataFrames, filter out
columns whose names start and end with ``__``.

Semantic Validation
-------------------

:class:`~pycypher.semantic_validator.SemanticValidator` performs
static analysis on parsed queries before execution:

* Detects undefined variables.
* Validates aggregation/non-aggregation mixing.
* Checks function arity and argument types.
* Reports unused variables.

Always validate user-supplied queries before execution in
security-sensitive contexts:

.. code-block:: python

   from pycypher import validate_query

   issues = validate_query("MATCH (n) RETURN m")
   for issue in issues:
       if issue.severity == "error":
           raise ValueError(f"Query validation failed: {issue.message}")

Error Handling
--------------

PyCypher raises typed exceptions rather than generic ``Exception``:

* ``GraphTypeNotFoundError`` — unknown entity or relationship type.
* ``UndefinedVariableError`` — reference to unbound variable.
* ``CypherSyntaxError`` — malformed Cypher query.
* ``QueryTimeoutError`` — wall-clock budget exceeded.
* ``QueryMemoryBudgetError`` — estimated memory exceeds budget.
* ``SecurityError`` — SQL injection or path traversal detected.

Catch specific exceptions rather than broad ``except Exception`` to
avoid masking security-relevant errors.

Logging and Audit
-----------------

PyCypher logs query execution through the ``shared.logger`` module:

* **Query start/end**: query text, elapsed time, row count, RSS delta.
* **Query plan**: node count, memory estimate, join presence.
* **Per-clause timing**: individual clause execution durations.
* **Errors**: full traceback with query context.

For audit compliance, configure the logger to write to a persistent
destination (file, syslog, or structured logging service).  Query
correlation IDs (``_query_id``) enable tracing individual queries across
log entries.

Deployment Checklist
--------------------

.. list-table::
   :header-rows: 1
   :widths: 10 60 30

   * - Priority
     - Action
     - Status
   * - P0
     - Set ``PYCYPHER_MAX_CROSS_JOIN_ROWS`` to a value appropriate for
       your hardware.
     - Required
   * - P0
     - Always use parameterised queries for user-supplied values.
     - Required
   * - P0
     - Set ``timeout_seconds`` on all user-facing query endpoints.
     - Required
   * - P1
     - Run ``validate_query()`` on user-supplied Cypher before execution.
     - Recommended
   * - P1
     - Set ``memory_budget_bytes`` based on available host memory.
     - Recommended
   * - P1
     - Configure persistent audit logging for query execution events.
     - Recommended
   * - P2
     - Pin dependency versions with upper bounds (already done in
       ``pyproject.toml``).
     - Done
   * - P2
     - Run ``uv sync --frozen`` in CI to prevent supply-chain drift.
     - Done

Security Testing
----------------

PyCypher includes comprehensive security test suites:

* ``tests/test_security_sql_injection_tdd.py`` — SQL injection prevention.
* ``tests/test_security_sql_injection_proof.py`` — SQL injection proof tests.
* ``tests/test_backend_engine_security.py`` — backend engine security.
* ``tests/test_data_source_security.py`` — data source URI validation.
* ``tests/test_duckdb_reader_security.py`` — DuckDB reader security.
* ``tests/test_helpers_serialization_security.py`` — serialisation safety.
* ``tests/test_neo4j_sink_security.py`` — Neo4j sink security.
* ``tests/test_dask_distributed_security.py`` — distributed security.
* ``tests/test_distributed_security_contracts.py`` — distributed contracts.

Run the full security suite:

.. code-block:: bash

   uv run pytest tests/ -m security -q

Or run all security-named test files:

.. code-block:: bash

   uv run pytest tests/test_security*.py tests/test_*security*.py -q
