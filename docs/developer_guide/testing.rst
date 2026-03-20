Testing
=======

Comprehensive guide to running, writing, and maintaining tests for PyCypher.

.. contents:: On this page
   :local:
   :depth: 2


Running Tests
-------------

Quick Reference
~~~~~~~~~~~~~~~

All commands use ``uv run`` to execute within the workspace virtual environment.

.. code-block:: bash

   # Run all tests (parallel)
   make test

   # Run all tests (serial, useful for debugging)
   make test-serial

   # Fast: stop on first failure, parallel
   make test-fast

   # Quick: minimal output, no coverage
   make test-quick

   # Re-run only previously failed tests
   make test-failed

   # Re-run failed tests first, then all
   make test-changed

Targeting Specific Tests
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Single file
   uv run pytest tests/test_ast_models.py

   # Single test class
   uv run pytest tests/test_ast_models.py::TestPatterns

   # Single test method
   uv run pytest tests/test_ast_models.py::TestPatterns::test_node_pattern_basic

   # By keyword pattern (matches test names)
   uv run pytest -k "aggregation and not percentile"

   # By marker
   uv run pytest -m "not integration"

Coverage
~~~~~~~~

.. code-block:: bash

   # HTML report in coverage_report/
   make coverage

   # Detailed terminal + HTML report
   make coverage-detailed

   # Inline coverage for a specific file
   uv run pytest tests/test_star_gaps.py --cov=pycypher.star --cov-report=term-missing


Test Architecture
-----------------

Directory Layout
~~~~~~~~~~~~~~~~

All tests live in ``tests/`` at the repository root. The project uses a flat
directory structure — each test file targets a specific module or feature area:

.. code-block:: text

   tests/
   ├── conftest.py                     # Shared fixtures (Context, Star, DataFrames)
   ├── fixtures/data/                  # Static test data files (Parquet, CSV)
   ├── benchmarks/                     # Performance benchmark tests
   ├── large_dataset/                  # Large-dataset integration tests
   ├── test_ast_models.py              # AST node construction and validation
   ├── test_grammar_parser.py          # Lark grammar → raw AST parsing
   ├── test_semantic_validator.py       # Static semantic analysis
   ├── test_star_gaps.py               # Star execution edge cases
   ├── test_binding_frame.py           # BindingFrame IR operations
   ├── test_create_clause.py           # CREATE clause execution
   ├── test_set_clause_execution.py    # SET clause execution
   ├── test_aggregation_dispatch.py    # Aggregation function routing
   └── ...                             # ~300 test files covering all features

Shared Fixtures
~~~~~~~~~~~~~~~

``conftest.py`` provides reusable fixtures for the most common test patterns.
Always prefer these over creating one-off test data:

.. code-block:: python

   def test_basic_query(person_star: Star) -> None:
       """person_star provides a Star with 4 Person entities."""
       result = person_star.execute_query(
           "MATCH (p:Person) RETURN p.name AS name"
       )
       assert len(result) == 4

**Available fixtures:**

``people_df``
   4-row Person DataFrame (Alice, Bob, Carol, Dave) with name, age, dept, salary.

``knows_df``
   3-row KNOWS relationship DataFrame with since attribute.

``person_star``
   Star with Person entities only.

``social_star``
   Star with Person entities and KNOWS relationships.

``empty_star``
   Star with empty context (no entities or relationships).

``scalar_registry``
   Shared ``ScalarFunctionRegistry`` singleton.


Writing Tests
-------------

Test Structure
~~~~~~~~~~~~~~

All test functions and methods **must** have type annotations.  Use
``pytest``-style classes to group related tests:

.. code-block:: python

   from __future__ import annotations

   import pandas as pd
   import pytest
   from pycypher import Star
   from pycypher.relational_models import Context, EntityMapping, EntityTable

   ID_COLUMN = "__ID__"


   @pytest.fixture
   def product_star() -> Star:
       """Star with a Product entity table."""
       df = pd.DataFrame({
           ID_COLUMN: [1, 2, 3],
           "name": ["Widget", "Gadget", "Doohickey"],
           "price": [9.99, 19.99, 4.99],
       })
       table = EntityTable.from_dataframe("Product", df)
       ctx = Context(entity_mapping=EntityMapping(mapping={"Product": table}))
       return Star(context=ctx)


   class TestProductQueries:
       """Tests for product catalog queries."""

       def test_return_all_products(self, product_star: Star) -> None:
           result = product_star.execute_query(
               "MATCH (p:Product) RETURN p.name AS name"
           )
           assert len(result) == 3
           assert set(result["name"]) == {"Widget", "Gadget", "Doohickey"}

       def test_filter_by_price(self, product_star: Star) -> None:
           result = product_star.execute_query(
               "MATCH (p:Product) WHERE p.price > 10 RETURN p.name AS name"
           )
           assert set(result["name"]) == {"Gadget"}

Testing Query Execution (End-to-End)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most common test pattern exercises the full pipeline — parsing, planning,
and execution — via ``Star.execute_query()``:

.. code-block:: python

   def test_aggregation_with_grouping(social_star: Star) -> None:
       """Grouped COUNT aggregation returns correct counts per department."""
       result = social_star.execute_query(
           "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS cnt"
       )
       # Convert to dict for order-independent assertion
       dept_counts = dict(zip(result["dept"], result["cnt"], strict=False))
       assert dept_counts["eng"] == 2
       assert dept_counts["mktg"] == 1

Testing AST Construction
~~~~~~~~~~~~~~~~~~~~~~~~

For parser and AST tests, validate structure without executing queries:

.. code-block:: python

   from pycypher.ast_models import ASTConverter, Query, Match, Return

   def test_parse_match_return() -> None:
       query = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")

       assert isinstance(query, Query)
       assert len(query.clauses) == 2
       assert isinstance(query.clauses[0], Match)
       assert isinstance(query.clauses[1], Return)

Testing Error Paths
~~~~~~~~~~~~~~~~~~~

Always verify that errors produce clear, actionable messages:

.. code-block:: python

   import pytest

   def test_empty_query_raises(person_star: Star) -> None:
       with pytest.raises(ValueError, match="must not be empty"):
           person_star.execute_query("")

   def test_unknown_function_suggests_correction(person_star: Star) -> None:
       with pytest.raises(Exception, match="(?i)did you mean"):
           person_star.execute_query(
               "MATCH (p:Person) RETURN tUpperr(p.name)"
           )

Testing Mutations (CREATE, SET, DELETE)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mutation tests should verify both the returned result and the context state:

.. code-block:: python

   def test_set_updates_context(person_star: Star) -> None:
       """SET clause persists changes to the underlying context."""
       person_star.execute_query(
           "MATCH (p:Person) WHERE p.name = 'Alice' SET p.age = 31 RETURN p"
       )
       # Verify the mutation persisted in the context
       person_df = person_star.context.entity_mapping["Person"].source_obj
       alice_row = person_df[person_df["name"] == "Alice"].iloc[0]
       assert alice_row["age"] == 31

Testing Shadow State Atomicity
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Mutations use a shadow-write pattern (begin → execute → commit/rollback).
Verify that failed queries leave the context unchanged:

.. code-block:: python

   def test_failed_mutation_rolls_back(person_star: Star) -> None:
       """A query that fails mid-execution must not leave partial mutations."""
       original_names = set(
           person_star.context.entity_mapping["Person"].source_obj["name"]
       )
       with pytest.raises(Exception):
           person_star.execute_query("MATCH (p:Person) SET p.INVALID RETURN p")
       current_names = set(
           person_star.context.entity_mapping["Person"].source_obj["name"]
       )
       assert current_names == original_names
       assert person_star.context._shadow == {}


Performance Testing
-------------------

Writing Robust Performance Tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Performance assertions are inherently noisy.  Use statistical approaches
to avoid flaky tests:

.. code-block:: python

   import statistics
   import time

   def test_view_faster_than_copy() -> None:
       """Verify that DataFrame view is faster than copy for read-only access."""
       n_trials = 7
       copy_times: list[float] = []
       view_times: list[float] = []

       df = pd.DataFrame({"a": range(10_000), "b": range(10_000)})

       for _ in range(n_trials):
           t0 = time.perf_counter()
           for _ in range(50):
               _ = df[["a"]].copy()
           copy_times.append(time.perf_counter() - t0)

           t0 = time.perf_counter()
           for _ in range(50):
               _ = df[["a"]]
           view_times.append(time.perf_counter() - t0)

       # Use median and generous tolerance (3x) for CI stability
       assert statistics.median(view_times) <= statistics.median(copy_times) * 3.0

**Rules for performance tests:**

1. Use **median of multiple trials** (≥5) instead of single measurements
2. Apply **generous tolerance** (2–3x) — test for regressions, not exact speedups
3. Include a **descriptive assertion message** with actual values for debugging
4. Never assert exact timings — only relative comparisons or order-of-magnitude

Benchmark Tests
~~~~~~~~~~~~~~~

The ``tests/benchmarks/`` directory contains pytest-benchmark tests:

.. code-block:: bash

   uv run pytest tests/benchmarks/ --benchmark-only


Integration Tests
-----------------

Spark Integration
~~~~~~~~~~~~~~~~~

Spark tests require a running Spark cluster or use ``local[*]`` mode:

.. code-block:: bash

   # Run Spark tests (requires Docker)
   make test-spark

   # Or directly with local mode
   uv run pytest tests/ -k "spark" -v

Neo4j Integration
~~~~~~~~~~~~~~~~~

Neo4j tests require a running Neo4j instance:

.. code-block:: bash

   # Start Neo4j via Docker
   docker compose up neo4j -d

   # Run Neo4j tests
   make test-neo4j

   # Or with custom connection
   NEO4J_URI=bolt://localhost:7687 NEO4J_PASSWORD=secret uv run pytest -k "neo4j"

Large Dataset Tests
~~~~~~~~~~~~~~~~~~~

Tests that exercise distributed backends (Dask, DuckDB, Polars):

.. code-block:: bash

   make test-large-dataset


Configuration for Testing
-------------------------

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

Tests respect the same ``PYCYPHER_*`` environment variables as production
(see ``pycypher.config``):

``PYCYPHER_QUERY_TIMEOUT_S`` (default: None)
   Default query timeout in seconds.

``PYCYPHER_MAX_CROSS_JOIN_ROWS`` (default: 10,000,000)
   Hard ceiling on cross-join result size (rows).

``PYCYPHER_RESULT_CACHE_MAX_MB`` (default: 100)
   Maximum result cache size in megabytes.

``PYCYPHER_RESULT_CACHE_TTL_S`` (default: 0)
   Result cache TTL in seconds (0 = no expiry).

``PYCYPHER_MAX_UNBOUNDED_PATH_HOPS`` (default: 20)
   BFS hop limit for unbounded variable-length paths (``[*]``).

Timeout Configuration
~~~~~~~~~~~~~~~~~~~~~

All tests have a 30-second timeout enforced by ``pytest-timeout`` (configured
in ``pyproject.toml``).  Tests that need longer (e.g. Spark integration) should
use:

.. code-block:: python

   @pytest.mark.timeout(120)
   def test_large_spark_query(spark_session):
       ...


Continuous Integration
----------------------

The CI pipeline (``.github/workflows/ci.yml``) runs three jobs:

1. **Tests**: Full test suite with ``pytest --timeout=30`` on Python 3.14t
2. **Dependency Compatibility**: Verifies all backend imports (Dask, DuckDB, etc.)
3. **Documentation**: Builds Sphinx docs to catch broken references

Tests must pass before merging to ``main``.


Type Checking
-------------

Run type checks with ``ty`` (not mypy):

.. code-block:: bash

   uv run ty check

All functions and methods **must** have type annotations.  The project uses
Python 3.14.0 (free-threaded build).
