PyCypher API
============

The PyCypher package provides comprehensive openCypher query parsing, AST
processing, and BindingFrame-based query execution against in-memory
DataFrames.

Quick Start
-----------

.. code-block:: python

   import pandas as pd
   from pycypher import ContextBuilder, Star

   # Build a graph context from DataFrames
   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })
   context = ContextBuilder().add_entity("Person", people).build()

   # Execute a Cypher query
   star = Star(context=context)
   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 28 RETURN p.name, p.age ORDER BY p.age"
   )
   print(result)

.. code-block:: python

   # Pre-execution validation
   from pycypher import validate_query

   errors = validate_query("MATCH (n:Person) RETURN m")
   for error in errors:
       print(f"{error.severity.value}: {error.message}")

.. code-block:: python

   # Backend selection (auto, pandas, duckdb, polars)
   context = ContextBuilder().add_entity("Person", people).build(backend="auto")
   print(context.backend_name)  # "pandas", "duckdb", or "polars"

Core Modules
------------

AST Models
~~~~~~~~~~

Pydantic-based AST node definitions for the openCypher grammar.  Every
parsed query is represented as a tree of these immutable model classes.

.. automodule:: pycypher.ast_models
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__

Grammar Parser
~~~~~~~~~~~~~~

Lark-based Cypher parser with LRU caching of parsed grammars and AST trees.

.. automodule:: pycypher.grammar_parser
   :members:
   :undoc-members:
   :show-inheritance:

Grammar Transformers
~~~~~~~~~~~~~~~~~~~~

Visitor classes that convert Lark parse trees into PyCypher AST models.

.. automodule:: pycypher.grammar_transformers
   :members:
   :undoc-members:
   :show-inheritance:

Grammar Rule Mixins
~~~~~~~~~~~~~~~~~~~

Modular mixin classes that compose the grammar transformer.  Each mixin
handles a specific grammar rule group (expressions, clauses, patterns, etc.).

.. automodule:: pycypher.grammar_rule_mixins
   :members:
   :undoc-members:
   :show-inheritance:

AST Converter
~~~~~~~~~~~~~

Converts dictionary-based Lark parse trees into typed Pydantic AST models.
Includes the ``from_cypher()`` classmethod for one-step parse-and-convert.

.. automodule:: pycypher.ast_converter
   :members:
   :undoc-members:
   :show-inheritance:

AST Rewriter
~~~~~~~~~~~~~

Programmatic AST transformations: ``WITH *`` expansion, ``RETURN`` stripping,
and multi-query merging for pipeline composition.

.. automodule:: pycypher.ast_rewriter
   :members:
   :undoc-members:
   :show-inheritance:

Data Model
----------

Relational Models
~~~~~~~~~~~~~~~~~

Entity/relationship data containers, ``Context``, and the relational algebra
operator hierarchy.  ``Context`` is the primary data holder — it stores entity
and relationship mappings and manages the backend engine lifecycle.

.. automodule:: pycypher.relational_models
   :members:
   :undoc-members:
   :show-inheritance:

Constants
~~~~~~~~~

Shared constants (column names, sentinel values) used across modules.

.. automodule:: pycypher.constants
   :members:
   :undoc-members:
   :show-inheritance:

Query Execution
---------------

Star (Query Orchestrator)
~~~~~~~~~~~~~~~~~~~~~~~~~

The main entry point for query execution.  ``Star`` accepts a ``Context``,
parses Cypher queries, and returns results as pandas DataFrames.

.. automodule:: pycypher.star
   :members:
   :undoc-members:
   :show-inheritance:

Clause Executor
~~~~~~~~~~~~~~~

Clause-by-clause execution engine for the BindingFrame IR.  Handles
clause-type dispatch (MATCH, WITH, RETURN, SET, DELETE, CREATE, etc.),
UNWIND processing, WHERE filter application, and dead column elimination.

.. automodule:: pycypher.clause_executor
   :members:
   :undoc-members:
   :show-inheritance:

Pattern Matcher
~~~~~~~~~~~~~~~

Handles MATCH clause pattern evaluation — node matching, relationship
traversal, and variable-length path expansion.

.. automodule:: pycypher.pattern_matcher
   :members:
   :undoc-members:
   :show-inheritance:

Path Expander
~~~~~~~~~~~~~

BFS-based path expansion for variable-length relationship patterns
(e.g. ``(a)-[:KNOWS*1..3]->(b)``).

.. automodule:: pycypher.path_expander
   :members:
   :undoc-members:
   :show-inheritance:

Expression Renderer
~~~~~~~~~~~~~~~~~~~

Converts AST expression nodes into human-readable column names for
RETURN/WITH clause output.

.. automodule:: pycypher.expression_renderer
   :members:
   :undoc-members:
   :show-inheritance:

BindingFrame Execution
----------------------

BindingFrame
~~~~~~~~~~~~

The core execution abstraction — a BindingFrame is a named, typed DataFrame
that tracks variable bindings through pattern matching and clause evaluation.

.. automodule:: pycypher.binding_frame
   :members:
   :undoc-members:
   :show-inheritance:

Evaluator Protocol
~~~~~~~~~~~~~~~~~~

Protocol interface for expression evaluation that breaks circular imports.
Defines the minimal contract sub-evaluators need from the top-level
expression evaluator.

.. automodule:: pycypher.evaluator_protocol
   :members:
   :undoc-members:
   :show-inheritance:

BindingExpression Evaluator
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Vectorised expression evaluator that dispatches AST expression nodes to
specialised evaluator modules.

.. automodule:: pycypher.binding_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Arithmetic Evaluator
~~~~~~~~~~~~~~~~~~~~

Handles ``+``, ``-``, ``*``, ``/``, ``%``, ``^`` with null propagation and
type coercion.

.. automodule:: pycypher.arithmetic_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Boolean Evaluator
~~~~~~~~~~~~~~~~~

Handles ``AND``, ``OR``, ``NOT``, ``XOR`` with Cypher's ternary null logic.

.. automodule:: pycypher.boolean_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Comparison Evaluator
~~~~~~~~~~~~~~~~~~~~

Handles ``=``, ``<>``, ``<``, ``>``, ``<=``, ``>=``, ``IS NULL``,
``IS NOT NULL``, unary operators, and ``CASE`` expressions.

.. automodule:: pycypher.comparison_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

String Predicate Evaluator
~~~~~~~~~~~~~~~~~~~~~~~~~~

Handles ``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``, ``IN``, and
regex matching with null propagation.

.. automodule:: pycypher.string_predicate_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

EXISTS Evaluator
~~~~~~~~~~~~~~~~

Handles ``EXISTS { MATCH ... }`` subquery evaluation and pattern
comprehensions with batched execution.

.. automodule:: pycypher.exists_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Aggregation Evaluator
~~~~~~~~~~~~~~~~~~~~~

Handles ``count()``, ``sum()``, ``avg()``, ``collect()``, ``stDev()``, etc.
with grouped and full-table aggregation.

.. automodule:: pycypher.aggregation_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Aggregation Planner
~~~~~~~~~~~~~~~~~~~

Detects aggregation expressions in RETURN/WITH items and plans
grouped vs full-table aggregation dispatch.

.. automodule:: pycypher.aggregation_planner
   :members:
   :undoc-members:
   :show-inheritance:

Collection Evaluator
~~~~~~~~~~~~~~~~~~~~

Handles list/map operations: ``[idx]``, ``keys()``, ``range()``, list
comprehensions, ``REDUCE``, and quantifier predicates (``all()``, ``any()``).

.. automodule:: pycypher.collection_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Scalar Functions
~~~~~~~~~~~~~~~~

Built-in Cypher scalar function registry (``toString()``, ``toInteger()``,
``trim()``, ``abs()``, math, string, temporal, and type-predicate functions).

.. automodule:: pycypher.scalar_functions
   :members:
   :undoc-members:
   :show-inheritance:

Scalar Function Evaluator
~~~~~~~~~~~~~~~~~~~~~~~~~

Dispatches scalar function calls from AST nodes to the registered
implementations.

.. automodule:: pycypher.scalar_function_evaluator
   :members:
   :undoc-members:
   :show-inheritance:

Execution Engine
----------------

Configuration
~~~~~~~~~~~~~

Centralized runtime configuration: query timeouts, cross-join limits,
result cache sizes, and BFS hop caps.  All settings are driven by
environment variables.

.. automodule:: pycypher.config
   :members:
   :undoc-members:
   :show-inheritance:

Backend Engine
~~~~~~~~~~~~~~

Protocol-based abstraction layer for pluggable DataFrame computation
backends.  Includes ``PandasBackend``, ``DuckDBBackend``, ``PolarsBackend``,
health checks, and a circuit breaker for graceful failover.

.. automodule:: pycypher.backend_engine
   :members:
   :undoc-members:
   :show-inheritance:

Query Planner
~~~~~~~~~~~~~

Handles memory estimation, operation reordering, and execution plan
optimization.  Integrates with the backend engine for adaptive join
strategy selection.

.. automodule:: pycypher.query_planner
   :members:
   :undoc-members:
   :show-inheritance:

Query Analyzer
~~~~~~~~~~~~~~

Pre-execution query analysis, planning, and MATCH reordering.  Performs
lazy computation graph planning, rule-based optimization, cardinality-based
MATCH clause reordering, LIMIT pushdown, and memory budget enforcement.

.. automodule:: pycypher.query_analyzer
   :members:
   :undoc-members:
   :show-inheritance:

Cardinality Estimator
~~~~~~~~~~~~~~~~~~~~~

Column and table statistics for cardinality estimation.  Provides
per-column NDV, null fraction, histograms, and self-correcting feedback
via actual-vs-estimated ratios.

.. automodule:: pycypher.cardinality_estimator
   :members:
   :undoc-members:
   :show-inheritance:

Scan Operators
~~~~~~~~~~~~~~

Scan and filter operators for the BindingFrame execution path.  Handles
entity/relationship table scanning, predicate pushdown, and dtype coercion.

.. automodule:: pycypher.scan_operators
   :members:
   :undoc-members:
   :show-inheritance:

Query Complexity
~~~~~~~~~~~~~~~~

Scores a parsed Cypher AST for resource-consumption risk: clause count,
join count, variable-length paths, aggregation depth, and cross-product
potential.

.. automodule:: pycypher.query_complexity
   :members:
   :undoc-members:
   :show-inheritance:

Query Optimizer
~~~~~~~~~~~~~~~

Rule-based query optimization pipeline: filter pushdown, limit pushdown,
join reordering, and predicate simplification.

.. automodule:: pycypher.query_optimizer
   :members:
   :undoc-members:
   :show-inheritance:

Query Validator
~~~~~~~~~~~~~~~

Combined validation: RETURN clause checks, clause ordering, and
structural rule enforcement.

.. automodule:: pycypher.query_validator
   :members:
   :undoc-members:
   :show-inheritance:

Query Formatter
~~~~~~~~~~~~~~~

Cypher query formatting and linting with configurable style rules.

.. automodule:: pycypher.query_formatter
   :members:
   :undoc-members:
   :show-inheritance:

Query Profiler
~~~~~~~~~~~~~~

Traces individual query execution to identify hot spots, track memory
allocation, and generate optimization recommendations.

.. automodule:: pycypher.query_profiler
   :members:
   :undoc-members:
   :show-inheritance:

Input Validator
~~~~~~~~~~~~~~~

Pre-parse input sanitization: query length limits, ID checks, content
safety, and parseability verification.

.. automodule:: pycypher.input_validator
   :members:
   :undoc-members:
   :show-inheritance:

Frame Joiner
~~~~~~~~~~~~

BindingFrame join and merge operations: coerce-join, multi-MATCH frame
merging, and OPTIONAL MATCH left-join semantics.

.. automodule:: pycypher.frame_joiner
   :members:
   :undoc-members:
   :show-inheritance:

LeapfrogTriejoin
~~~~~~~~~~~~~~~~

Worst-case optimal multi-way join algorithm (Veldhuizen 2014).  Activated
automatically for 3+ frame joins on a shared variable, achieving O(N^{w/2})
complexity vs O(N^{w-1}) for iterated binary joins.

.. automodule:: pycypher.leapfrog_triejoin
   :members:
   :undoc-members:
   :show-inheritance:

Graph Index
~~~~~~~~~~~

Graph-native index structures for accelerating pattern matching: adjacency
indexes, property value indexes, and label-partitioned indexes.

.. automodule:: pycypher.graph_index
   :members:
   :undoc-members:
   :show-inheritance:

Projection Planner
~~~~~~~~~~~~~~~~~~

Plans RETURN and WITH clause projections: alias inference, qualification,
and modifier application (ORDER BY, LIMIT, SKIP, DISTINCT).

.. automodule:: pycypher.projection_planner
   :members:
   :undoc-members:
   :show-inheritance:

Variable Manager
~~~~~~~~~~~~~~~~

Variable scoping, conflict detection, and safe renaming for multi-query
composition.

.. automodule:: pycypher.variable_manager
   :members:
   :undoc-members:
   :show-inheritance:

Pipeline
~~~~~~~~

Stage-based query execution pipeline with parse, validate, plan, and
execute stages.

.. automodule:: pycypher.pipeline
   :members:
   :undoc-members:
   :show-inheritance:

Lazy Evaluation
~~~~~~~~~~~~~~~

Defers computation until results are needed, enabling efficient processing
of large datasets and variable-length paths.

.. automodule:: pycypher.lazy_eval
   :members:
   :undoc-members:
   :show-inheritance:

Result Cache
~~~~~~~~~~~~

LRU query result cache with size-bounded eviction and TTL support.
Uses ``OrderedDict`` for O(1) LRU operations, readers-writer lock for
concurrent access, and generation-based invalidation on mutation commits.

.. automodule:: pycypher.result_cache
   :members:
   :undoc-members:
   :show-inheritance:

Timeout Handler
~~~~~~~~~~~~~~~

Timeout management for query execution.  Provides a context manager that
arms both cooperative and hard (SIGALRM) timeouts with reliable cleanup.

.. automodule:: pycypher.timeout_handler
   :members:
   :undoc-members:
   :show-inheritance:

Mutation Engine
~~~~~~~~~~~~~~~

Handles write operations: ``CREATE``, ``SET``, ``DELETE``, ``REMOVE``,
``MERGE``, and ``FOREACH`` clauses with shadow-layer transaction semantics.

.. automodule:: pycypher.mutation_engine
   :members:
   :undoc-members:
   :show-inheritance:

Multi-Query Composition
-----------------------

Multi-Query Analyzer
~~~~~~~~~~~~~~~~~~~~

Builds a dependency graph from multiple Cypher queries by analyzing
entity/relationship types produced and consumed by each query.

.. automodule:: pycypher.multi_query_analyzer
   :members:
   :undoc-members:
   :show-inheritance:

Multi-Query Executor
~~~~~~~~~~~~~~~~~~~~

Orchestrates execution of multi-query pipelines with dependency-aware
ordering and shared context propagation.

.. automodule:: pycypher.multi_query_executor
   :members:
   :undoc-members:
   :show-inheritance:

Validation
----------

Semantic Validator
~~~~~~~~~~~~~~~~~~

Pre-execution validation: undefined variable detection, aggregation rule
checking, type constraint verification.

.. automodule:: pycypher.semantic_validator
   :members:
   :undoc-members:
   :show-inheritance:

Ingestion Layer
---------------

The ingestion layer provides Arrow (via PyArrow) as the canonical in-memory
tabular format and DuckDB as the universal ingestion adapter.

Context Builder
~~~~~~~~~~~~~~~

Fluent API for constructing ``Context`` objects from DataFrames, Parquet
files, CSV files, and other tabular sources.

.. automodule:: pycypher.ingestion.context_builder
   :members:
   :undoc-members:
   :show-inheritance:

Data Sources
~~~~~~~~~~~~

.. automodule:: pycypher.ingestion.data_sources
   :members:
   :undoc-members:
   :show-inheritance:

Pipeline Config
~~~~~~~~~~~~~~~

.. automodule:: pycypher.ingestion.config
   :members:
   :undoc-members:
   :show-inheritance:

DuckDB Reader
~~~~~~~~~~~~~

.. automodule:: pycypher.ingestion.duckdb_reader
   :members:
   :undoc-members:
   :show-inheritance:

Arrow Utilities
~~~~~~~~~~~~~~~

.. automodule:: pycypher.ingestion.arrow_utils
   :members:
   :undoc-members:
   :show-inheritance:

Output Writer
~~~~~~~~~~~~~

.. automodule:: pycypher.ingestion.output_writer
   :members:
   :undoc-members:
   :show-inheritance:

Security
~~~~~~~~

.. automodule:: pycypher.ingestion.security
   :members:
   :undoc-members:
   :show-inheritance:

Sinks
-----

Neo4j Sink
~~~~~~~~~~

Write pycypher query results to a Neo4j graph database using idempotent
``MERGE`` semantics.  Requires the ``neo4j`` driver (included in ``uv sync --group dev``)::

    uv pip install neo4j

.. automodule:: pycypher.sinks.neo4j
   :members:
   :undoc-members:
   :show-inheritance:

Distributed Execution
---------------------

Cluster
~~~~~~~

Coordination protocols, worker registration, query routing, and fault
tolerance for enterprise-scale distributed execution.  Provides local
implementations for testing; network transport is deferred to Phase 3.

.. automodule:: pycypher.cluster
   :members:
   :undoc-members:
   :show-inheritance:

CLI
---

NMETL CLI
~~~~~~~~~

Command-line interface for running Cypher queries against YAML-configured
data pipelines.

.. automodule:: pycypher.nmetl_cli
   :members:
   :undoc-members:
   :show-inheritance:

Security & Observability
------------------------

Audit Logging
~~~~~~~~~~~~~

Opt-in structured query audit logging.  Emits one JSON record per query
execution with query_id, timing, status, and row counts.  Activated via
``PYCYPHER_AUDIT_LOG`` environment variable.

.. automodule:: pycypher.audit
   :members:
   :undoc-members:
   :show-inheritance:

Rate Limiter
~~~~~~~~~~~~

Thread-safe token-bucket rate limiter for resource protection in
multi-tenant deployments.  Configured via ``PYCYPHER_RATE_LIMIT_QPS``
and ``PYCYPHER_RATE_LIMIT_BURST``.

.. automodule:: pycypher.rate_limiter
   :members:
   :undoc-members:
   :show-inheritance:

Utilities
---------

Types
~~~~~

Type aliases and type definitions used across the codebase.

.. automodule:: pycypher.cypher_types
   :members:
   :undoc-members:
   :show-inheritance:

Query Learning — ML-Powered Optimization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Adaptive query optimization using online learning with exponential moving
averages.  No heavyweight ML dependencies.

.. automodule:: pycypher.query_learning
   :members:
   :undoc-members:
   :show-inheritance:

Exceptions
~~~~~~~~~~

Complete exception hierarchy.  All exceptions are importable from the
top-level ``pycypher`` package.

.. automodule:: pycypher.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
