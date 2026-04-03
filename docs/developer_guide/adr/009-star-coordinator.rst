ADR-009: Star as Central Query Coordinator
==========================================

:Status: Accepted (with known limitations)
:Date: 2024
:Relates to: ``packages/pycypher/src/pycypher/star.py``

Context
-------

Users need a single entry point to execute Cypher queries against an in-memory
graph.  The execution pipeline involves parsing, AST conversion, semantic
validation, query planning, clause-by-clause execution, mutation coordination,
and result formatting.  These steps must be orchestrated in the correct order
with proper error handling and resource management.

Decision
--------

Implement ``Star`` as the main user-facing coordinator.  The primary API is:

.. code-block:: python

   star = Star(context=context)
   result = star.execute_query("MATCH (p:Person) RETURN p.name")

``Star`` delegates to focused components:

- ``get_default_parser()`` for parsing
- ``ASTConverter`` for AST conversion
- ``SemanticValidator`` for pre-execution validation
- ``PatternMatcher`` for MATCH clause execution
- ``BindingExpressionEvaluator`` for expression evaluation
- ``MutationEngine`` for CREATE/SET/DELETE
- ``QueryProfiler`` for timing (optional)

Alternatives Considered
-----------------------

1. **Separate parser, planner, executor classes with factory** — More modular
   but adds ceremony for the common case.  Users would need to assemble a
   pipeline before executing a query.

2. **Functional pipeline** (compose functions) — Clean but makes shared state
   (context, profiler, cache) awkward to thread through.

3. **Builder pattern** — ``QueryBuilder().parse(q).validate().plan().execute()``
   — Flexible but verbose for the 90% case of "just run this query."

Consequences
------------

- Simple, discoverable API: ``Star.execute_query()`` does everything.
- ``Star`` is becoming a god object — it coordinates too many concerns
  (see Task #6: "Decompose Star class").  Future work should extract a
  ``QueryExecutor`` that owns the pipeline while ``Star`` remains a
  thin user-facing facade.
- Adding new pipeline stages (e.g., cost-based planning) requires modifying
  ``Star``, which is not ideal for extensibility.
