ADR-004: Query-Scoped Shadow Write Atomicity
=============================================

:Status: Accepted
:Date: 2025
:Relates to: ``packages/pycypher/src/pycypher/mutation_engine.py``

Context
-------

Cypher queries with SET, CREATE, or DELETE clauses mutate the in-memory graph.
If a query fails mid-execution (e.g., a type error in a later clause), the
context could be left in a partially-mutated state — some entities updated,
others not.  This violates the principle of least surprise and makes error
recovery impossible.

Decision
--------

Buffer all mutations in a per-query **shadow layer** on ``Context``.  The
mutation lifecycle is:

.. code-block:: python

   context.begin_query()    # initialise shadow layer
   try:
       result = execute_inner(query)
       context.commit_query()   # promote shadows to canonical tables
       return result
   except Exception:
       context.rollback_query() # discard all pending mutations
       raise

``MutationEngine`` coordinates CREATE, SET, DELETE, REMOVE, and MERGE
operations, writing exclusively to the shadow layer until commit.

Alternatives Considered
-----------------------

1. **Direct mutation** — Simplest implementation but breaks atomicity; no
   recovery from partial failures.

2. **Per-clause checkpoints** — More granular but significantly more complex;
   unclear what "partial success" means for a multi-clause query.

3. **Undo log with replay** — Possible but adds complexity for a scenario
   where discard-and-retry is simpler and sufficient.

Consequences
------------

- Failed queries never leave the context in a partially-mutated state.
- Shadow layer memory is proportional to the number of mutated entities
  (not total graph size).
- Query semantics are predictable and testable.
- MERGE operations can check existing state while buffering new writes.
