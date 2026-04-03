ADR-004: Shadow-Write Mutation Pattern for Atomicity
====================================================

:Status: Accepted
:Date: 2024-08
:Affects: ``packages/pycypher/src/pycypher/mutation_engine.py``,
          ``packages/pycypher/src/pycypher/relational_models.py``

Context
-------

Cypher supports write operations (CREATE, SET, DELETE, MERGE, REMOVE) that
must be atomic — either all mutations in a query succeed, or none take effect.
A query like ``MATCH (a) SET a.x = 1 SET a.y = a.x + 1`` must see consistent
state within the same query, and a failing query must leave the context
unchanged.

Without atomicity, a crash mid-query could leave entity tables in a
partially-mutated state with no way to recover.

Decision
--------

Implement a **two-phase shadow-write pattern** in ``Context``:

1. **begin_query()** — opens a transaction by initialising an empty shadow
   layer (``context._shadow = {}``)
2. **Mutation methods** (SET, CREATE, DELETE, etc.) write to the shadow layer
   rather than modifying original DataFrames in place
3. **commit_query()** — promotes shadow DataFrames to the canonical
   ``source_obj`` on each affected EntityTable/RelationshipTable
4. **rollback_query()** — discards the shadow layer, leaving ``source_obj``
   unchanged

All write-path operations go through ``MutationEngine``, which delegates
to Context's shadow layer.

Consequences
------------

**Benefits:**

- Full rollback semantics — failed queries never corrupt state
- Consistent read-your-writes within a single query (mutations read from
  shadow when available)
- Simple implementation — shadow is a dict of entity-type to DataFrame
- No external transaction coordinator needed

**Trade-offs:**

- Memory overhead: shadow layer holds copies of modified DataFrames during
  query execution
- Not suitable for concurrent multi-query transactions (single-writer model)
- Shadow merge on commit is O(n) in the number of modified entity types

**Lifecycle:**

.. code-block:: text

   begin_query()
       ├── SET a.x = 1      → writes to _shadow["Person"]
       ├── SET a.y = a.x + 1 → reads from _shadow, writes to _shadow
       └── success?
           ├── yes → commit_query()   → _shadow → source_obj
           └── no  → rollback_query() → discard _shadow
