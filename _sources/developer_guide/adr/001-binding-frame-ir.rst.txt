ADR-001: BindingFrame as Core Intermediate Representation
=========================================================

:Status: Accepted
:Date: 2025-01
:Relates to: ``packages/pycypher/src/pycypher/binding_frame.py``

Context
-------

The original query execution engine used opaque relational algebra operators
with 32-character hex ``HASH_ID`` column names and ``EntityType__propertyname``
prefixed columns.  Every operator had to thread a ``variable_map`` dictionary
to translate between Cypher variable names and internal column names.  This
was fragile, error-prone, and made debugging nearly impossible — a DataFrame
dump showed hex IDs rather than recognizable variable names.

Decision
--------

Replace the legacy relational algebra with **BindingFrame**, a DataFrame-native
IR where column names *are* Cypher variable names.  Each row represents one
possible assignment of entity/relationship IDs to those variables.  Properties
are fetched on demand via ``get_property(var, prop)`` rather than being stored
in the frame.

Key operations:

- ``EntityScan(entity_type, var_name)`` — produces a single-column frame
- ``RelationshipScan(rel_type, var_name)`` — produces a three-column frame
  (rel ID, source, target)
- ``join(left_col, right_col)`` — inner join on shared structural key
- ``filter(mask)`` — boolean mask filter

Alternatives Considered
-----------------------

1. **Keep legacy relational algebra** — Too fragile; ``variable_map`` threading
   was the root cause of multiple production bugs.

2. **Prefixed column names** (``Person__name``) — Still bleeds internal naming
   conventions into operator logic and forces every operator to understand
   the prefix format.

3. **Hash-ID columns with external symbol table** — Marginally cleaner but
   still requires lookup-table indirection for every property access.

Consequences
------------

- Eliminates three fragility sources: opaque HASH_IDs, prefixed columns,
  ``variable_map`` threading.
- Self-documenting: a DataFrame dump shows ``p``, ``r``, ``q`` instead of
  ``a7f3...``.
- On-demand property fetch via ID-keyed vectorized lookup avoids fetching
  unused properties.
- Requires ``GraphIndexManager`` for efficient property/adjacency lookups.
- All evaluators, the pattern matcher, and the mutation engine operate
  directly on BindingFrame columns.
