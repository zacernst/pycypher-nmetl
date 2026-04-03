ADR-007: Graph-Native Index Structures
=======================================

:Status: Accepted
:Date: 2025
:Relates to: ``packages/pycypher/src/pycypher/graph_index.py``

Context
-------

Pattern matching in Cypher requires finding neighbors, looking up entities by
property values, and filtering by labels.  Without indexes, every pattern
match performs a full table scan — O(E) for each relationship traversal and
O(N) for each property filter.  For graphs with millions of edges, this makes
interactive queries impractical.

Decision
--------

Implement three graph-native index types managed by ``GraphIndexManager``:

- **AdjacencyIndex** — maps ``(entity_id, direction)`` to neighbor lists.
  Reduces neighbor lookups from O(E) to O(degree).

- **PropertyValueIndex** — maps ``(entity_type, property, value)`` to ID sets
  via hash table.  Reduces equality filters from O(N) to O(1).

- **EntityLabelIndex** — sorted ID arrays per label.  Reduces label membership
  tests to O(log n) via binary search.

Indexes are built lazily on first access and invalidated on mutations (CREATE,
SET, DELETE).  Index data uses frozen tuples for thread-safe concurrent reads.

Alternatives Considered
-----------------------

1. **No indexing** — Unacceptable for any non-trivial graph size.

2. **B-tree indexes** — More overhead for construction and maintenance; similar
   lookup performance for equality queries, slightly better for range queries
   which are uncommon in pattern matching.

3. **Hash-only indexes** — Would work for equality but not ordered access;
   the sorted label index enables efficient range scans and set intersection.

Consequences
------------

- Pattern matching goes from O(E) to O(degree) for each relationship traversal.
- Enables the query planner to choose between hash join, broadcast join,
  merge join, or nested-loop join based on index availability and estimated
  cardinalities.
- Memory cost proportional to data size (index storage mirrors data storage).
- Lazy construction avoids upfront cost for indexes that may never be needed.
- Invalidation on mutation ensures index consistency without explicit
  management.
