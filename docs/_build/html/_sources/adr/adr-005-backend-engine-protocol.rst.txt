ADR-005: Backend Engine Protocol for Pluggable DataFrames
=========================================================

:Status: Accepted
:Date: 2024-10
:Affects: ``packages/pycypher/src/pycypher/backend_engine.py``,
          ``packages/pycypher/src/pycypher/backends/``

Context
-------

PyCypher was originally hard-coded to pandas for all DataFrame operations.
As datasets grow, users need alternative backends (DuckDB for analytical
queries, Polars for Arrow-native processing, Dask for distributed
computation) without rewriting the query engine.

Abstracting all ~918 pandas API calls would be impractical and fragile.

Decision
--------

Define a ``BackendEngine`` **protocol** that captures the ~15 primitive
operations that BindingFrame, Star, and MutationEngine actually depend on.
Operations are grouped into five categories:

1. **Scan** — load entity IDs from source data
2. **Transform** — filter, join, rename, concat, distinct, assign_column,
   drop_columns
3. **Aggregate** — grouped or full-table aggregation
4. **Order** — sort, limit, skip
5. **Materialise** — to_pandas, row_count, is_empty, memory_estimate_bytes

Concrete implementations:

- ``PandasBackend`` — default, zero-cost wrapper around existing pandas ops
- ``DuckDBBackend`` — SQL-based analytical backend with lazy evaluation
- ``PolarsBackend`` — Arrow-native backend

Backend selection is controlled via ``PYCYPHER_BACKEND`` environment variable
(``auto``, ``pandas``, ``duckdb``, ``polars``) or programmatically through
``Context(backend=...)``.

Consequences
------------

**Benefits:**

- Users can switch backends without changing query code
- Protocol surface is small (~15 methods) and stable
- Each backend can optimise for its strengths (DuckDB for aggregations,
  Polars for lazy evaluation)
- ``auto`` mode selects the best available backend

**Trade-offs:**

- Not all pandas features are available through the protocol; some advanced
  operations may fall back to pandas
- Backend implementations must be kept in sync when new primitive operations
  are added
- Testing must cover all backends to prevent drift

**Integration points:**

Operations that route through the backend:

- ``BindingFrame.join``, ``left_join``, ``cross_join``
- ``BindingFrame.filter``, ``rename``
- ``PatternMatcher`` — concat, distinct
- ``MutationEngine`` — CREATE via concat, DELETE via filter
