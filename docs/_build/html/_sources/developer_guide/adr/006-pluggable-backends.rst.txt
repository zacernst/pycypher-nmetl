ADR-006: Pluggable Backend Engine Protocol
==========================================

:Status: Accepted
:Date: 2025
:Relates to: ``packages/pycypher/src/pycypher/backend_engine.py``

Context
-------

PyCypher's execution engine was tightly coupled to pandas.  For large datasets,
pandas becomes a bottleneck — it cannot push predicates into native C code
and struggles with datasets that exceed memory.  Users need the option to use
DuckDB (analytical SQL), Polars (Arrow-native lazy evaluation), or Dask
(distributed) without changing their Cypher queries.

Decision
--------

Define a ``BackendEngine`` protocol that captures ~15 primitive DataFrame
operations (filter, join, project, aggregate, sort, limit, etc.).  Three
implementations:

- ``PandasBackend`` — default, zero-cost wrapper around existing pandas code
- ``DuckDBBackend`` — pushes operations to DuckDB's SQL engine for analytical
  workloads
- ``PolarsBackend`` — uses Polars lazy evaluation for Arrow-native execution

Selection via ``Context(backend="duckdb")`` or ``PYCYPHER_BACKEND`` environment
variable.  The ``"auto"`` mode selects the backend based on dataset size and
available libraries.

Alternatives Considered
-----------------------

1. **Hard-code pandas throughout** — No migration path for large datasets.

2. **Abstract all 918 pandas API calls** — Discovered during audit; impractical
   to abstract every call.  The ~15 primitive operations capture the essential
   semantics.

3. **Use an existing dataframe abstraction library** (e.g., Ibis, Narwhals) —
   None were mature enough at decision time, and the abstraction surface was
   small enough to own.

Consequences
------------

- Users opt into DuckDB/Polars transparently — no query changes needed.
- User-facing API always returns ``pd.DataFrame`` via ``to_pandas()`` escape
  hatch for compatibility.
- All existing pandas-based code unaffected; backend adoption is incremental.
- ~15 operations is a manageable abstraction surface to maintain.
