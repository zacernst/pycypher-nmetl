ADR-002: BindingFrame as Intermediate Representation
====================================================

:Status: Accepted
:Date: 2024-06
:Affects: ``packages/pycypher/src/pycypher/binding_frame.py``,
          ``packages/pycypher/src/pycypher/star.py``

Context
-------

The original query execution pipeline used ``Relation`` subclasses
(``EntityTable``, ``Join``, ``FilterRows``, ``Projection``) that operated
on pandas DataFrames with prefixed column names like ``Person__name`` and
opaque ``HASH_ID`` columns.  This caused three recurring problems:

1. **Opaque column names** — ``Projection`` produced ``HASH_ID`` columns
   that required a ``variable_map`` lookup table to trace back to Cypher
   variables.
2. **Prefixed entity columns** — Column names like ``Person__name`` leaked
   into operator logic, requiring hacks like ``_ensure_full_entity_data``.
3. **Metadata threading** — ``variable_map`` and ``variable_type_map`` had
   to be threaded through every ``Relation`` subclass, creating tight coupling.

Decision
--------

Introduce ``BindingFrame`` as the intermediate representation for query
execution.  A BindingFrame is a pandas DataFrame where:

- **Columns are named after Cypher variables** (e.g. ``p``, ``q``, ``r``)
- **Each row is one assignment** of entity/relationship IDs to those variables
- **Attributes are never stored** in the frame; they are fetched on-demand
  from ``Context`` via ``get_property()``

The column *is* the variable — no lookup table required.

Consequences
------------

**Benefits:**

- Eliminates the ``variable_map``/``variable_type_map`` metadata entirely
- Column names are self-describing — debugging is straightforward
- Clean separation between structure (which IDs match) and content (what
  properties those IDs have)
- Enables lazy attribute resolution — only fetch what RETURN actually needs
- Simplifies operator implementation (join, filter, project all work on
  variable-named columns directly)

**Trade-offs:**

- Property access requires a lookup into ``Context`` rather than reading
  from the DataFrame directly — adds one level of indirection
- Migrating from the Relation-based pipeline required touching most of the
  execution path

**Performance notes:**

- Module-level debug checks (set once at import) avoid per-call overhead
- Pre-built numpy ufuncs for ndarray normalisation
- ``_null_series()`` uses ``np.empty + fill`` instead of list allocation
