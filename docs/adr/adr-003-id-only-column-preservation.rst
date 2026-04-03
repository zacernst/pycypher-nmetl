ADR-003: ID-Only Column Preservation Strategy
==============================================

:Status: Accepted
:Date: 2024-06
:Affects: ``packages/pycypher/src/pycypher/constants.py``,
          ``packages/pycypher/src/pycypher/relational_models.py``,
          ``packages/pycypher/src/pycypher/binding_frame.py``

Context
-------

During query execution, the engine must track which entities and relationships
match a pattern (MATCH), filter them (WHERE), and then project attributes
(RETURN).  The naive approach — carrying all attributes through every
intermediate step — wastes memory and complicates joins when different entity
types have overlapping column names.

Decision
--------

Intermediate execution state (BindingFrames) stores **only identity columns**:

- ``__ID__`` — unique entity identifier
- ``__SOURCE__`` — relationship source node ID
- ``__TARGET__`` — relationship target node ID

Attributes (``name``, ``age``, etc.) are fetched on-demand from the
``Context`` layer only when needed — typically during RETURN projection or
WHERE property access.

This is enforced by convention: BindingFrame columns correspond to Cypher
variables and contain only ID values.

Consequences
------------

**Benefits:**

- Joins operate on small ID-only frames, reducing memory and improving speed
- No column name collisions between entity types during joins
- Clear separation of concerns: BindingFrame tracks *structure* (which IDs
  match), Context provides *content* (what properties those IDs have)
- Lazy attribute resolution means unused properties are never fetched

**Trade-offs:**

- Every property access in WHERE or RETURN requires a lookup into Context,
  adding one indirection step per property reference
- Property caching is needed for repeated access to the same property within
  a single query (handled by the property lookup cache)

**Constants:**

.. code-block:: python

   ID_COLUMN = "__ID__"
   RELATIONSHIP_SOURCE_COLUMN = "__SOURCE__"
   RELATIONSHIP_TARGET_COLUMN = "__TARGET__"
