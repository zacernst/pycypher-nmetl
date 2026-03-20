|Install and run tests|

|Build Sphinx documentation|

PyCypher — Cypher Query Engine for Python
==========================================

PyCypher is a Cypher query engine that executes openCypher queries against
ordinary pandas DataFrames.  It is built on a Lark-based parser, a
Pydantic AST, and a BindingFrame execution layer — no graph database
required.

Quick start
-----------

.. code:: python

   import pandas as pd
   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   # Build a context from plain DataFrames
   ctx = ContextBuilder.from_dict({
       "Person": pd.DataFrame({
           "__ID__":  ["p1", "p2", "p3"],
           "name":    ["Alice", "Bob", "Carol"],
           "age":     [30, 25, 35],
       }),
       "KNOWS": pd.DataFrame({
           "__SOURCE__": ["p1", "p2"],
           "__TARGET__": ["p2", "p3"],
           "since":     [2020, 2021],
       }),
   })

   star = Star(context=ctx)

   # Simple property projection
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY age ASC"
   )
   print(result)

   # Relationship traversal
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS from, b.name AS to"
   )
   print(result)

   # Aggregation
   result = star.execute_query(
       "MATCH (p:Person) RETURN count(p) AS total, avg(p.age) AS avg_age"
   )
   print(result)

The ``Star`` class is the main entry point.  ``ContextBuilder.from_dict``
automatically detects entity tables (any key with a ``__ID__`` column) and
relationship tables (any key with both ``__SOURCE__`` and ``__TARGET__``
columns).

Use ``parameters`` for safe value injection:

.. code:: python

   result = star.execute_query(
       "MATCH (p:Person) WHERE p.name = $name AND p.age > $min_age "
       "RETURN p.age AS age",
       parameters={"name": "Alice", "min_age": 20},
   )


Supported Cypher features
--------------------------

Pattern matching
~~~~~~~~~~~~~~~~

- Node and relationship patterns with named and anonymous variables
- Directed (``-->``/``<--``) and undirected (``--``) relationship traversal
- Typed (``[:KNOWS]``) and untyped (``[]``) relationship patterns
- Relationship type union: ``[:KNOWS|LIKES]`` or ``[:KNOWS|:LIKES]``
- Inline node-property filters: ``MATCH (p:Person {name: 'Alice'})``
- Inline relationship-property filters: ``-[r:KNOWS {since: 2020}]->``
- Variable-length paths: ``[*m..n]`` BFS; unbounded ``[*]`` (capped)
- ``shortestPath()`` / ``allShortestPaths()`` — minimum-hop path finding
- Multiple ``MATCH`` clauses in one query (cross-join)
- ``OPTIONAL MATCH`` — left-outer-join semantics; unmatched rows yield ``null``

Filtering and predicates
~~~~~~~~~~~~~~~~~~~~~~~~

- ``WHERE`` with full expression support, boolean operators, comparisons
- String predicates: ``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``, ``=~``
- Membership tests: ``IN``, ``NOT IN``
- Null checks: ``IS NULL``, ``IS NOT NULL``
- Null literal: ``null``

Expressions
~~~~~~~~~~~

- Arithmetic: ``+``, ``-``, ``*``, ``/``, ``%``, ``^``
- Searched and simple ``CASE … WHEN … THEN … ELSE … END``
- List comprehensions: ``[x IN list WHERE cond | expr]``
- Quantifier predicates: ``all()``, ``any()``, ``none()``, ``single()``
- Accumulation: ``reduce(acc = init, var IN list | step)``
- Full scalar function registry (see ``Star.available_functions()``)
- Graph introspection: ``id(n)``, ``labels(n)``, ``type(r)``, ``keys(n)``, ``properties(n)``

Control flow
~~~~~~~~~~~~

- ``UNWIND`` — expand a list into individual rows (standalone or after MATCH/WITH)
- ``FOREACH (var IN list | ...)`` — iterate over a list and execute inner clauses

Projection and aggregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``WITH`` — projection, grouped aggregation; DISTINCT, ORDER BY, SKIP, LIMIT
- ``WITH *`` — pass-through all in-scope bindings (ORDER BY / SKIP / LIMIT apply)
- ``RETURN`` — expression projection; DISTINCT, ORDER BY (ASC/DESC), SKIP, LIMIT
- ``RETURN *`` — return all in-scope variables
- Standalone ``RETURN``/``WITH`` (no preceding MATCH) — evaluates literal and scalar expressions directly
- Aggregations: ``count()``, ``sum()``, ``avg()``, ``min()``, ``max()``,
  ``collect()``, ``stdev()`` (sample), ``stdevp()`` (population),
  ``percentileCont(expr, p)`` (linear), ``percentileDisc(expr, p)`` (discrete)

Set operations
~~~~~~~~~~~~~~

- ``UNION`` (deduplicates) and ``UNION ALL`` (preserves duplicates)

Mutation
~~~~~~~~

All mutations are staged in a shadow layer and committed atomically when
the query succeeds; a failed query rolls back automatically.

- ``CREATE`` — insert nodes and relationships; new entity types are registered automatically
- ``SET`` — write a computed expression back to an entity property
- ``REMOVE p.property`` — remove a property from matched nodes.
  (``REMOVE p:Label`` is accepted but is a no-op — label membership is
  implicit in entity-table identity and cannot be removed per-row.)
- ``DELETE`` — remove matched entity rows
- ``DETACH DELETE`` — remove matched entities and all relationship rows referencing them
- ``MERGE`` — upsert: match an existing node or create it if absent; idempotent.
  Supports ``ON CREATE SET`` (fires only on creation) and ``ON MATCH SET``
  (fires only when the node already existed); both may appear together.
- ``FOREACH (var IN list | ...)`` — iterate over a list and apply inner SET/CREATE/MERGE clauses

Procedure calls
~~~~~~~~~~~~~~~

- ``CALL procedure(args) YIELD col1, col2`` — invokes a registered procedure and
  introduces the YIELDed columns into the binding frame.

  Built-in ``db.*`` procedures:

  - ``db.labels()`` → ``label`` (one row per registered entity type)
  - ``db.relationshipTypes()`` → ``relationshipType`` (one row per relationship type)
  - ``db.propertyKeys()`` → ``propertyKey`` (one row per unique user-visible property)

Not yet supported
~~~~~~~~~~~~~~~~~

- ``CALL { ... }`` subquery blocks (inline subqueries)
- Full-text index operations (``CALL db.index.fulltext.*``)


Pipeline configuration (nmetl)
-------------------------------

PyCypher ships with ``nmetl``, a command-line ETL tool that executes
Cypher queries defined in a YAML pipeline config:

.. code:: bash

   nmetl run pipeline.yml            # execute all queries
   nmetl query --entity Person=people.csv "MATCH (p:Person) RETURN p.name"
   nmetl validate pipeline.yml       # validate config without running

See ``nmetl --help`` for the full command reference.


Under the hood
--------------

PyCypher uses Lark (Earley) to parse Cypher into a Pydantic-validated AST,
then translates the AST into BindingFrame operations executed entirely in
pandas.  No graph database, no JVM, no external process.


Installation
------------

Mac and Linux
~~~~~~~~~~~~~

You'll need to be able to run ``uv`` in order to use the ``Makefile``.
To install ``uv`` on Linux or Mac:

.. code:: bash

   curl -LsSf https://astral.sh/uv/install.sh | sh

If you don't have ``make`` on your Mac, then you should:

.. code:: bash

   brew install make

And if you don't have ``brew``, then install it with:

.. code:: bash

   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

If you're running Linux without ``make``, then follow the directions for
your distribution. For example, on Ubuntu, you can:

.. code:: bash

   sudo apt install make

Windows
~~~~~~~

On Windows, erase your hard drive, install Linux, and then follow the
directions above.

Setting everything up
---------------------

To set up the virtual environment, install all the dependencies, install
the right version of Python, build the package, install it as an
editable project, run a bunch of unit tests, and build HTML
documentation, do:

.. code:: bash

   make all

To clean everything up, deleting the virtual environment, documentation,
and so on, do:

.. code:: bash

   make clean

You don't *need* to use the ``Makefile``, and therefore you don't *need*
to have ``uv`` installed on your system. But that's what all the cool
kids are using these days.

.. |Install and run tests| image:: https://github.com/zacernst/pycypher/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/zacernst/pycypher/actions/workflows/ci.yml
.. |Build Sphinx documentation| image:: https://github.com/zacernst/pycypher/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/zacernst/pycypher/actions/workflows/ci.yml
