Getting Started
===============

Installation
------------

PyCypher uses ``uv`` for package management:

.. code-block:: bash

   # Clone and install
   git clone <repository-url>
   cd pycypher-nmetl
   uv sync

   # Or install from PyPI
   pip install pycypher

Quick Start: Run Your First Query
----------------------------------

The fastest path to results — load a pandas DataFrame and execute Cypher:

.. code-block:: python

   import pandas as pd
   from pycypher import Star, Context, EntityTable, ID_COLUMN

   # 1. Bring your data as a DataFrame
   df = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })

   # 2. Wrap it in an EntityTable
   table = EntityTable.from_dataframe("Person", df)

   # 3. Build a Context and a Star executor
   from pycypher.relational_models import EntityMapping, RelationshipMapping
   context = Context(
       entity_mapping=EntityMapping(mapping={"Person": table}),
       relationship_mapping=RelationshipMapping(mapping={}),
   )
   star = Star(context=context)

   # 4. Execute Cypher — returns a pandas DataFrame
   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name, p.age AS age"
   )
   print(result)
   #     name  age
   # 0  Alice   30
   # 1  Carol   35

Using ContextBuilder (Recommended)
-----------------------------------

:class:`~pycypher.ingestion.ContextBuilder` is the friendliest API for building
a ``Context`` from files or DataFrames:

.. code-block:: python

   import pandas as pd
   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   people = pd.DataFrame({
       "user_id": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })

   context = (
       ContextBuilder()
       .add_entity("Person", people, id_col="user_id")
       .build()
   )

   star = Star(context=context)
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age ASC"
   )

You can also load directly from files:

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity("Person", "data/people.csv")
       .add_entity("Product", "data/products.parquet")
       .add_relationship(
           "BOUGHT",
           "data/purchases.csv",
           source_col="user_id",
           target_col="product_id",
       )
       .build()
   )

One-Line Context Builder
-------------------------

When all your data is already in memory as DataFrames, use
:meth:`~pycypher.ingestion.ContextBuilder.from_dict` to build a context in
a single call:

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion import ContextBuilder
   from pycypher import Star

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })
   products = pd.DataFrame({
       "__ID__": [10, 20],
       "title": ["Widget", "Gadget"],
       "price": [9.99, 49.99],
   })

   # One call — no chaining required
   context = ContextBuilder.from_dict({"Person": people, "Product": products})
   star = Star(context=context)

   result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")

Graph Queries: MATCH with Relationships
-----------------------------------------

.. code-block:: python

   result = star.execute_query(
       """
       MATCH (buyer:Person)-[:BOUGHT]->(item:Product)
       WHERE buyer.age >= 30
       RETURN buyer.name AS buyer, item.name AS product
       ORDER BY buyer.name
       """
   )

WHERE Label Predicates
-----------------------

``WHERE n:Label`` tests whether a node variable belongs to a specific entity
type at query time.  This is useful when MATCH scans multiple entity types with
an unlabeled pattern (``MATCH (n)``) and you want to filter or branch on the
actual type:

.. code-block:: python

   # Filter to only Person nodes from a heterogeneous scan
   result = star.execute_query(
       "MATCH (n) WHERE n:Person RETURN n.name AS name"
   )

   # Negation — exclude a specific type
   result = star.execute_query(
       "MATCH (n) WHERE NOT n:Product RETURN n.name AS name"
   )

   # Combine label predicate with property filter
   result = star.execute_query(
       "MATCH (n) WHERE n:Person AND n.age > 30 RETURN n.name AS name"
   )

   # Redundant but valid — labeled MATCH + WHERE :Label confirms the type
   result = star.execute_query(
       "MATCH (p:Person) WHERE p:Person RETURN p.name AS name"
   )

Label predicates compose naturally with AND / OR / NOT and with all other
WHERE predicates.  The compound form ``n:Label1:Label2`` applies AND semantics
(the node must have both labels simultaneously) — useful when an entity table
carries multiple inherited labels.

Variable-Length Paths
----------------------

Match paths of variable hop depth using ``*min..max`` syntax on relationship
patterns:

.. code-block:: python

   # Any number of KNOWS hops (1 or more)
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name, b.name"
   )

   # Exactly 2–3 hops
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*2..3]->(b:Person) RETURN a.name, b.name"
   )

   # Up to 4 hops — find indirect connections
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*1..4]->(b:Person) RETURN a.name, b.name"
   )

shortestPath and allShortestPaths
----------------------------------

Use ``shortestPath()`` to find the minimum-hop route between two nodes.
``allShortestPaths()`` returns every path tied for the minimum length:

.. code-block:: python

   # Shortest route from Alice to Dave
   result = star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'})
       MATCH p = shortestPath((a)-[:KNOWS*]->(b))
       RETURN a.name AS start, b.name AS end, length(p) AS hops
       """
   )
   # → one row: start='Alice', end='Dave', hops=2

   # All shortest paths (same minimum hop count)
   result = star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'})
       MATCH p = allShortestPaths((a)-[:KNOWS*]->(b))
       RETURN a.name AS start, b.name AS end, length(p) AS hops
       """
   )
   # → one or more rows, all with hops == minimum

If no path exists between the two nodes the result is empty (not an error).

OPTIONAL MATCH — Left-Join Semantics
--------------------------------------

``OPTIONAL MATCH`` preserves rows even when the pattern has no match.
Unmatched variables are bound to ``null``:

.. code-block:: python

   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:BOUGHT]->(item:Product)
       WITH p.name AS person, item.name AS product
       RETURN person, product
       """
   )
   # People who have not bought anything appear with product = null

WITH + UNWIND: List Explosion
------------------------------

``WITH`` pipelines data between stages.  ``UNWIND`` explodes a list
column into one row per element:

.. code-block:: python

   # Collect all names and unwind into individual rows
   result = star.execute_query(
       """
       MATCH (p:Person)
       WITH collect(p.name) AS names
       UNWIND names AS name
       RETURN name
       """
   )

   # Explode a list property
   result = star.execute_query(
       """
       MATCH (p:Person)
       WITH p.name AS name, p.tags AS tags
       UNWIND tags AS tag
       RETURN name, tag
       """
   )

Aggregation
-----------

.. code-block:: python

   # Count, group by department
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS n"
   )

   # Collect values into a list
   result = star.execute_query(
       "MATCH (p:Person) RETURN collect(p.name) AS all_names"
   )

   # Order and paginate
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 5"
   )

   # Null placement: NULLS FIRST puts null-valued rows before non-null rows.
   # Default (no keyword) is NULLS LAST, matching Neo4j 5.x behaviour.
   result = star.execute_query(
       "MATCH (n:Person) RETURN n.name, n.score "
       "ORDER BY n.score ASC NULLS FIRST"
   )

WITH * — Pass-Through Projection
---------------------------------

``WITH *`` forwards all variables from the preceding MATCH into subsequent
clauses without requiring an explicit alias list:

.. code-block:: python

   # Access any matched variable after WITH * — useful for filtering then projecting
   result = star.execute_query(
       "MATCH (p:Person) WITH * WHERE p.age > 28 RETURN p.name AS name, p.age AS age"
   )

   # Sort and cap rows before explicit projection
   result = star.execute_query(
       "MATCH (p:Person) WITH * ORDER BY p.age ASC LIMIT 3 RETURN p.name AS name"
   )

UNION — Combining Query Results
---------------------------------

``UNION`` merges the results of two or more queries, deduplicating rows.
``UNION ALL`` keeps duplicates:

.. code-block:: python

   # Names from two entity types (deduplication applied)
   result = star.execute_query(
       """
       MATCH (p:Person) RETURN p.name AS name
       UNION
       MATCH (c:Company) RETURN c.name AS name
       """
   )

   # Keep all rows including duplicates
   result = star.execute_query(
       """
       MATCH (p:Person) WHERE p.dept = 'Eng' RETURN p.name AS name
       UNION ALL
       MATCH (p:Person) WHERE p.age < 30 RETURN p.name AS name
       """
   )

Mutating the Graph — CREATE, MERGE, DELETE, REMOVE
----------------------------------------------------

PyCypher supports the full Cypher write path.  All mutations are
**atomic**: changes are buffered and only committed when the query
succeeds.  An exception mid-query rolls back all pending changes.

CREATE — Insert New Nodes and Relationships
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a single node
   star.execute_query(
       "CREATE (p:Person {name: 'Dave', age: 28})"
   )

   # Create a relationship between existing nodes
   star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
       CREATE (a)-[:KNOWS]->(b)
       """
   )

MERGE — Upsert Semantics
~~~~~~~~~~~~~~~~~~~~~~~~~

``MERGE`` either matches an existing pattern or creates it.  Use
``ON CREATE SET`` and ``ON MATCH SET`` to apply different updates:

.. code-block:: python

   # Find or create a Person with this name
   star.execute_query(
       "MERGE (p:Person {name: 'Eve'}) SET p.created = true"
   )

   # Track first-seen vs update-time
   star.execute_query(
       """
       MERGE (p:Person {name: 'Alice'})
       ON CREATE SET p.created_at = timestamp()
       ON MATCH  SET p.updated_at = timestamp()
       """
   )

When the entity type referenced in a ``MERGE`` pattern is not registered in
the context, MERGE treats it as *absent* and creates the pattern — this is
expected Cypher semantics for bootstrapping a new entity type.  If a genuine
runtime error occurs during the match phase (e.g. a programming error, not a
missing type), the error propagates as a
:class:`~pycypher.exceptions.GraphTypeNotFoundError` so bugs are never
silently swallowed.

SET — Update Properties
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Set a single property
   star.execute_query(
       "MATCH (p:Person {name: 'Alice'}) SET p.score = 99"
   )

   # Set multiple properties at once
   star.execute_query(
       "MATCH (p:Person) SET p.active = true, p.last_seen = timestamp()"
   )

   # Set relationship properties
   star.execute_query(
       "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.since = 2020"
   )

REMOVE — Delete Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Remove a property from all matching nodes
   star.execute_query(
       "MATCH (p:Person) WHERE p.temp_flag IS NOT NULL REMOVE p.temp_flag"
   )

DELETE — Remove Nodes
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Delete nodes that match the pattern
   star.execute_query(
       "MATCH (p:Person {name: 'Dave'}) DELETE p"
   )

FOREACH — Iterate Over a List
-------------------------------

``FOREACH`` applies a write clause (SET, CREATE, MERGE, DELETE) to each
element of a list.  It is the standard Cypher idiom for batch mutations:

.. code-block:: python

   # Mark every person in a list as active
   star.execute_query(
       """
       MATCH (p:Person)
       WITH collect(p) AS people
       FOREACH (person IN people | SET person.active = true)
       """
   )

   # Create a chain of nodes from a list of values
   star.execute_query(
       """
       FOREACH (name IN ['Alice', 'Bob', 'Carol'] |
           MERGE (p:Person {name: name})
       )
       """
   )

Functional Expressions
-----------------------

PyCypher supports the full suite of Cypher functional expressions for
in-query list manipulation and subquery predicates.  All are evaluated in
a **single vectorised pass** over the frame — no per-row Python overhead.

List Comprehensions
~~~~~~~~~~~~~~~~~~~

``[variable IN list WHERE condition | map_expression]`` — filter and/or
transform a list inline.  The WHERE clause and map expression are optional:

.. code-block:: python

   # Filter — keep only high scores
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN [s IN p.scores WHERE s > 50] AS high_scores"
   )

   # Transform — double every score
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN [s IN p.scores | s * 2] AS doubled"
   )

   # Filter + transform in one step
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN [s IN p.scores WHERE s > 50 | s * 2] AS high_doubled"
   )

Pattern Comprehensions
~~~~~~~~~~~~~~~~~~~~~~

``[(anchor)-[:REL]->(target) WHERE condition | map_expression]`` — collect
matched graph neighbours into a per-row list:

.. code-block:: python

   # Names of all people each person knows
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN p.name AS person, "
       "       [(p)-[:KNOWS]->(f) | f.name] AS friends"
   )

   # Older friends only
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN p.name AS person, "
       "       [(p)-[:KNOWS]->(f:Person) WHERE f.age > p.age | f.name] AS older_friends"
   )

Quantifier Predicates
~~~~~~~~~~~~~~~~~~~~~

Test whether a condition holds for some, all, none, or exactly one element:

.. code-block:: python

   # any() — at least one element satisfies the predicate
   result = star.execute_query(
       "MATCH (p:Person) WHERE any(s IN p.scores WHERE s > 90) "
       "RETURN p.name AS name"
   )

   # all() — every element satisfies the predicate (vacuously true for empty)
   result = star.execute_query(
       "MATCH (p:Person) WHERE all(s IN p.scores WHERE s >= 0) "
       "RETURN p.name AS name"
   )

   # none() — no element satisfies the predicate
   result = star.execute_query(
       "MATCH (p:Person) WHERE none(s IN p.scores WHERE s < 0) "
       "RETURN p.name AS name"
   )

   # single() — exactly one element satisfies the predicate
   result = star.execute_query(
       "MATCH (p:Person) WHERE single(s IN p.scores WHERE s > 95) "
       "RETURN p.name AS name"
   )

EXISTS Subqueries
~~~~~~~~~~~~~~~~~

``EXISTS { MATCH ... WHERE ... }`` tests whether an inner graph pattern
produces at least one result.  The inner query is **batched**: it executes
once for all outer rows, not once per row:

.. code-block:: python

   # People who know at least one other person
   result = star.execute_query(
       "MATCH (p:Person) WHERE EXISTS { MATCH (p)-[:KNOWS]->(q) } "
       "RETURN p.name AS name"
   )

   # NOT EXISTS — people who know nobody
   result = star.execute_query(
       "MATCH (p:Person) WHERE NOT EXISTS { MATCH (p)-[:KNOWS]->(q) } "
       "RETURN p.name AS name"
   )

   # Filter the subquery match with WHERE
   result = star.execute_query(
       "MATCH (p:Person) "
       "WHERE EXISTS { MATCH (p)-[:KNOWS]->(q:Person) WHERE q.age > 30 } "
       "RETURN p.name AS name"
   )

REDUCE
~~~~~~

``reduce(accumulator = initial, variable IN list | step_expression)`` —
fold a list into a scalar value.  The accumulator carries the running
result; ``step_expression`` receives both the accumulator and the current
element:

.. code-block:: python

   # Sum of a list of scores
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN reduce(total = 0, s IN p.scores | total + s) AS score_sum"
   )

   # Maximum element
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN reduce(m = -1, s IN p.scores | CASE WHEN s > m THEN s ELSE m END) AS max_score"
   )

Scalar Functions
----------------

Over 130 scalar functions are available in any expression context.  All
functions are null-safe: passing ``null`` returns ``null`` unless documented
otherwise.

Math and trigonometric functions (``abs``, ``sin``, ``log``, ``pow``, etc.)
are evaluated using numpy C-level array operations — no per-row Python
overhead.  String predicate functions (``startsWith``, ``endsWith``,
``contains``) use the pandas ``.str`` Cython accessor.  These two categories
are safe to use in WHERE clauses on large frames without performance concern.

.. code-block:: python

   # String manipulation
   result = star.execute_query(
       "MATCH (p:Person) RETURN toUpper(p.name) AS upper_name"
   )

   # Null-safe default via coalesce
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN coalesce(p.nickname, p.name) AS display_name"
   )

   # Type predicate — filter rows where a property has the expected type
   result = star.execute_query(
       "MATCH (r:Record) WHERE isString(r.code) AND isInteger(r.count) "
       "RETURN r.code, r.count"
   )

   # Temporal — parse and decompose a duration
   result = star.execute_query(
       "RETURN duration('P1Y6M').months AS months"
       # → 6
   )

   # Rounding with explicit mode (Neo4j 5.x 3-arg form)
   # Modes: HALF_UP (default, away from zero), HALF_DOWN, HALF_EVEN (banker's),
   #        CEILING, FLOOR, UP (away from zero), DOWN (truncation)
   result = star.execute_query(
       "MATCH (p:Person) RETURN round(p.score, 2, 'HALF_EVEN') AS rounded"
   )
   # round(2.5, 0, 'HALF_EVEN') → 2.0 (nearest even)
   # round(2.5, 0, 'HALF_UP')   → 3.0 (away from zero)
   # round(2.5, 0, 'CEILING')   → 3.0 (toward +∞)

   # Bitwise operations — ETL flag/bitmask processing (Neo4j 5.x)
   result = star.execute_query(
       "MATCH (e:Event) WHERE bitAnd(e.flags, 4) > 0 "
       "RETURN e.name AS name, bitOr(e.flags, 1) AS flags_with_bit0"
   )
   # bitAnd / bitOr / bitXor / bitNot / bitShiftLeft / bitShiftRight
   # All accept integer args, return null when any arg is null.

**Function categories** (131 functions registered as of this writing):

.. list-table::
   :widths: 25 75
   :header-rows: 1

   * - Category
     - Functions
   * - String
     - ``toUpper`` / ``upper``, ``toLower`` / ``lower``, ``trim``, ``ltrim``,
       ``rtrim``, ``substring``, ``left``, ``right``, ``size``, ``length`` / ``len``,
       ``replace``, ``split``, ``reverse``, ``isEmpty``
   * - Extended string
     - ``lpad``, ``rpad``, ``repeat``, ``btrim``, ``indexOf``, ``charAt``,
       ``char``, ``charCodeAt``, ``normalize``, ``toStringOrNull``,
       ``startsWith``, ``endsWith``, ``contains``, ``byteSize``
   * - Type conversion
     - ``toString`` / ``str``, ``toInteger`` / ``int``, ``toFloat`` / ``float``, ``toBoolean`` / ``bool``,
       ``toBooleanOrNull``, ``toIntegerOrNull``, ``toFloatOrNull``
   * - List type conversion
     - ``toStringList``, ``toIntegerList``, ``toFloatList``, ``toBooleanList``
   * - Math
     - ``abs``, ``ceil``, ``floor``,
       ``round`` *(1-arg, 2-arg, or 3-arg with mode)*,
       ``sign``, ``sqrt``, ``cbrt``,
       ``log``, ``exp``, ``log10``, ``log2``, ``pow``, ``hypot``, ``fmod``,
       ``gcd``, ``lcm``
   * - Bitwise
     - ``bitAnd``, ``bitOr``, ``bitXor``, ``bitNot``,
       ``bitShiftLeft``, ``bitShiftRight``
   * - Trigonometric
     - ``sin``, ``cos``, ``tan``, ``asin``, ``acos``, ``atan``, ``atan2``,
       ``cot``, ``sinh``, ``cosh``, ``tanh``, ``haversin``, ``degrees``, ``radians``
   * - Constants & random
     - ``pi``, ``e``, ``rand``, ``infinity``, ``isNaN``, ``isFinite``,
       ``isInfinite``
   * - List
     - ``head``, ``last``, ``tail``, ``range``, ``sort``, ``flatten``,
       ``toList``, ``min``, ``max``
   * - Map
     - ``keys``, ``values``, ``properties``
   * - Temporal (parse)
     - ``date``, ``datetime``, ``localdatetime``, ``duration``
   * - Temporal (truncate)
     - ``date.truncate``, ``datetime.truncate``, ``localdatetime.truncate``
   * - Temporal (now)
     - ``timestamp`` / ``now``, ``localtime``, ``localdate``
   * - Type predicates
     - ``isString``, ``isInteger``, ``isFloat``, ``isBoolean``, ``isList``,
       ``isMap``
   * - Type introspection
     - ``valueType``
   * - Graph introspection
     - ``id``, ``elementId``, ``labels``, ``type``, ``keys``, ``exists``
   * - Hash & encoding
     - ``md5``, ``sha1``, ``sha256``, ``encodeBase64``, ``decodeBase64``
   * - Temporal arithmetic
     - ``date + duration``, ``date - duration``, ``date - date``,
       ``datetime + duration``, ``datetime - duration``,
       ``datetime - datetime``, ``duration + duration``,
       ``duration - duration``
   * - Utility
     - ``coalesce``, ``nullIf``, ``randomUUID``

Discover all registered functions at runtime with
:meth:`~pycypher.star.Star.available_functions`:

.. code-block:: python

   print(star.available_functions())
   # ['abs', 'acos', 'asin', ..., 'valuetype', 'values']

Auto-generated Column Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a ``RETURN`` item has no explicit ``AS`` alias, pycypher generates a
display name from the expression — matching Neo4j's behaviour:

.. list-table::
   :widths: 50 50
   :header-rows: 1

   * - RETURN expression
     - Result column name
   * - ``RETURN p.name``
     - ``name``  *(property name, unqualified)*
   * - ``RETURN p``
     - ``p``  *(variable name)*
   * - ``RETURN toUpper(p.name)``
     - ``toUpper(name)``
   * - ``RETURN abs(p.age)``
     - ``abs(age)``
   * - ``RETURN p.age + 1``
     - ``age + 1``
   * - ``RETURN 42``
     - ``42``
   * - ``RETURN 'hello'``
     - ``hello``
   * - ``RETURN true``
     - ``true``
   * - ``RETURN null``
     - ``null``
   * - ``RETURN p.age > 25``
     - ``age > 25``  *(comparison)*
   * - ``RETURN p.name STARTS WITH 'A'``
     - ``name STARTS WITH A``
   * - ``RETURN p.name IS NULL``
     - ``name IS NULL``
   * - ``RETURN NOT p.active``
     - ``NOT active``
   * - ``RETURN p.age > 18 AND p.active``
     - ``age > 18 AND active``
   * - ``RETURN -p.age``
     - ``-age``  *(unary negation)*
   * - ``RETURN $limit``
     - ``$limit``  *(query parameter)*
   * - ``RETURN count(*)``
     - ``count(*)``
   * - ``RETURN xs[0]``
     - ``xs[0]``  *(index lookup)*
   * - ``RETURN xs[1..3]``
     - ``xs[1..3]``  *(slicing)*
   * - ``RETURN [x IN xs \| x * 2]``
     - ``[x IN xs \| x * 2]``  *(list comprehension)*
   * - ``RETURN CASE WHEN … END``
     - ``case``

Using ``AS`` always overrides the auto-generated name:

.. code-block:: python

   # Two aliasless columns — each gets its own display name
   result = star.execute_query(
       "MATCH (p:Person) RETURN toUpper(p.name), p.age + 1"
   )
   print(result.columns.tolist())
   # ['toUpper(name)', 'age + 1']

Running Tests
-------------

.. code-block:: bash

   uv run pytest             # full suite
   uv run pytest -n 4        # parallel (faster)
   uv run pytest -k Person   # filter by keyword

Next Steps
----------

* :doc:`user_guide/query_processing` — deep-dive on the execution model
* :doc:`api/pycypher` — full API reference
* :doc:`tutorials/index` — step-by-step tutorials
