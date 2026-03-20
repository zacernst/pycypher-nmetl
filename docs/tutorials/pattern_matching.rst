Pattern Matching
================

Advanced techniques for matching graph patterns in Cypher queries.

Learning Objectives
-------------------

* Match relationships between nodes with typed and untyped patterns
* Use label predicates to filter by entity type at query time
* Traverse variable-length paths and find shortest paths
* Use OPTIONAL MATCH for left-join semantics
* Combine multiple MATCH clauses for cross-product queries
* Collect graph neighbours with pattern comprehensions
* Test list conditions with quantifier predicates and EXISTS subqueries

Prerequisites
-------------

This tutorial assumes you have completed :doc:`basic_query_parsing` and
have a ``Star`` instance ready.  All examples below use this setup:

.. code-block:: python

   import pandas as pd
   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   ctx = ContextBuilder.from_dict({
       "Person": pd.DataFrame({
           "__ID__":   ["p1", "p2", "p3", "p4"],
           "name":     ["Alice", "Bob", "Carol", "Dave"],
           "age":      [30, 25, 35, 28],
           "scores":   [[85, 92], [70, 55], [95, 88, 60], [40]],
       }),
       "Product": pd.DataFrame({
           "__ID__":  ["x1", "x2"],
           "title":   ["Widget", "Gadget"],
           "price":   [9.99, 49.99],
       }),
       "KNOWS": pd.DataFrame({
           "__SOURCE__": ["p1", "p2", "p3"],
           "__TARGET__": ["p2", "p3", "p4"],
           "since":      [2020, 2021, 2019],
       }),
       "BOUGHT": pd.DataFrame({
           "__SOURCE__": ["p1", "p3"],
           "__TARGET__": ["x1", "x2"],
       }),
   })
   star = Star(context=ctx)


Relationship Patterns
---------------------

The simplest graph query matches a directed edge between two nodes:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS]->(b:Person) "
       "RETURN a.name AS knower, b.name AS known"
   )
   # Alice→Bob, Bob→Carol, Carol→Dave

The relationship type is specified inside square brackets (``[:KNOWS]``).
You can also bind the relationship to a variable to access its properties:

.. code-block:: python

   result = star.execute_query(
       "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
       "RETURN a.name AS from, b.name AS to, r.since AS since"
   )

Inline property filters work on both nodes and relationships:

.. code-block:: python

   # Only KNOWS edges from 2020 or later
   result = star.execute_query(
       "MATCH (a:Person)-[r:KNOWS {since: 2020}]->(b:Person) "
       "RETURN a.name AS from, b.name AS to"
   )

   # Inline node property filter
   result = star.execute_query(
       "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person) "
       "RETURN b.name AS friend"
   )


Label Predicates in WHERE
--------------------------

When you use an unlabeled node pattern (``MATCH (n)``), PyCypher scans
all registered entity types.  ``WHERE n:Label`` filters to a specific type
at query time:

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

Label predicates compose with AND / OR / NOT like any other WHERE
predicate.  The compound form ``n:Label1:Label2`` applies AND semantics —
the node must have both labels.


Variable-Length Paths
---------------------

Append ``*min..max`` to a relationship pattern to match paths of variable
hop depth.  PyCypher executes these as a BFS traversal:

.. code-block:: python

   # Any number of KNOWS hops (1 or more, capped internally)
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

.. tip::

   Always prefer bounded hop limits (``*1..3``) over unbounded (``*``) on
   large graphs.  Unbounded paths explore every reachable edge, and
   execution time is proportional to the number of reachable edges.

Use ``length()`` on a named path variable to get the hop count:

.. code-block:: python

   result = star.execute_query(
       "MATCH p = (a:Person)-[:KNOWS*1..4]->(b:Person) "
       "RETURN a.name, b.name, length(p) AS hops"
   )


shortestPath and allShortestPaths
----------------------------------

``shortestPath()`` finds the minimum-hop route between two nodes.
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
   # One row: start='Alice', end='Dave', hops depends on graph structure

   # All shortest paths (every path at minimum hop count)
   result = star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'})
       MATCH p = allShortestPaths((a)-[:KNOWS*]->(b))
       RETURN a.name AS start, b.name AS end, length(p) AS hops
       """
   )

If no path exists between the two nodes, the result is empty (not an error).


OPTIONAL MATCH — Left-Join Semantics
--------------------------------------

``OPTIONAL MATCH`` preserves rows from the preceding ``MATCH`` even when
the optional pattern has no match.  Unmatched variables are bound to
``null`` — just like a SQL LEFT JOIN:

.. code-block:: python

   # Everyone is returned; product is null for those who bought nothing
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:BOUGHT]->(item:Product)
       WITH p.name AS person, item.title AS product
       RETURN person, product
       """
   )

   # Find people who have NOT bought anything
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:BOUGHT]->(item:Product)
       WITH p.name AS person, item.title AS product
       WHERE product IS NULL
       RETURN person
       """
   )

   # Null-safe display with coalesce
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:BOUGHT]->(item:Product)
       WITH p.name AS person, coalesce(item.title, 'nothing') AS product
       RETURN person, product
       """
   )


Cross-Product MATCH
--------------------

When two MATCH patterns share no common variables, PyCypher produces the
Cartesian product of both scans (equivalent to SQL CROSS JOIN):

.. code-block:: python

   # All (person, product) pairs
   result = star.execute_query(
       "MATCH (p:Person), (pr:Product) "
       "RETURN p.name AS person, pr.title AS product"
   )

   # Self cross-join: all ordered (A, B) pairs where A != B
   result = star.execute_query(
       "MATCH (p:Person), (q:Person) "
       "WHERE p.name <> q.name "
       "RETURN p.name AS pname, q.name AS qname"
   )


Pattern Comprehensions
-----------------------

Pattern comprehensions collect graph neighbours into a per-row list
without a separate MATCH clause.  The syntax embeds a pattern inside
square brackets:

``[(anchor)-[:REL]->(target) WHERE condition | map_expression]``

.. code-block:: python

   # Names of all people each person knows
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN p.name AS person, "
       "       [(p)-[:KNOWS]->(f) | f.name] AS friends"
   )

   # Only older friends (filter with WHERE inside the comprehension)
   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN p.name AS person, "
       "       [(p)-[:KNOWS]->(f:Person) WHERE f.age > p.age | f.name] AS older_friends"
   )

Pattern comprehensions are evaluated via a single ``pd.merge`` across all
anchor rows — they do not loop per row.


EXISTS Subqueries
------------------

``EXISTS { MATCH ... WHERE ... }`` tests whether an inner pattern produces
at least one result.  The subquery executes once for the entire outer
frame (batched), not once per row:

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


Quantifier Predicates
----------------------

Test whether a condition holds for some, all, none, or exactly one element
of a list:

.. code-block:: python

   # any() — at least one score above 90
   result = star.execute_query(
       "MATCH (p:Person) WHERE any(s IN p.scores WHERE s > 90) "
       "RETURN p.name AS name"
   )

   # all() — every score is non-negative (vacuously true for empty lists)
   result = star.execute_query(
       "MATCH (p:Person) WHERE all(s IN p.scores WHERE s >= 0) "
       "RETURN p.name AS name"
   )

   # none() — no score below 50
   result = star.execute_query(
       "MATCH (p:Person) WHERE none(s IN p.scores WHERE s < 50) "
       "RETURN p.name AS name"
   )

   # single() — exactly one score above 90
   result = star.execute_query(
       "MATCH (p:Person) WHERE single(s IN p.scores WHERE s > 90) "
       "RETURN p.name AS name"
   )


Performance Tips
-----------------

1. **Push WHERE early.** Place selective filters immediately after MATCH
   to reduce the number of rows before joins and projections.

2. **Bound your paths.** Use ``*1..3`` instead of ``*`` to prevent
   unbounded BFS traversals on large graphs.

3. **Use inline property filters.** ``MATCH (p:Person {name: 'Alice'})``
   filters during the scan phase, before any joins occur.

4. **Prefer pattern comprehensions** over separate MATCH + collect() when
   you need a per-row list of neighbours — they express the intent more
   concisely and execute in a single merge pass.


Next Steps
----------

* :doc:`basic_query_parsing` — foundational query execution
* :doc:`../user_guide/query_processing` — execution model deep-dive
* :doc:`../api/pycypher` — full API reference
