Cypher for SQL Users
====================

A translation guide for developers who know SQL but are new to Cypher.
Read this in 10 minutes and you'll understand every PyCypher example.

.. contents:: On this page
   :local:
   :depth: 2


The 60-Second Mental Model
--------------------------

+--------------------+--------------------+--------------------------------------+
| SQL Concept        | Cypher Equivalent  | Key Difference                       |
+====================+====================+======================================+
| ``SELECT``         | ``RETURN``         | Same role — choose output columns    |
+--------------------+--------------------+--------------------------------------+
| ``FROM table``     | ``MATCH (n:Label)``| Nodes replace tables                 |
+--------------------+--------------------+--------------------------------------+
| ``WHERE``          | ``WHERE``          | Same syntax for property filters     |
+--------------------+--------------------+--------------------------------------+
| ``JOIN ... ON``    | ``()-[:REL]->()``  | Relationships replace JOINs          |
+--------------------+--------------------+--------------------------------------+
| ``LEFT JOIN``      | ``OPTIONAL MATCH`` | Preserves rows with no match         |
+--------------------+--------------------+--------------------------------------+
| ``GROUP BY``       | *(implicit)*       | Non-aggregated RETURN columns group  |
+--------------------+--------------------+--------------------------------------+
| ``ORDER BY``       | ``ORDER BY``       | Identical                            |
+--------------------+--------------------+--------------------------------------+
| ``LIMIT``          | ``LIMIT``          | Identical                            |
+--------------------+--------------------+--------------------------------------+
| ``INSERT INTO``    | ``CREATE``         | Creates nodes or relationships       |
+--------------------+--------------------+--------------------------------------+
| ``UPDATE``         | ``SET``            | Sets properties on matched nodes     |
+--------------------+--------------------+--------------------------------------+
| ``DELETE FROM``    | ``DELETE``         | Removes matched nodes                |
+--------------------+--------------------+--------------------------------------+
| subquery           | ``WITH``           | Pipes results between query stages   |
+--------------------+--------------------+--------------------------------------+

The biggest conceptual shift: **in SQL you join tables; in Cypher you
traverse relationships.**


Side-by-Side Examples
---------------------

All examples below use this dataset:

.. code-block:: python

   import pandas as pd
   from pycypher import Star
   from pycypher.ingestion import ContextBuilder

   people = pd.DataFrame({
       "__ID__": [1, 2, 3, 4],
       "name": ["Alice", "Bob", "Carol", "Dave"],
       "age": [30, 25, 35, 28],
       "dept": ["eng", "sales", "eng", "sales"],
   })

   products = pd.DataFrame({
       "__ID__": [10, 20, 30],
       "title": ["Widget", "Gadget", "Gizmo"],
       "price": [9.99, 49.99, 14.99],
   })

   purchases = pd.DataFrame({
       "__ID__": [100, 101, 102, 103],
       "__SOURCE__": [1, 2, 1, 3],
       "__TARGET__": [10, 20, 30, 10],
   })

   context = ContextBuilder.from_dict({
       "Person": people,
       "Product": products,
       "BOUGHT": purchases,
   })
   star = Star(context=context)


SELECT ... FROM ... WHERE
~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT name, age FROM Person WHERE age > 28

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 28 RETURN p.name AS name, p.age AS age"
   )

.. code-block:: text

    name  age
   Alice   30
   Carol   35

Key difference: In Cypher, ``p`` is a *variable* bound to each ``Person``
node.  You access properties with dot notation: ``p.age``, ``p.name``.


JOIN (Inner Join via Relationship)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT p.name, pr.title
   FROM Person p
   JOIN purchases ON purchases.person_id = p.id
   JOIN Product pr ON pr.id = purchases.product_id

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person)-[:BOUGHT]->(pr:Product) "
       "RETURN p.name AS buyer, pr.title AS product"
   )

.. code-block:: text

   buyer product
   Alice  Widget
   Alice   Gizmo
     Bob  Gadget
   Carol  Widget

Key difference: No ``ON`` clause needed — the ``-[:BOUGHT]->`` pattern
encodes the join condition.  The arrow ``->`` indicates direction.


LEFT JOIN (OPTIONAL MATCH)
~~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT p.name, pr.title
   FROM Person p
   LEFT JOIN purchases ON purchases.person_id = p.id
   LEFT JOIN Product pr ON pr.id = purchases.product_id

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) "
       "OPTIONAL MATCH (p)-[:BOUGHT]->(pr:Product) "
       "RETURN p.name AS person, pr.title AS product"
   )

.. code-block:: text

   person product
    Alice  Widget
    Alice   Gizmo
      Bob  Gadget
    Carol  Widget
     Dave    None

Key difference: ``OPTIONAL MATCH`` preserves all ``Person`` rows.
Dave appears with ``None`` because he has no purchases.


GROUP BY with Aggregation
~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT dept, COUNT(*) AS headcount
   FROM Person
   GROUP BY dept
   ORDER BY dept

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) "
       "RETURN p.dept AS department, count(p) AS headcount "
       "ORDER BY department"
   )

.. code-block:: text

   department  headcount
          eng          2
        sales          2

Key difference: No ``GROUP BY`` clause.  Cypher groups implicitly by
all non-aggregated columns in RETURN.  ``count(p)`` counts nodes, not
rows.


Subqueries (WITH Clause)
~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT name FROM (
       SELECT name, age FROM Person WHERE age > 25
   ) sub
   WHERE age < 35

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) "
       "WHERE p.age > 25 "
       "WITH p.name AS name, p.age AS age "
       "WHERE age < 35 "
       "RETURN name"
   )

.. code-block:: text

   name
   Alice
    Dave

Key difference: ``WITH`` pipes results between query stages, like a
subquery.  Each ``WITH`` creates a new scope — only columns listed in
``WITH`` are available downstream.


EXISTS (Subquery Predicate)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   SELECT name FROM Person p
   WHERE EXISTS (
       SELECT 1 FROM purchases WHERE person_id = p.id
   )

**Cypher:**

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) "
       "WHERE EXISTS { MATCH (p)-[:BOUGHT]->() } "
       "RETURN p.name AS name"
   )

.. code-block:: text

   name
   Alice
     Bob
   Carol


COLLECT (No SQL Equivalent)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cypher can aggregate values into lists — something SQL cannot do natively:

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person)-[:BOUGHT]->(pr:Product) "
       "RETURN p.name AS buyer, collect(pr.title) AS products"
   )

.. code-block:: text

   buyer        products
   Alice [Widget, Gizmo]
     Bob        [Gadget]
   Carol        [Widget]


Common SQL Patterns Translated
------------------------------

DISTINCT
~~~~~~~~

**SQL:** ``SELECT DISTINCT dept FROM Person``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person) RETURN DISTINCT p.dept AS dept

COUNT with Filter
~~~~~~~~~~~~~~~~~

**SQL:** ``SELECT COUNT(*) FROM Person WHERE age > 30``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person) WHERE p.age > 30 RETURN count(p) AS n

LIKE (String Matching)
~~~~~~~~~~~~~~~~~~~~~~

**SQL:** ``SELECT name FROM Person WHERE name LIKE 'A%'``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name AS name

Other string predicates: ``ENDS WITH``, ``CONTAINS``.

IN List
~~~~~~~

**SQL:** ``SELECT name FROM Person WHERE dept IN ('eng', 'sales')``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person) WHERE p.dept IN ['eng', 'sales'] RETURN p.name AS name

Note: Cypher uses square brackets ``[ ]`` for lists, not parentheses.

COALESCE / NULL Handling
~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:** ``SELECT COALESCE(nickname, name) FROM Person``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person) RETURN coalesce(p.nickname, p.name) AS display_name

NULL handling: ``IS NULL``, ``IS NOT NULL`` work identically to SQL.

CASE Expressions
~~~~~~~~~~~~~~~~

**SQL:** ``SELECT CASE WHEN age > 30 THEN 'senior' ELSE 'junior' END FROM Person``

**Cypher:**

.. code-block:: cypher

   MATCH (p:Person)
   RETURN CASE WHEN p.age > 30 THEN 'senior' ELSE 'junior' END AS level

INSERT / UPDATE / DELETE
~~~~~~~~~~~~~~~~~~~~~~~~

**SQL:**

.. code-block:: sql

   INSERT INTO Person (name, age) VALUES ('Eve', 29);
   UPDATE Person SET age = 31 WHERE name = 'Alice';
   DELETE FROM Person WHERE name = 'Dave';

**Cypher:**

.. code-block:: cypher

   CREATE (e:Person {name: 'Eve', age: 29})

   MATCH (p:Person {name: 'Alice'}) SET p.age = 31

   MATCH (p:Person {name: 'Dave'}) DELETE p


When to Use Graph Patterns vs SQL Thinking
------------------------------------------

**Graph patterns work better when:**

- You're traversing relationships between entities (social networks,
  supply chains, dependency graphs)
- Join depth is variable or unknown (``MATCH path = (a)-[:KNOWS*1..5]->(b)``)
- You want to express "find all connected entities" naturally

**SQL thinking works fine when:**

- You're filtering a single entity type (``MATCH (p:Person) WHERE ...``)
- You're doing simple aggregation (counts, sums, averages)
- You have a fixed, known join structure

**PyCypher handles both well** — the Cypher syntax is expressive for graph
traversal while remaining readable for simple table-like queries.


Key Syntax Differences Cheat Sheet
-----------------------------------

.. list-table::
   :widths: 35 35 30
   :header-rows: 1

   * - SQL
     - Cypher
     - Note
   * - ``table.column``
     - ``variable.property``
     - Dot notation for both
   * - ``'string'``
     - ``'string'``
     - Same quoting
   * - ``NULL``
     - ``null``
     - Lowercase in Cypher
   * - ``TRUE / FALSE``
     - ``true / false``
     - Lowercase in Cypher
   * - ``(1, 2, 3)``
     - ``[1, 2, 3]``
     - Square brackets for lists
   * - ``AS alias``
     - ``AS alias``
     - Same syntax
   * - ``AND / OR / NOT``
     - ``AND / OR / NOT``
     - Same syntax
   * - ``<> / !=``
     - ``<>``
     - Cypher uses ``<>`` only
   * - ``BETWEEN a AND b``
     - ``>= a AND <= b``
     - No BETWEEN keyword
   * - ``LIKE '%text%'``
     - ``CONTAINS 'text'``
     - Named predicates instead
   * - ``OFFSET n``
     - ``SKIP n``
     - Different keyword
   * - ``COUNT(*)``
     - ``count(*)``
     - Lowercase functions
