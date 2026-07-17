Zero to Hello World
===================

Get a working Cypher query in under 5 minutes.  No external files, no
databases — just Python and PyCypher.

.. contents:: On this page
   :local:
   :depth: 1

Prerequisites
-------------

- Python 3.14+
- PyCypher installed (see :doc:`getting_started` for installation)

Level 1: Your First Query
--------------------------

Three lines of setup, one query, instant results:

.. code-block:: python

   import pandas as pd
   from pycypher import ContextBuilder, Star

   # Create in-memory data
   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })

   # Build context and execute
   context = ContextBuilder.from_dict({"Person": people})
   star = Star(context=context)

   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name AS name, p.age AS age"
   )
   print(result)

**Output:**

.. code-block:: text

     name  age
    Alice   30
      Bob   25
    Carol   35

**What just happened:**

1. ``ContextBuilder.from_dict()`` wrapped your DataFrame as a ``Person``
   entity table.
2. ``Star`` is the query engine — call ``execute_query()`` with any Cypher
   string.
3. The result is a standard ``pandas.DataFrame``.

The ``__ID__`` column is required — it uniquely identifies each row (like a
primary key).

Level 2: Filtering with WHERE
-------------------------------

Add a ``WHERE`` clause to filter results:

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age >= 30 RETURN p.name AS name, p.age AS age"
   )

**Output:**

.. code-block:: text

     name  age
    Alice   30
    Carol   35

Any comparison operator works: ``=``, ``<>``, ``<``, ``>``, ``<=``, ``>=``.
Combine with ``AND``, ``OR``, ``NOT``.

Level 3: Multiple Entity Types
-------------------------------

Add more DataFrames to query across different entity types:

.. code-block:: python

   products = pd.DataFrame({
       "__ID__": [10, 20, 30],
       "title": ["Widget", "Gadget", "Gizmo"],
       "price": [9.99, 49.99, 24.99],
   })

   context = ContextBuilder.from_dict({
       "Person": people,
       "Product": products,
   })
   star = Star(context=context)

   result = star.execute_query(
       "MATCH (p:Product) WHERE p.price < 30 "
       "RETURN p.title AS product, p.price AS price ORDER BY p.price ASC"
   )

**Output:**

.. code-block:: text

    product  price
     Widget   9.99
      Gizmo  24.99

Level 4: Relationships
-----------------------

Connect entities with relationships.  A relationship DataFrame needs three
special columns: ``__ID__``, ``__SOURCE__`` (from-entity ID), and
``__TARGET__`` (to-entity ID):

.. code-block:: python

   purchases = pd.DataFrame({
       "__ID__": [100, 101, 102],
       "__SOURCE__": [1, 2, 1],     # Person IDs
       "__TARGET__": [10, 20, 30],   # Product IDs
       "date": ["2024-01", "2024-02", "2024-03"],
   })

   context = (
       ContextBuilder()
       .add_entity("Person", people)
       .add_entity("Product", products)
       .add_relationship(
           "BOUGHT", purchases,
           source_col="__SOURCE__", target_col="__TARGET__",
       )
       .build()
   )
   star = Star(context=context)

   result = star.execute_query(
       "MATCH (person:Person)-[:BOUGHT]->(item:Product) "
       "RETURN person.name AS buyer, item.title AS product "
       "ORDER BY buyer, product"
   )

**Output:**

.. code-block:: text

    buyer product
    Alice   Gizmo
    Alice  Widget
      Bob  Gadget

The ``-[:BOUGHT]->`` arrow in the MATCH pattern follows the relationship
from source to target.

Level 5: Aggregation
---------------------

Use ``count()``, ``collect()``, ``sum()``, ``avg()``, and other aggregation
functions:

.. code-block:: python

   result = star.execute_query(
       "MATCH (person:Person)-[:BOUGHT]->(item:Product) "
       "RETURN person.name AS buyer, count(item) AS items_bought, "
       "       collect(item.title) AS products "
       "ORDER BY items_bought DESC"
   )

**Output:**

.. code-block:: text

    buyer  items_bought        products
    Alice             2  [Widget, Gizmo]
      Bob             1        [Gadget]

Runnable Example
-----------------

All the code above is available as a single runnable file:

.. code-block:: bash

   uv run python examples/hello_world.py

Next Steps
----------

- :doc:`getting_started` — Full getting started guide with all Cypher features
- ``examples/quickstart.py`` — Extended example with error handling
- ``examples/social_network/run_demo.py`` — Graph traversal patterns
