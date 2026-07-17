Graph Modeling
==============

Learn how to design your tabular data as a graph for PyCypher — choosing
entity types, modeling relationships, handling properties, and avoiding
common pitfalls.

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Completed :doc:`basic_query_parsing`
* Familiarity with pandas DataFrames

Why Graph Modeling Matters
--------------------------

PyCypher executes Cypher queries against DataFrames, not a graph database.
This means **you** decide how to carve your tabular data into entities and
relationships.  Good modeling choices make queries natural and fast; poor
choices force awkward workarounds and slow cross-joins.

The Core Abstraction
--------------------

PyCypher has two primitives:

**Entities** — things with an identity and properties (nodes in graph terms):

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion import ContextBuilder

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })

**Relationships** — directed connections between entities:

.. code-block:: python

   knows = pd.DataFrame({
       "__SOURCE__": [1, 2],
       "__TARGET__": [2, 3],
       "since": [2020, 2021],
   })

The ``__ID__``, ``__SOURCE__``, and ``__TARGET__`` columns are the
structural glue.  Everything else is a property.

Designing Entities
------------------

One Table per Concept
~~~~~~~~~~~~~~~~~~~~~

Each real-world concept becomes a separate entity type.  Don't merge
unrelated data into a single table:

.. code-block:: python

   # GOOD: separate entity types
   context = (
       ContextBuilder()
       .add_entity("Person", people_df)
       .add_entity("Company", companies_df)
       .add_entity("Product", products_df)
       .build()
   )

   # BAD: one mega-table with a "type" column
   # This forces every query to filter by type manually

Choosing IDs
~~~~~~~~~~~~

The ``__ID__`` column uniquely identifies each row within its entity type.
IDs can be integers or strings — choose whatever your source data provides:

.. code-block:: python

   # Integer IDs (from a database primary key)
   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
   })

   # String IDs (from a natural key or UUID)
   people = pd.DataFrame({
       "__ID__": ["alice-01", "bob-02", "carol-03"],
       "name": ["Alice", "Bob", "Carol"],
   })

   # Using an existing column as the ID
   raw = pd.DataFrame({"user_id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
   context = ContextBuilder().add_entity("Person", raw, id_col="user_id").build()

.. warning::

   IDs must be unique within each entity type.  Duplicate IDs produce
   unexpected cross-join results during MATCH.

Properties
~~~~~~~~~~

Every non-ID column becomes a property accessible via ``n.property_name``
in Cypher:

.. code-block:: python

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
       "active": [True, True, False],
       "scores": [[85, 92], [70], [95, 88, 60]],  # list properties work too
   })

Property types map directly from pandas dtypes: ``int64``, ``float64``,
``object`` (strings), ``bool``, and Python lists.

Designing Relationships
-----------------------

Relationship Direction
~~~~~~~~~~~~~~~~~~~~~~

Relationships in PyCypher are **directed**: ``__SOURCE__`` → ``__TARGET__``.
Model the direction that reflects the real-world semantics:

.. code-block:: python

   # Person -[:KNOWS]-> Person (social link)
   knows = pd.DataFrame({
       "__SOURCE__": [1, 2],   # who initiates
       "__TARGET__": [2, 3],   # who is known
   })

   # Person -[:WORKS_AT]-> Company (employment)
   works_at = pd.DataFrame({
       "__SOURCE__": [1, 2],   # employee
       "__TARGET__": [10, 20], # company
   })

In queries, ``(a)-[:KNOWS]->(b)`` follows the direction.  Use
``(a)<-[:KNOWS]-(b)`` or ``(a)-[:KNOWS]-(b)`` for reverse or undirected.

Relationship Properties
~~~~~~~~~~~~~~~~~~~~~~~

Add columns beyond ``__SOURCE__`` and ``__TARGET__`` for relationship
properties:

.. code-block:: python

   works_at = pd.DataFrame({
       "__SOURCE__": [1, 2, 3],
       "__TARGET__": [10, 10, 20],
       "role": ["Engineer", "Manager", "Analyst"],
       "start_year": [2018, 2015, 2020],
   })

Query them by binding the relationship to a variable:

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) "
       "RETURN p.name, r.role, c.name"
   )

Multiple Relationship Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Register each relationship type separately:

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity("Person", people)
       .add_entity("Company", companies)
       .add_entity("Product", products)
       .add_relationship("KNOWS", knows_df,
                         source_col="person_a", target_col="person_b")
       .add_relationship("WORKS_AT", works_at_df,
                         source_col="employee_id", target_col="company_id")
       .add_relationship("BOUGHT", purchases_df,
                         source_col="buyer_id", target_col="product_id")
       .build()
   )

Common Modeling Patterns
------------------------

Hub-and-Spoke (Star Schema)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A central entity connected to many satellite entities.  Natural for
customer-centric or product-centric analytics:

::

    Person ──WORKS_AT──> Company
      │
      ├──BOUGHT──> Product
      │
      └──LIVES_IN──> City

.. code-block:: python

   # "Who bought products from their own city?"
   result = star.execute_query(
       """
       MATCH (p:Person)-[:BOUGHT]->(prod:Product),
             (p)-[:LIVES_IN]->(c:City),
             (prod)-[:SOLD_IN]->(c)
       RETURN p.name, prod.title, c.name
       """
   )

Chain (Sequential Events)
~~~~~~~~~~~~~~~~~~~~~~~~~

Events linked in sequence — log analysis, process workflows:

::

    Event1 ──NEXT──> Event2 ──NEXT──> Event3

.. code-block:: python

   events = pd.DataFrame({
       "__ID__": ["e1", "e2", "e3", "e4"],
       "action": ["login", "view_page", "add_cart", "checkout"],
       "timestamp": ["2024-01-01 10:00", "2024-01-01 10:05",
                      "2024-01-01 10:12", "2024-01-01 10:15"],
   })
   next_event = pd.DataFrame({
       "__SOURCE__": ["e1", "e2", "e3"],
       "__TARGET__": ["e2", "e3", "e4"],
   })

   # Find 3-step funnels
   result = star.execute_query(
       """
       MATCH (a:Event)-[:NEXT]->(b:Event)-[:NEXT]->(c:Event)
       RETURN a.action, b.action, c.action
       """
   )

Bipartite Graph
~~~~~~~~~~~~~~~

Two entity types connected only through relationships (no intra-type edges).
Common for recommendation systems:

::

    User ──RATED──> Movie
    User ──RATED──> Movie

.. code-block:: python

   # Users who rated the same movie as Alice
   result = star.execute_query(
       """
       MATCH (alice:User {name: 'Alice'})-[:RATED]->(m:Movie)<-[:RATED]-(other:User)
       WHERE other.name <> 'Alice'
       RETURN DISTINCT other.name AS similar_user, m.title AS shared_movie
       """
   )

Try It Yourself
---------------

**Exercise 1**: Model a university dataset with Students, Courses, and
Professors.  Students enroll in courses (ENROLLED_IN) and professors
teach courses (TEACHES).

.. code-block:: python

   # Your DataFrames here:
   students = pd.DataFrame({"__ID__": [...], "name": [...], "year": [...]})
   courses = pd.DataFrame({"__ID__": [...], "title": [...], "credits": [...]})
   professors = pd.DataFrame({"__ID__": [...], "name": [...], "dept": [...]})
   enrolled_in = pd.DataFrame({"__SOURCE__": [...], "__TARGET__": [...]})
   teaches = pd.DataFrame({"__SOURCE__": [...], "__TARGET__": [...]})

   # Query: "Which professors teach courses that Alice is enrolled in?"

.. toggle::

   .. code-block:: python

      result = star.execute_query(
          """
          MATCH (s:Student {name: 'Alice'})-[:ENROLLED_IN]->(c:Course)<-[:TEACHES]-(p:Professor)
          RETURN p.name AS professor, c.title AS course
          """
      )

**Exercise 2**: You have a CSV of email messages with ``sender``,
``recipient``, ``subject``, and ``timestamp`` columns.  How would you
model this as entities and relationships?

Common Pitfalls
---------------

1. **Forgetting ``__ID__``** — Without a unique ID column, entity rows
   cannot be joined to relationships.  Use ``id_col`` in ContextBuilder
   to rename an existing column.

2. **Mismatched ID types** — If ``__SOURCE__`` contains strings but
   ``__ID__`` contains integers, relationships won't match.  Ensure
   consistent types.

3. **Denormalized relationships** — Putting relationship data as columns
   on an entity table (e.g. ``person.company_name``) prevents graph
   traversal.  Extract it into a separate relationship table.

4. **Undirected data forced into one direction** — For symmetric
   relationships (e.g. "is friends with"), either store both directions
   or use undirected match syntax ``(a)-[:FRIENDS]-(b)``.

Next Steps
----------

* :doc:`basic_query_parsing` — execute queries against your modeled data
* :doc:`pattern_matching` — advanced traversal techniques
* :doc:`data_etl_pipeline` — load data from files and databases
