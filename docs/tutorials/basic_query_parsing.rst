Basic Query Execution
=====================

This tutorial shows how to run Cypher queries against your data using PyCypher.

Learning Objectives
-------------------

* Load data and build a query context
* Execute MATCH / WHERE / RETURN queries
* Filter, sort, and aggregate results
* Inspect the parsed AST when needed

Setup
-----

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion import ContextBuilder
   from pycypher import Star

   # Load data from a CSV (or pass a DataFrame directly)
   context = (
       ContextBuilder()
       .add_entity("Person", "data/people.csv")
       .build()
   )
   star = Star(context=context)

If your data is already in memory as a DataFrame, the fastest path is
:meth:`~pycypher.ingestion.ContextBuilder.from_dict`:

.. code-block:: python

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name":   ["Alice", "Bob", "Carol"],
       "age":    [30, 25, 35],
   })
   context = ContextBuilder.from_dict({"Person": people})
   star = Star(context=context)

Running Queries
---------------

Simple MATCH
~~~~~~~~~~~~

.. code-block:: python

   # Returns a pandas DataFrame
   result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")
   print(result)
   #     name
   # 0  Alice
   # 1    Bob
   # 2  Carol

Filtering with WHERE
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name, p.age AS age"
   )
   print(result)
   #     name  age
   # 0  Alice   30
   # 1  Carol   35

Sorting and Limiting
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Top 2 oldest people
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name AS name ORDER BY p.age DESC LIMIT 2"
   )

Aggregation
~~~~~~~~~~~

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) RETURN count(p) AS total, avg(p.age) AS avg_age"
   )

Relationship Patterns
---------------------

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity("Person", "data/people.csv")
       .add_entity("Product", "data/products.csv")
       .add_relationship("BOUGHT", "data/purchases.csv",
                         source_col="user_id", target_col="product_id")
       .build()
   )
   star = Star(context=context)

   result = star.execute_query(
       """
       MATCH (buyer:Person)-[:BOUGHT]->(item:Product)
       WHERE buyer.age >= 30
       RETURN buyer.name AS buyer, item.name AS product
       ORDER BY buyer.name
       """
   )

Discovering Available Functions
--------------------------------

Use :meth:`~pycypher.star.Star.available_functions` to see all registered
scalar functions at any time:

.. code-block:: python

   print(star.available_functions())
   # ['abs', 'acos', 'asin', ..., 'toupper', 'trim']

   # Use them in any expression context
   result = star.execute_query(
       "MATCH (p:Person) RETURN toUpper(p.name) AS upper_name"
   )

Advanced: Inspecting the Parsed AST
-------------------------------------

For users who need to programmatically inspect or introspect parsed query
structure, :meth:`~pycypher.ast_models.ASTConverter.from_cypher` returns a
fully-typed Pydantic AST:

.. code-block:: python

   from pycypher.ast_models import ASTConverter, Match, Return

   query = "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
   ast = ASTConverter.from_cypher(query)

   # ast is a Query object; clauses is the flat list of parsed clauses
   for clause in ast.clauses:
       print(type(clause).__name__, clause)

   # Check the first clause is a MATCH
   match_clause = ast.clauses[0]
   assert isinstance(match_clause, Match)
   print("WHERE predicate:", match_clause.where)

   # Check the last clause is a RETURN
   return_clause = ast.clauses[-1]
   assert isinstance(return_clause, Return)
   print("RETURN items:", return_clause.items)

Error Handling
--------------

.. code-block:: python

   try:
       result = star.execute_query("MATCH (p:Person) RETURN p.salary AS s")
   except KeyError as e:
       # Property 'salary' not found on entity type 'Person'
       print(f"Property error: {e}")

   try:
       result = star.execute_query("MATCH (x:Unknown) RETURN x.name")
   except ValueError as e:
       # Entity type 'Unknown' is not registered
       print(f"Entity error: {e}")

Next Steps
----------

* :doc:`../user_guide/query_processing` — execution model deep-dive
* :doc:`pattern_matching` — advanced graph patterns
* :doc:`../api/pycypher` — full API reference
