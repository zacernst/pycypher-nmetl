PyCypher
========

A Python Cypher query parser and execution engine built on relational algebra.

PyCypher parses `Cypher <https://neo4j.com/docs/cypher-manual/>`_ graph queries
and executes them against in-memory DataFrames, providing a complete pipeline
from query string to result set.

Getting Started
---------------

.. code-block:: python

   import pandas as pd
   from pycypher import ContextBuilder, Star

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "age": [30, 25, 35],
   })

   context = ContextBuilder.from_dict({"Person": people})
   star = Star(context=context)

   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age >= 30 "
       "RETURN p.name AS name, p.age AS age ORDER BY p.age"
   )

Core API
--------

.. automodule:: pycypher
   :members: Star, ContextBuilder, Context, SemanticValidator, validate_query
   :undoc-members:

Ingestion Layer
---------------

.. automodule:: pycypher.ingestion
   :members:
   :undoc-members:

Configuration
-------------

.. automodule:: pycypher.config
   :members: apply_preset, show_config
   :undoc-members:

Exception Hierarchy
-------------------

All exceptions are importable from ``pycypher`` directly. See
:mod:`pycypher` module docstring for the full categorised hierarchy.

.. automodule:: pycypher.exceptions
   :members:
   :undoc-members:
   :show-inheritance:
