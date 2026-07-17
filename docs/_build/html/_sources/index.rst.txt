PyCypher Documentation
======================

PyCypher is a Cypher query engine that executes openCypher queries against
ordinary pandas DataFrames.  It is built on a Lark-based parser, a
Pydantic AST, and a BindingFrame execution layer — no graph database required.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   hello_world
   getting_started
   tutorials/index
   api/index
   user_guide/index
   deployment/index
   developer_guide/index
   adr/index

Overview
--------

The project ships two packages:

* **PyCypher** — openCypher query parser, AST models, and BindingFrame
  execution engine
* **Shared** — Common utilities, logging, and telemetry

PyCypher also includes **nmetl**, a command-line ETL tool that executes
Cypher queries defined in a YAML pipeline config.

Key Features
------------

* Complete openCypher grammar support (Earley parser via Lark)
* Strongly-typed AST models with Pydantic
* Vectorised query execution against pandas DataFrames
* 130+ built-in scalar functions
* Mutation support (CREATE, SET, DELETE, MERGE) with atomic rollback
* Query parameter injection for safe value binding

Quick Start
-----------

.. code-block:: python

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

   # Execute Cypher — returns a pandas DataFrame
   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 25 RETURN p.name AS name ORDER BY p.age"
   )
   print(result)

See :doc:`getting_started` for the full walkthrough including relationships,
aggregation, mutation, and the ``ContextBuilder`` API.

Examples
--------

The ``examples/`` directory contains runnable scripts that demonstrate
PyCypher features end-to-end:

* **``ast_conversion_example.py``** — Parse a Cypher query, convert to typed
  AST, traverse and pretty-print the tree.
* **``advanced_grammar_examples.py``** — Exercise the full breadth of the
  openCypher grammar (OPTIONAL MATCH, UNWIND, CASE, list comprehensions,
  variable-length paths, etc.).
* **``functions_in_where.py``** — Use scalar functions (``toUpper``,
  ``toLower``, ``trim``, ``abs``, ``toInteger``) inside WHERE predicates
  against a sample dataset.
* **``scalar_functions_in_with.py``** — Demonstrate scalar function usage
  in WITH clause projections.

Run any example with:

.. code-block:: bash

   uv run python examples/ast_conversion_example.py

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
