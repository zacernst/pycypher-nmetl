User Guide
==========

Comprehensive guides for using PyCypher features.

.. toctree::
   :maxdepth: 2
   :caption: User Guides:

   variables
   ast_nodes
   query_processing
   error_handling
   configuration
   performance_tuning

Overview
--------

The user guide provides in-depth reference material for PyCypher's core
abstractions:

* :doc:`variables` — How the ``Variable`` class represents named references
  throughout the AST and execution engine.
* :doc:`ast_nodes` — Complete catalogue of all AST node types with field
  descriptions and construction examples.
* :doc:`query_processing` — The full query lifecycle: parsing, AST conversion,
  semantic validation, BindingFrame execution, expression evaluation,
  aggregation, mutation, and performance characteristics.
* :doc:`error_handling` — Exception hierarchy, catching patterns, and
  best practices for robust query error handling.
* :doc:`configuration` — Complete reference for all environment variables
  and per-query configuration options.
* :doc:`performance_tuning` — Practical strategies for optimising query
  execution: timeouts, memory budgets, cross-join limits, result caching,
  parse caching, query structure best practices, and a production deployment
  checklist.
