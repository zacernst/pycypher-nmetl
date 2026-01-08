API Reference
=============

This section contains the complete API reference for all NMETL packages.

.. toctree::
   :maxdepth: 2
   :caption: Package APIs:

   pycypher
   nmetl
   fastopendata
   shared

Package Overview
----------------

PyCypher
~~~~~~~~

The PyCypher package provides openCypher query parsing and AST processing capabilities.

Key modules:

* ``ast_models``: Pydantic-based AST node definitions
* ``grammar_parser``: Lark-based parser for openCypher grammar
* ``validation``: Query validation and type checking
* ``solver``: SAT solver integration for query optimization

NMETL
~~~~~

The NMETL package provides ETL functionality for network-based data transformation.

FastOpenData
~~~~~~~~~~~~

FastOpenData provides fast data loading and processing utilities with support for various data formats.

Shared
~~~~~~

The Shared package contains common utilities and base classes used across all packages.
