PyCypher
========

PyCypher is a Python library that parses Cypher queries into an Abstract Syntax Tree (AST) consisting of Python objects. This allows for programmatic manipulation and execution of Cypher queries.

Features
--------

* Parse Cypher queries into a navigable AST
* Manipulate query components programmatically
* Execute queries against various backends
* Extensible architecture for custom implementations

Components
----------

The PyCypher package consists of several key components:

* **Core**: The core parser and lexer components
* **ETL**: Components for Extract, Transform, Load operations
* **Shims**: Adapters for different graph backends
* **Util**: Utility functions and helpers

.. toctree::
   :maxdepth: 2
   
   api
   tutorials/index
