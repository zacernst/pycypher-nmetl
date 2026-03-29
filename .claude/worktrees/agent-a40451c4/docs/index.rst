NMETL Documentation
===================

NMETL (Network-based Multi-Entity Transform Layer) is a comprehensive data processing framework that combines graph database capabilities with ETL (Extract, Transform, Load) functionality.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   getting_started
   tutorials/index
   api/index
   user_guide/index
   developer_guide/index

Overview
--------

NMETL provides:

* **PyCypher**: openCypher query parser and AST processing
* **NMETL**: ETL pipeline for network-based data transformation
* **FastOpenData**: Fast data loading and processing utilities
* **Shared**: Common utilities and base classes

Key Features
------------

* Complete openCypher grammar support
* Strongly-typed AST models with Pydantic
* Query validation and optimization
* SAT solver integration
* Multiple backend support (FoundationDB, RocksDB, etc.)
* Comprehensive data lineage tracking

Quick Start
-----------

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter

   # Parse a Cypher query
   parser = GrammarParser()
   query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
   
   # Convert to typed AST
   raw_ast = parser.parse_to_ast(query)
   converter = ASTConverter()
   typed_ast = converter.convert(raw_ast)
   
   # Validate the query
   result = typed_ast.validate()
   if result.is_valid:
       print("Query is valid!")

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
