Getting Started
===============

Installation
------------

NMETL uses ``uv`` for package management. Install the project and its dependencies:

.. code-block:: bash

   # Clone the repository
   git clone <repository-url>
   cd pycypher-nmetl

   # Install with uv
   uv sync

   # Or install specific packages
   uv pip install -e packages/pycypher
   uv pip install -e packages/nmetl
   uv pip install -e packages/fastopendata
   uv pip install -e packages/shared

Basic Usage
-----------

Parsing Cypher Queries
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter
   
   parser = GrammarParser()
   converter = ASTConverter()
   
   query = """
   MATCH (person:Person)-[:KNOWS]->(friend)
   WHERE person.age > 30
   RETURN person.name, friend.name
   """
   
   # Parse to raw AST
   raw_ast = parser.parse_to_ast(query)
   
   # Convert to typed AST
   typed_ast = converter.convert(raw_ast)
   
   # Validate
   validation = typed_ast.validate()
   print(f"Valid: {validation.is_valid}")

Working with Variables
~~~~~~~~~~~~~~~~~~~~~~

All variables in the AST are represented using the ``Variable`` class:

.. code-block:: python

   from pycypher.ast_models import Variable, NodePattern, PropertyLookup
   
   # Create a variable
   var = Variable(name="person")
   
   # Use in node pattern
   node = NodePattern(
       variable=var,
       labels=["Person"],
       properties=None
   )
   
   # Use in property lookup
   prop = PropertyLookup(
       expression=var,
       property_name="age"
   )

Type Checking
-------------

The project uses ``mypy`` for type checking. Run type checks with:

.. code-block:: bash

   uv run mypy packages/pycypher/src
   uv run mypy packages/nmetl/src
   uv run mypy packages/fastopendata/src

Running Tests
-------------

Run the test suite with pytest:

.. code-block:: bash

   # Run all tests
   uv run pytest

   # Run specific test file
   uv run pytest tests/test_ast_models.py

   # Run with coverage
   uv run pytest --cov=pycypher --cov=nmetl

Building Documentation
----------------------

Generate HTML documentation with Sphinx:

.. code-block:: bash

   cd docs
   uv run sphinx-build -b html . _build/html

Open ``_build/html/index.html`` in your browser to view the documentation.

Next Steps
----------

* Read the :doc:`tutorials/index` for detailed examples
* Explore the :doc:`api/index` for API reference
* Check out :doc:`user_guide/index` for advanced features
