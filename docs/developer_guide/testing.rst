Testing
=======

Running and writing tests for NMETL.

Development Environment
-----------------------

Set up your testing environment:

.. code-block:: bash

   # Install in development mode
   uv sync
   
   # Install test dependencies
   uv pip install pytest pytest-cov

Running Tests
-------------

Run All Tests
~~~~~~~~~~~~~

.. code-block:: bash

   uv run pytest

Run Specific Test Files
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Test AST models
   uv run pytest tests/test_ast_models.py
   
   # Test grammar parser
   uv run pytest tests/test_grammar_parser.py
   
   # Test validation
   uv run pytest tests/test_validation.py

Run Tests with Coverage
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   uv run pytest --cov=pycypher --cov=nmetl --cov=fastopendata --cov=shared
   
   # Generate HTML coverage report
   uv run pytest --cov=pycypher --cov-report=html

Run Specific Tests
~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Run by test name pattern
   uv run pytest -k "test_node_pattern"
   
   # Run a specific test
   uv run pytest tests/test_ast_models.py::TestPatterns::test_node_pattern_basic

Writing Tests
-------------

Test Structure
~~~~~~~~~~~~~~

Follow pytest conventions:

.. code-block:: python

   import pytest
   from pycypher.ast_models import Variable, NodePattern
   
   class TestNodePattern:
       """Tests for NodePattern AST node"""
       
       def test_basic_node_pattern(self):
           """Test creating a basic node pattern"""
           var = Variable(name="n")
           node = NodePattern(
               variable=var,
               labels=["Person"],
               properties=None
           )
           
           assert node.variable.name == "n"
           assert node.labels == ["Person"]
           assert node.properties is None
       
       def test_node_pattern_with_properties(self):
           """Test node pattern with properties"""
           # Test implementation
           pass

Using Fixtures
~~~~~~~~~~~~~~

Use fixtures for common test data:

.. code-block:: python

   import pytest
   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter
   
   @pytest.fixture
   def parser():
       """Provide a GrammarParser instance"""
       return GrammarParser()
   
   @pytest.fixture
   def converter():
       """Provide an ASTConverter instance"""
       return ASTConverter()
   
   def test_parse_simple_query(parser, converter):
       query = "MATCH (n) RETURN n"
       raw_ast = parser.parse_to_ast(query)
       typed_ast = converter.convert(raw_ast)
       assert typed_ast is not None

Type Checking
-------------

Run type checks with mypy:

.. code-block:: bash

   # Check all packages
   uv run mypy packages/pycypher/src
   uv run mypy packages/nmetl/src
   uv run mypy packages/fastopendata/src
   uv run mypy packages/shared/src

Test Guidelines
---------------

1. **Test Coverage**: Aim for >90% coverage on new code
2. **Test Naming**: Use descriptive names that explain what is being tested
3. **Assertions**: Include clear assertion messages
4. **Isolation**: Tests should be independent and not rely on execution order
5. **Documentation**: Add docstrings to test classes and complex tests

Continuous Integration
----------------------

Tests run automatically on:

* Pull requests
* Commits to main branch
* Scheduled nightly builds

See `.github/workflows/` for CI configuration.
