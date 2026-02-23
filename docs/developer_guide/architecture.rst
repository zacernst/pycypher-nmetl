Architecture
============

Overview of NMETL architecture and design principles.

Project Structure
-----------------

NMETL is a **monorepo workspace** containing four interdependent packages:

.. code-block:: text

   pycypher-nmetl/
   ├── packages/
   │   ├── shared/        # Common utilities and base classes
   │   ├── pycypher/      # openCypher parser and AST
   │   ├── nmetl/         # ETL pipeline
   │   └── fastopendata/  # Fast data loading utilities
   ├── tests/             # Test suite
   ├── docs/              # Documentation
   └── pyproject.toml     # Workspace configuration

Dependency Order
~~~~~~~~~~~~~~~~

Packages depend on each other in this order::

   shared → pycypher → nmetl
          ↘ fastopendata

- **shared** has no dependencies on other packages
- **pycypher** and **fastopendata** depend on **shared**
- **nmetl** depends on **pycypher** and **shared**

PyCypher Architecture
---------------------

Core Components
~~~~~~~~~~~~~~~

**Grammar Parser**

Uses Lark parser with LALR algorithm:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   
   # Lark-based parser with custom visitor pattern
   parser = GrammarParser()
   raw_ast = parser.parse_to_ast(query)

**AST Models**

Pydantic-based strongly-typed AST nodes:

.. code-block:: python

   from pycypher.ast_models import (
       Query, Match, Return,
       NodePattern, Variable,
       ASTConverter
   )
   
   # All nodes are Pydantic models
   # - Automatic validation
   # - Type safety
   # - Serialization support

**Relational Algebra**

Type-based column namespacing prevents collisions:

.. code-block:: python

   from pycypher.relational_models import (
       EntityTable,      # For node types
       RelationshipTable, # For relationship types
       FilterRows,       # Semi-join filter
       Join,             # ID-only join
       Projection        # Attribute fetching
   )

Design Patterns
---------------

Visitor Pattern
~~~~~~~~~~~~~~~

AST traversal uses visitor pattern:

.. code-block:: python

   class MyVisitor:
       def visit_Match(self, node):
           # Process Match nodes
           pass
       
       def visit_Return(self, node):
           # Process Return nodes
           pass

**Used in:**
- AST conversion (raw → typed)
- Validation
- Optimization
- Translation to relational algebra

Type-Based Namespacing
~~~~~~~~~~~~~~~~~~~~~~

All relational operators use type prefixes:

.. code-block:: python

   # EntityTable columns
   # Person____ID__   (double underscore for special columns)
   # Person__name     (single underscore for attributes)
   # Person__age
   
   # RelationshipTable columns
   # KNOWS____SOURCE__
   # KNOWS____TARGET__
   # KNOWS____ID__

**Benefits:**
- No column name collisions in  joins
- Deterministic naming
- Clear type ownership
- Efficient merging

ID-Only Preservation
~~~~~~~~~~~~~~~~~~~~

FilterRows and Join operators preserve only ID columns:

.. code-block:: python

   # FilterRows returns semi-join result
   filtered = FilterRows(source=table, condition=...)
   # Result has only: Person____ID__
   
   # Join merges on ID columns
   joined = Join(left=person_table, right=knows_table)
   # Result has only: Person____ID__, KNOWS____ID__
   
   # Projection fetches attributes on-demand
   projected = Projection(source=joined, columns=["Person__name"])
   # Result has: Person____ID__, Person__name

**Rationale:**
- Separates ID tracking from attribute access
- Improves join performance
- Reduces memory footprint
- Enables lazy loading

Extension Points
----------------

Custom Query Optimizers
~~~~~~~~~~~~~~~~~~~~~~~

Extend the QueryOptimizer class:

.. code-block:: python

   from pycypher.query_optimizer import QueryOptimizer
   from pycypher.ast_models import Query
   
   class MyOptimizer(QueryOptimizer):
       def optimize(self, query: Query) -> Query:
           # Apply custom optimizations
           return optimized_query

Testing Architecture
--------------------

Test Organization
~~~~~~~~~~~~~~~~~

Tests follow package structure:

.. code-block:: text

   tests/
   ├── test_ast_models.py              # AST node tests
   ├── test_ast_models_coverage_gaps.py # Coverage improvements
   ├── test_grammar_parser.py          # Parser tests
   ├── test_relational_algebra.py      # Relational algebra tests
   ├── test_semantic_validator.py      # Validation tests
   ├── test_query_optimizer.py         # Optimizer tests  
   └── fixtures.py                     # Shared fixtures

**Conventions:**
- Use pytest fixtures for common test data
- Separate unit tests from integration tests
- Aim for >90% coverage on new code

Test Execution
~~~~~~~~~~~~~~

.. code-block:: bash

   # Run all tests
   uv run pytest
   
   # Run with coverage
   uv run pytest --cov=pycypher --cov-report=html
   
   # Run specific tests
   uv run pytest tests/test_ast_models.py
   
   # Parallel execution
   uv run pytest -n 4

Configuration Management
------------------------

Environment Management
~~~~~~~~~~~~~~~~~~~~~~

Use ``uv`` for all Python operations:

.. code-block:: bash

   uv sync
   
   # Run Python scripts
   uv run python script.py
   
   # Install packages
   uv pip install <package>

**Never use pip directly** - always use ``uv pip`` to maintain workspace consistency.

Type Checking
~~~~~~~~~~~~~

Use ``ty`` (not mypy) for type checking:

.. code-block:: bash

   uv run ty check

**Requirements:**
- All functions MUST have type annotations
- Python 3.14.0a6+freethreaded required
- Strict type checking enabled

Code Formatting
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Format code
   make format  # Runs isort + ruff format
   
   # Check without modifying
   ruff check

**Configuration:** ``ruff.toml`` with ``select = ["ALL"]`` (highly strict)

Build System
------------

Makefile Targets
~~~~~~~~~~~~~~~~

.. code-block:: bash

   make test      # Run test suite
   make coverage  # Generate coverage report
   make format    # Format code
   make docs      # Build documentation
   make clean     # Remove build artifacts

**Build ordering:** The Makefile handles workspace dependencies automatically.

Package Management
~~~~~~~~~~~~~~~~~~

Workspace packages reference each other via ``tool.uv.sources``:

.. code-block:: toml

   [tool.uv.sources]
   pycypher = { workspace = true }
   shared = { workspace = true }

**After editing ``pyproject.toml``:** Always run ``uv sync``

Performance Characteristics
---------------------------

Parser
~~~~~~

- **Grammar:** LALR parser (fast, deterministic)
- **AST size:** O(n) in query complexity
- **Speed:** ~1000 queries/second for typical queries

Relational Algebra
~~~~~~~~~~~~~~~~~~

- **Translation:** O(n) in AST size
- **Column naming:** Deterministic, cached
- **Join strategy:** ID-only, efficient merging

Query Execution
~~~~~~~~~~~~~~~

**Relational Algebra:**
- Translation: O(n) in AST size
- Execution: pandas-based operations
- Memory: DataFrame-dependent

**Performance Tips:**
- Filter early (push down filters)
- Use ID-only joins where possible
- Fetch attributes lazily via Projection

For More Information
--------------------

* See :doc:`testing` for test guidelines
* See :doc:`contributing` for development workflow
* See :doc:`release` for release process
