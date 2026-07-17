Architecture
============

Overview of the PyCypher architecture and design principles.

Project Structure
-----------------

This is a **monorepo workspace** containing three interdependent packages:

.. code-block:: text

   pycypher-nmetl/
   ├── packages/
   │   ├── shared/         # Common utilities and logging
   │   ├── pycypher/       # Cypher parser, AST, and BindingFrame execution engine
   │   └── fastopendata/   # ETL pipeline for open data (Census, TIGER, OSM)
   ├── tests/              # Test suite
   ├── docs/               # Documentation
   └── pyproject.toml      # Workspace configuration

Dependency Order
~~~~~~~~~~~~~~~~

Packages depend on each other in this order::

   shared → pycypher → fastopendata

- **shared** has no dependencies on other packages
- **pycypher** depends on **shared**
- **fastopendata** depends on **shared** and **pycypher**

PyCypher Architecture
---------------------

Core Components
~~~~~~~~~~~~~~~

**Grammar Parser**

Uses Lark with the Earley algorithm for maximum grammar expressiveness.  The
compiled parser is cached as a process-level singleton via
:func:`~pycypher.grammar_parser.get_default_parser`; do not construct
``GrammarParser()`` directly in application code:

.. code-block:: python

   from pycypher.grammar_parser import get_default_parser

   # Returns the cached singleton — safe to call repeatedly
   parser = get_default_parser()
   raw_ast = parser.parse_to_ast(query)

**Grammar Rule Mixins** (modular transformer architecture)

Grammar transformation rules are organized into five focused mixin modules
under ``pycypher.grammar_rule_mixins/``:

.. code-block:: text

   grammar_rule_mixins/
   ├── __init__.py       # Re-exports all 5 mixins
   ├── literals.py       # LiteralRulesMixin — numbers, strings, booleans, null, lists, maps
   ├── expressions.py    # ExpressionRulesMixin — boolean, comparison, arithmetic, string ops
   ├── patterns.py       # PatternRulesMixin — node/relationship patterns, labels, properties
   ├── functions.py      # FunctionRulesMixin — function calls, CASE, comprehensions, REDUCE
   └── clauses.py        # ClauseRulesMixin — MATCH, RETURN, WITH, SET, DELETE, CREATE, MERGE

``CompositeTransformer`` (in ``grammar_transformers.py``) orchestrates these
mixins with method-resolution caching for thread-safe concurrent AST warmup.

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

**BindingFrame IR**

The core intermediate representation for query execution:

.. code-block:: python

   from pycypher.binding_frame import BindingFrame, EntityScan, BindingFilter
   from pycypher.binding_evaluator import BindingExpressionEvaluator

   # BindingFrame: DataFrame whose columns ARE Cypher variable names
   # No column prefixing, no hash IDs, no variable_map threading
   scan = EntityScan(entity_type="Person", var_name="p")
   frame = scan.scan(context)

   # Properties fetched on demand — never stored in the frame
   names = frame.get_property("p", "name")

**Relational Models**

Data containers for entity and relationship tables:

.. code-block:: python

   from pycypher.relational_models import (
       EntityTable,        # Holds entity IDs + attributes
       RelationshipTable,  # Holds relationship IDs + source/target
       Context,            # Holds all tables + registered functions
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
- Translation to BindingFrame operations

BindingFrame Execution Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Variables are column names.  Each row is one possible assignment of entity
IDs to those variables.  Attributes are fetched on demand by ID-keyed lookup
against the entity table in the ``Context``:

.. code-block:: python

   # EntityScan: produces single-column frame
   # Person IDs in column "p"

   # RelationshipScan: produces three-column frame
   # rel IDs in "r", source in "_src_r", target in "_tgt_r"

   # join(left_col, right_col): inner join on the shared structural key
   # filter(mask): boolean mask filter

**Benefits over legacy relational algebra:**

- No opaque 32-hex HASH_ID column names
- No ``EntityType__propertyname`` column names bleeding into operator logic
- No ``variable_map`` metadata threading through every operator
- Variables as column names: the column *is* the variable

Query-Scoped Atomicity
~~~~~~~~~~~~~~~~~~~~~~

SET clause mutations are buffered in a shadow layer on ``Context``
and only committed on successful query completion:

.. code-block:: python

   context.begin_query()   # initialise shadow layer
   try:
       result = execute_inner(query)
       context.commit_query()    # promote shadows to canonical tables
       return result
   except Exception:
       context.rollback_query()  # discard all pending mutations
       raise

This ensures a failed query never leaves the context in a partially-mutated
state.

Extension Points
----------------

Custom Cypher Functions
~~~~~~~~~~~~~~~~~~~~~~~

Register user-defined functions via the ``@context.cypher_function`` decorator:

.. code-block:: python

   from pycypher.relational_models import Context

   context = Context()

   @context.cypher_function
   def my_function(x):
       return x * 2

   # Now callable as my_function(n.value) in Cypher queries

Custom Scalar Functions
~~~~~~~~~~~~~~~~~~~~~~~

Register additional scalar functions in the ``ScalarFunctionRegistry``:

.. code-block:: python

   from pycypher.scalar_functions import ScalarFunctionRegistry

   registry = ScalarFunctionRegistry.get_instance()
   registry.register_function(
       "myFunc", lambda s: s.apply(str.upper), min_args=1, max_args=1,
   )

Testing Architecture
--------------------

Test Organization
~~~~~~~~~~~~~~~~~

Tests are co-located in the ``tests/`` directory:

.. code-block:: text

   tests/
   ├── test_ast_models.py              # AST node tests
   ├── test_ast_models_coverage_gaps.py # Coverage improvements
   ├── test_grammar_parser.py          # Parser tests
   ├── test_data_correctness_*.py      # Data correctness tests
   ├── test_semantic_validator.py      # Validation tests
   ├── test_binding_frame.py           # BindingFrame IR tests
   └── test_star_gaps.py               # Star execution tests

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

**Configuration:** ``pyproject.toml`` (``[tool.ruff]`` section) with ``select = ["ALL"]`` (highly strict)

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

- **Grammar:** Earley parser (handles ambiguous grammars; trades speed for
  expressiveness vs LALR)
- **Caching:** Grammar compiled once per process (``get_default_parser()``
  singleton).  Parse result (AST) also cached by query string with LRU
  capacity 512 — repeated queries are O(1) lookups.
- **Cold parse:** ~56 ms per unique query; **warm (cached):** < 0.1 ms.

Query Execution
~~~~~~~~~~~~~~~

- **Translation:** O(n) in AST size
- **Property lookups:** ID-keyed, vectorised via pandas ``set_index`` +
  ``map``; never fetched unless referenced in a clause.
- **Aggregation:** Grouped aggregation uses pandas native Cython paths
  (``"count"``, ``"mean"``, ``"min"``, ``"max"``, ``grouped.sum()``,
  ``grouped.std()``) rather than per-group Python callbacks.
- **Scalar functions:** Math functions (``abs``, ``ceil``, ``floor``,
  ``sign``, ``sqrt``, ``cbrt``, ``log``, ``log2``, ``log10``, ``exp``,
  ``pow``, and all trig functions) use numpy C-level array operations via
  a ``_make_math1_np`` / ``_make_trig1_np`` factory pattern — one numpy
  call per Series regardless of row count.  String predicate functions
  (``startsWith``, ``endsWith``, ``contains``) use the pandas ``.str``
  Cython accessor.  These replace the previous ``pd.Series.apply()``
  paths (one Python call per row) and give ~3–5× speedup on large frames.
- **Performance tips:** Push WHERE predicates early; use inline property
  filters ``{prop: val}`` on node patterns to reduce scan sizes; avoid
  unbounded variable-length paths (``*``) on large graphs.

For More Information
--------------------

* See :doc:`testing` for test guidelines
* See :doc:`contributing` for development workflow
* See :doc:`release` for release process
