Error Handling Patterns
=======================

PyCypher uses a structured exception hierarchy that lets you catch errors
at the granularity you need.  All exceptions are importable from the
top-level ``pycypher`` package.

Exception Hierarchy
-------------------

.. code-block:: text

   SyntaxError
   └── CypherSyntaxError          — invalid Cypher syntax

   ValueError
   ├── ASTConversionError          — grammar parsed but AST build failed
   │   └── GrammarTransformerSyncError  — grammar/AST model mismatch
   ├── GraphTypeNotFoundError      — unknown entity label or relationship type
   ├── VariableNotFoundError       — variable not in scope
   ├── VariableTypeMismatchError   — variable exists but wrong type
   ├── UnsupportedFunctionError    — unknown function name
   ├── FunctionArgumentError       — wrong argument count
   ├── MissingParameterError       — query parameter not provided
   └── InvalidCastError            — failed type cast (e.g. toInteger("abc"))

   TypeError
   ├── WrongCypherTypeError        — unexpected expression type
   └── IncompatibleOperatorError   — operator/type mismatch
       └── TemporalArithmeticError — date/time arithmetic error

   TimeoutError
   └── QueryTimeoutError           — query exceeded wall-clock budget

   MemoryError
   └── QueryMemoryBudgetError      — estimated memory exceeds budget

Every custom exception inherits from a Python built-in, so existing
``except ValueError`` or ``except TypeError`` handlers continue to work
without modification.

.. _cyphersyntaxerror:

Catching Parse Errors
---------------------

Use :class:`~pycypher.exceptions.CypherSyntaxError` for user-provided
queries that might have typos:

.. code-block:: python

   from pycypher import Star, CypherSyntaxError

   star = Star(context)
   try:
       result = star.execute_query(user_query)
   except CypherSyntaxError as e:
       print(f"Syntax error at line {e.line}, column {e.column}")
       # e.query contains the original query string

.. _variablenotfounderror:
.. _unsupportedfunctionerror:
.. _graphtypenotfounderror:

Catching Runtime Errors
-----------------------

For dynamic queries, catch the specific exception types:

.. code-block:: python

   from pycypher import (
       Star,
       VariableNotFoundError,
       UnsupportedFunctionError,
       GraphTypeNotFoundError,
   )

   try:
       result = star.execute_query(query)
   except VariableNotFoundError as e:
       # e.variable_name, e.available_variables, e.hint
       print(f"Unknown variable '{e.variable_name}'")
       print(f"Available: {e.available_variables}")
   except UnsupportedFunctionError as e:
       # e.function_name, e.supported_functions, e.category
       print(f"No such function '{e.function_name}'")
   except GraphTypeNotFoundError as e:
       # e.type_name
       print(f"No entity type '{e.type_name}' registered")

.. _querytimeouterror:
.. _querymemorybudgeterror:
.. _querycomplexityerror:

Resource Limit Errors
---------------------

Protect against runaway queries in production:

.. code-block:: python

   from pycypher import Star, QueryTimeoutError, QueryMemoryBudgetError

   try:
       result = star.execute_query(
           query,
           timeout_seconds=10.0,
           memory_budget_bytes=500 * 1024 * 1024,  # 500 MB
       )
   except QueryTimeoutError as e:
       print(f"Query timed out after {e.elapsed_seconds:.1f}s "
             f"(budget: {e.timeout_seconds}s)")
   except QueryMemoryBudgetError as e:
       print(f"Estimated {e.estimated_bytes / 1e6:.0f}MB exceeds "
             f"budget {e.budget_bytes / 1e6:.0f}MB")

Pre-execution Validation
------------------------

Use :func:`~pycypher.semantic_validator.validate_query` to catch errors
*before* executing the query:

.. code-block:: python

   from pycypher import validate_query

   errors = validate_query("MATCH (n:Person) RETURN m.name")
   for error in errors:
       print(f"{error.severity.value}: {error.message}")
       # "error: Variable 'm' is not defined..."

.. _cyclicdependencyerror:
.. _securityerror:

Security and Pipeline Errors
-----------------------------

:class:`~pycypher.exceptions.SecurityError` is raised when SQL injection,
path traversal, or SSRF attempts are detected in data source URIs or
DuckDB queries.

:class:`~pycypher.exceptions.CyclicDependencyError` is raised when a
multi-query pipeline contains circular dependencies that prevent
topological ordering.

Best Practices
--------------

1. **Catch specific exceptions first**, then broad ones:

   .. code-block:: python

      try:
          result = star.execute_query(query)
      except CypherSyntaxError:
          ...  # user typo
      except (VariableNotFoundError, GraphTypeNotFoundError):
          ...  # schema mismatch
      except (QueryTimeoutError, QueryMemoryBudgetError):
          ...  # resource limits
      except (TypeError, ValueError):
          ...  # catch-all for remaining pycypher errors

2. **Use structured attributes** rather than parsing error messages.
   Every exception exposes the relevant data as named attributes.

3. **Use pre-execution validation** for user-submitted queries to give
   faster, friendlier feedback without the cost of execution.

4. **Set timeouts in production** — either via ``timeout_seconds`` parameter
   or the ``PYCYPHER_QUERY_TIMEOUT_S`` environment variable.
