Query Validation
================

Learn how to validate openCypher queries before execution using PyCypher's
two-level validation system: **syntax validation** (grammar parsing) and
**semantic validation** (variable scoping, aggregation rules, etc.).

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Familiarity with Cypher query syntax

Overview
--------

PyCypher provides two independent validation layers:

1. **Syntax validation** — checks whether a query string is grammatically valid
   Cypher via :meth:`~pycypher.grammar_parser.GrammarParser.validate`.
2. **Semantic validation** — checks whether a syntactically valid query makes
   *logical* sense (no undefined variables, correct aggregation usage, etc.)
   via :class:`~pycypher.semantic_validator.SemanticValidator`.

Both can be used *before* executing a query, allowing you to surface errors
early — especially useful in interactive tools or multi-tenant systems.

Syntax Validation
-----------------

Quick Check
~~~~~~~~~~~

The :meth:`~pycypher.grammar_parser.GrammarParser.validate` method returns
``True`` for valid Cypher and ``False`` for syntax errors without raising:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser

   parser = GrammarParser()

   parser.validate("MATCH (n:Person) RETURN n.name")  # True
   parser.validate("METCH (n) RETRN n")               # False

Catching Parse Errors
~~~~~~~~~~~~~~~~~~~~~

For richer diagnostics, parse the query directly and catch Lark exceptions:

.. code-block:: python

   from lark.exceptions import UnexpectedInput

   try:
       tree = parser.parse("MATCH (n:Person RETURN n")
   except UnexpectedInput as e:
       print(f"Syntax error at line {e.line}, column {e.column}")
       print(f"  {e}")

Lark raises specific subclasses — ``UnexpectedToken``, ``UnexpectedCharacters``,
``UnexpectedEOF`` — that you can catch individually for more targeted handling.

Semantic Validation
-------------------

Basic Usage
~~~~~~~~~~~

The :class:`~pycypher.semantic_validator.SemanticValidator` walks a parsed
Lark tree and reports issues:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.semantic_validator import SemanticValidator

   parser = GrammarParser()
   validator = SemanticValidator()

   tree = parser.parse("MATCH (n:Person) RETURN m")
   errors = validator.validate(tree)

   for error in errors:
       print(error)
   # ERROR: Variable 'm' is used but not defined

Error Severity Levels
~~~~~~~~~~~~~~~~~~~~~

Each :class:`~pycypher.semantic_validator.ValidationError` carries a severity:

.. code-block:: python

   from pycypher.semantic_validator import ErrorSeverity

   for error in errors:
       if error.severity == ErrorSeverity.ERROR:
           print(f"BLOCK: {error.message}")
       elif error.severity == ErrorSeverity.WARNING:
           print(f"WARN:  {error.message}")
       elif error.severity == ErrorSeverity.INFO:
           print(f"INFO:  {error.message}")

- **ERROR** — the query will fail or produce wrong results
- **WARNING** — the query will work but may have issues
- **INFO** — suggestions for improvement

What Gets Validated
~~~~~~~~~~~~~~~~~~~

The semantic validator checks:

* **Undefined variables** — using a variable that was never bound in a MATCH
  or WITH clause
* **Variable scope** — variables must be visible in the clause where they
  are referenced
* **Aggregation rules** — mixing aggregated and non-aggregated expressions
  in RETURN without grouping
* **Function signatures** — known function names and arity

Convenience Wrapper
~~~~~~~~~~~~~~~~~~~

For the simplest use case, import the top-level :func:`~pycypher.validate_query`
helper:

.. code-block:: python

   from pycypher import validate_query

   errors = validate_query("MATCH (n:Person) RETURN m")
   for error in errors:
       print(f"{error.severity.value}: {error.message}")

AST-Level Validation
--------------------

Beyond the Lark-tree semantic validator, the typed AST layer
(:mod:`pycypher.ast_models`) provides a :class:`~pycypher.ast_models.ValidationResult`
framework.  This is used internally but is also available for custom checks:

.. code-block:: python

   from pycypher.ast_models import ValidationResult, ValidationSeverity

   result = ValidationResult()
   result.add_error("Missing RETURN clause")
   result.add_warning("Unused variable 'x'", suggestion="Remove or reference it")

   if result.is_valid:
       print("No errors")
   else:
       for issue in result.errors:
           print(issue)

The ``ValidationResult`` object provides filtered access:

- ``result.errors`` — error-level issues only
- ``result.warnings`` — warning-level issues only
- ``result.infos`` — informational issues only
- ``result.is_valid`` — ``True`` if no errors (warnings are acceptable)
- ``result.has_errors`` / ``result.has_warnings`` — boolean checks

Runtime Exception Handling
--------------------------

When validation is skipped or insufficient, PyCypher raises specific
exceptions at execution time.  These are designed to be actionable:

.. code-block:: python

   from pycypher import (
       Star,
       VariableNotFoundError,
       UnsupportedFunctionError,
       GraphTypeNotFoundError,
   )

   try:
       result = star.execute_query("MATCH (n:Person) RETURN m.name")
   except VariableNotFoundError as e:
       # e.variable_name: the undefined variable
       # e.available_variables: what IS defined
       print(f"Unknown variable '{e.variable_name}'")
       print(f"  Available: {e.available_variables}")

   try:
       result = star.execute_query("MATCH (n:Person) RETURN noSuchFunc(n)")
   except UnsupportedFunctionError as e:
       # e.function_name: what was called
       # e.supported_functions: list of valid alternatives
       print(f"No function '{e.function_name}'")

   try:
       result = star.execute_query("MATCH (g:Ghost) RETURN g")
   except GraphTypeNotFoundError as e:
       # e.type_name: the unknown entity label
       print(f"Entity type '{e.type_name}' not in context")

See the :mod:`pycypher` package docstring for the complete exception hierarchy.

Putting It Together
-------------------

A robust validation pipeline might look like this:

.. code-block:: python

   from lark.exceptions import UnexpectedInput
   from pycypher.grammar_parser import GrammarParser
   from pycypher.semantic_validator import SemanticValidator, ErrorSeverity

   def validate_before_run(query: str) -> list[str]:
       """Return a list of human-readable error messages, or [] if valid."""
       parser = GrammarParser()
       issues = []

       # Step 1: syntax check
       try:
           tree = parser.parse(query)
       except UnexpectedInput as e:
           return [f"Syntax error at {e.line}:{e.column}: {e}"]

       # Step 2: semantic check
       validator = SemanticValidator()
       for error in validator.validate(tree):
           if error.severity == ErrorSeverity.ERROR:
               issues.append(error.message)

       return issues

Next Steps
----------

* :doc:`ast_manipulation` — inspect and build AST nodes programmatically
* :doc:`basic_query_parsing` — execute queries end-to-end
* :doc:`../user_guide/query_processing` — detailed pipeline reference
