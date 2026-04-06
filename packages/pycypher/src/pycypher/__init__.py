"""PyCypher package for Cypher query parsing and execution.

This package provides functionality for parsing and executing Cypher queries
against graph-like data structures represented as collections of facts.

Quick start::

    import pandas as pd
    from pycypher import ContextBuilder, Star

    df = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
    context = ContextBuilder().add_entity("Person", df).build()
    star = Star(context=context)
    result = star.execute_query(
        "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC"
    )

Or load multiple entity types at once with :meth:`ContextBuilder.from_dict`::

    context = ContextBuilder.from_dict({"Person": people_df, "Company": companies_df})

You can check which backend engine is active via :attr:`Context.backend_name`::

    print(context.backend_name)  # "pandas", "duckdb", or "polars"

For optional pre-execution semantic validation (undefined variable detection,
aggregation rule checking, etc.) use :class:`SemanticValidator` or the
convenience wrapper :func:`validate_query`::

    from pycypher import validate_query

    errors = validate_query("MATCH (n:Person) RETURN m")
    for error in errors:
        print(f"{error.severity.value}: {error.message}")

Exception hierarchy
-------------------

All exceptions are importable from :mod:`pycypher` directly.

**Parse / conversion errors** (catch when building queries dynamically):

- :class:`CypherSyntaxError` (:class:`SyntaxError`) — invalid Cypher syntax (wraps Lark errors with line/column info)
- :class:`ASTConversionError` — grammar parsed but AST conversion failed
- :class:`GrammarTransformerSyncError` — grammar/AST model mismatch (subclass of above)

**Type errors** (catch when operand types are wrong):

- :class:`WrongCypherTypeError` (:class:`TypeError`) — unexpected expression type
- :class:`InvalidCastError` (:class:`ValueError`) — failed type cast
- :class:`IncompatibleOperatorError` (:class:`TypeError`) — operator/type mismatch
- :class:`TemporalArithmeticError` — date/time arithmetic error (subclass of above)

**Runtime errors** (catch during query execution):

- :class:`GraphTypeNotFoundError` (:class:`ValueError`) — unknown entity label or relationship type
- :class:`VariableNotFoundError` — variable not in scope (includes close-match suggestions)
- :class:`VariableTypeMismatchError` — variable exists but wrong type
- :class:`UnsupportedFunctionError` — unknown function (lists available alternatives)
- :class:`UnsupportedOperatorError` (:class:`ValueError`) — operator not in dispatch table
- :class:`FunctionArgumentError` — wrong argument count
- :class:`MissingParameterError` — query parameter not provided
- :class:`PatternComprehensionError` (:class:`ValueError`) — invalid pattern comprehension structure
- :class:`QueryComplexityError` (:class:`ValueError`) — query complexity exceeds configured limit
- :class:`QueryTimeoutError` (:class:`TimeoutError`) — query exceeded wall-clock budget

**Dependency errors** (catch when composing multi-query pipelines):

- :class:`CyclicDependencyError` (:class:`ValueError`) — circular dependency in query graph

**Security errors** (catch when validating untrusted input):

- :class:`SecurityError` (:class:`Exception`) — SQL injection, path traversal, or SSRF detected

Example — catching specific errors::

    from pycypher import Star, VariableNotFoundError, UnsupportedFunctionError

    try:
        result = star.execute_query("MATCH (n:Person) RETURN m.name")
    except VariableNotFoundError as e:
        print(f"Unknown variable '{e.variable_name}'. Available: {e.available_variables}")
    except UnsupportedFunctionError as e:
        print(f"No such function '{e.function_name}'. Try: {e.supported_functions}")

To list all supported Cypher functions::

    print(star.available_functions())

API stability
-------------

PyCypher is in **Alpha** (``0.0.x``).  Breaking changes may occur in any
release.  The following table summarises the stability of each public symbol:

.. list-table::
   :header-rows: 1
   :widths: 40 15 45

   * - Symbol
     - Stability
     - Notes
   * - :class:`Star`, :meth:`Star.execute_query`
     - **Stable**
     - Core entry point; breaking changes will be announced in CHANGELOG.
   * - :class:`ContextBuilder`, :class:`Context`
     - **Stable**
     - Primary data-loading API.
   * - All exception classes
     - **Stable**
     - Exception hierarchy is stable; new attributes may be added.
   * - :func:`validate_query`, :class:`SemanticValidator`
     - **Stable**
     - Pre-execution validation API.
   * - :class:`Pipeline`, :class:`Stage`
     - Provisional
     - Multi-stage ETL pipeline; API may change in ``0.1.0``.
   * - :class:`ResultCache`, :func:`get_cache_stats`
     - Provisional
     - Caching internals; API may change.
"""

from __future__ import annotations

from pycypher.exceptions import (
    ASTConversionError,
    CyclicDependencyError,
    CypherSyntaxError,
    FunctionArgumentError,
    GrammarTransformerSyncError,
    GraphTypeNotFoundError,
    IncompatibleOperatorError,
    InvalidCastError,
    MissingParameterError,
    PatternComprehensionError,
    QueryComplexityError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    SecurityError,
    TemporalArithmeticError,
    UnsupportedFunctionError,
    UnsupportedOperatorError,
    VariableNotFoundError,
    VariableTypeMismatchError,
    WrongCypherTypeError,
)
from pycypher.ingestion import ContextBuilder
from pycypher.pipeline import Pipeline, PipelineContext, PipelineResult, Stage
from pycypher.relational_models import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.config import apply_preset, show_config
from pycypher.semantic_validator import SemanticValidator, validate_query
from pycypher.star import ResultCache, Star, get_cache_stats

__version__ = "0.0.19"

__all__ = [
    # Exceptions — parse/conversion errors
    "ASTConversionError",
    "CypherSyntaxError",
    "GrammarTransformerSyncError",
    # Exceptions — type errors
    "IncompatibleOperatorError",
    "InvalidCastError",
    "TemporalArithmeticError",
    "WrongCypherTypeError",
    # Exceptions — dependency errors
    "CyclicDependencyError",
    # Exceptions — runtime errors
    "FunctionArgumentError",
    "GraphTypeNotFoundError",
    "MissingParameterError",
    "PatternComprehensionError",
    "QueryComplexityError",
    "QueryMemoryBudgetError",
    "QueryTimeoutError",
    "UnsupportedFunctionError",
    "UnsupportedOperatorError",
    "VariableNotFoundError",
    "VariableTypeMismatchError",
    # Exceptions — security errors
    "SecurityError",
    # Constants
    "ID_COLUMN",
    "RELATIONSHIP_SOURCE_COLUMN",
    "RELATIONSHIP_TARGET_COLUMN",
    # Ingestion helpers
    "ContextBuilder",
    # Data containers
    "Context",
    "EntityMapping",
    "EntityTable",
    "RelationshipMapping",
    "RelationshipTable",
    # Pre-execution validation
    "SemanticValidator",
    # Core query execution
    "Star",
    "validate_query",
    # Caching
    "ResultCache",
    "get_cache_stats",
    # Pipeline
    "Pipeline",
    "PipelineContext",
    "PipelineResult",
    "Stage",
    # Configuration
    "apply_preset",
    "show_config",
]


def __getattr__(name: str) -> type:
    """Lazy attribute lookup with typo suggestions against ``__all__``."""
    import difflib

    matches = difflib.get_close_matches(name, __all__, n=1, cutoff=0.6)
    hint = f"  Did you mean {matches[0]!r}?" if matches else ""
    msg = f"module {__name__!r} has no attribute {name!r}.{hint}"
    raise AttributeError(msg)
