"""Custom exceptions for the PyCypher package.

This package defines custom exception classes used throughout the PyCypher
package for handling specific error conditions during query parsing and
execution.

The module is split into three submodules for maintainability:

- :mod:`pycypher.exceptions.base` — Infrastructure (docs links, environment detection, sanitization)
- :mod:`pycypher.exceptions.parse` — Parse-time errors (syntax, AST conversion)
- :mod:`pycypher.exceptions.runtime` — Runtime errors (type, variable, function, resource, security)
"""

# Base infrastructure
from pycypher.exceptions.base import (
    _DOCS_ANCHORS,
    _DOCS_BASE_URL,
    DocsLink,
    _detect_environment,
    _docs_hint,
    _make_docs_link,
    sanitize_error_message,
)

# Parse-time errors
from pycypher.exceptions.parse import (
    ASTConversionError,
    CypherSyntaxError,
    GrammarTransformerSyncError,
)

# Runtime errors
from pycypher.exceptions.runtime import (
    CacheLockTimeoutError,
    CyclicDependencyError,
    FunctionArgumentError,
    GraphTypeNotFoundError,
    IncompatibleOperatorError,
    InvalidCastError,
    MissingParameterError,
    PatternComprehensionError,
    QueryComplexityError,
    QueryMemoryBudgetError,
    QueryTimeoutError,
    RateLimitError,
    SecurityError,
    TemporalArithmeticError,
    UnsupportedFunctionError,
    UnsupportedOperatorError,
    VariableNotFoundError,
    VariableTypeMismatchError,
    WorkerExecutionError,
    WrongCypherTypeError,
    _complexity_suggestions,
    _type_specific_suggestion,
)

__all__ = [
    # Base
    "DocsLink",
    "sanitize_error_message",
    # Parse
    "ASTConversionError",
    "CypherSyntaxError",
    "GrammarTransformerSyncError",
    # Runtime
    "CacheLockTimeoutError",
    "CyclicDependencyError",
    "FunctionArgumentError",
    "GraphTypeNotFoundError",
    "IncompatibleOperatorError",
    "InvalidCastError",
    "MissingParameterError",
    "PatternComprehensionError",
    "QueryComplexityError",
    "QueryMemoryBudgetError",
    "QueryTimeoutError",
    "RateLimitError",
    "SecurityError",
    "TemporalArithmeticError",
    "UnsupportedFunctionError",
    "UnsupportedOperatorError",
    "VariableNotFoundError",
    "VariableTypeMismatchError",
    "WorkerExecutionError",
    "WrongCypherTypeError",
]
