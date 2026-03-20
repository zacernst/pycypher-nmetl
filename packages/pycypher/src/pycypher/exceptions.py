"""Custom exceptions for the PyCypher package.

This module defines custom exception classes used throughout the PyCypher
package for handling specific error conditions during query parsing and
execution.
"""


class GraphTypeNotFoundError(ValueError):
    """Raised when a node label or relationship type is not registered in the context.

    This is a :class:`ValueError` subclass so that existing code catching
    ``ValueError`` continues to work.  New code that only wants to handle the
    "entity/relationship type absent from context" case (e.g. MERGE's create
    fallback) can catch this specific subclass without accidentally swallowing
    unrelated errors.

    Attributes:
        type_name: The label or relationship type that was not found.

    Example::

        try:
            EntityScan("Ghost", "g").scan(context)
        except GraphTypeNotFoundError as exc:
            print(f"Type not registered: {exc.type_name}")

    """

    def __init__(self, type_name: str, message: str = "") -> None:
        """Initialise with the missing type name and an optional detail message.

        Args:
            type_name: The label or relationship type that was absent.
            message: Optional additional detail; if omitted a default is used.

        """
        self.type_name = type_name
        detail = (
            message
            or f"Graph type {type_name!r} is not registered in the context."
        )
        super().__init__(detail)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"GraphTypeNotFoundError(type_name={self.type_name!r})"


class WrongCypherTypeError(TypeError):
    """Exception raised when a Cypher expression has an unexpected type.

    This exception is thrown when the CypherParser can parse an expression
    but the resulting type is not what was expected for the given context.

    Attributes:
        message: Human-readable error message describing the type mismatch.

    """

    def __init__(self, message: str) -> None:
        """Initialize the exception with an error message.

        Args:
            message: Description of the type error that occurred.

        """
        self.message = message
        super().__init__(self.message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"WrongCypherTypeError(message={self.message!r})"


class InvalidCastError(ValueError):
    """Exception raised when a type cast operation fails.

    This exception is thrown when attempting to cast a value to an
    incompatible type during query processing or data conversion.

    Attributes:
        message: Human-readable error message describing the cast failure.

    """

    def __init__(self, message: str) -> None:
        """Initialize the exception with an error message.

        Args:
            message: Description of the cast error that occurred.

        """
        self.message = message
        super().__init__(self.message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"InvalidCastError(message={self.message!r})"


# New exception classes for error handling modernization (Loop 247)


class ASTConversionError(ValueError):
    """Exception raised when AST conversion from grammar parse tree fails.

    This exception is thrown when the grammar parser successfully parses a query
    but the conversion to typed AST models fails, typically due to grammar/AST
    model synchronization issues or unsupported syntax constructs.

    Attributes:
        query_fragment: The portion of the query that failed conversion.
        node_type: The AST node type that could not be converted.

    """

    def __init__(
        self,
        message: str,
        query_fragment: str = "",
        node_type: str = "",
    ) -> None:
        """Initialize with error message and optional context.

        Args:
            message: Human-readable description of the conversion failure.
            query_fragment: The query text that triggered the error.
            node_type: The AST node type that failed conversion.

        """
        self.query_fragment = query_fragment
        self.node_type = node_type

        # Build concise, actionable error message
        if query_fragment:
            short_fragment = (
                query_fragment[:50] + "..."
                if len(query_fragment) > 50
                else query_fragment
            )
            full_message = f"{message} Query: {short_fragment!r}"
        else:
            full_message = message

        super().__init__(full_message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = (
            [f"query_fragment={self.query_fragment!r}"]
            if self.query_fragment
            else []
        )
        if self.node_type:
            parts.append(f"node_type={self.node_type!r}")
        detail = ", ".join(parts)
        return (
            f"ASTConversionError({detail})"
            if detail
            else f"ASTConversionError({str(self)!r})"
        )


class GrammarTransformerSyncError(ASTConversionError):
    """Exception raised when grammar transformer and AST models are out of sync.

    This is a specialized ASTConversionError for cases where the grammar parser
    produces a node type that has no corresponding AST model class.

    Attributes:
        missing_node_type: The node type that lacks a corresponding AST class.

    """

    def __init__(
        self,
        message: str,
        missing_node_type: str = "",
        query_fragment: str = "",
    ) -> None:
        """Initialize with sync error details.

        Args:
            message: Description of the synchronization issue.
            missing_node_type: The node type missing from AST models.
            query_fragment: The query text that triggered the error.

        """
        self.missing_node_type = missing_node_type

        sync_message = (
            "Grammar and AST models are out of sync. "
            f"{message} "
            f"Missing node type: {missing_node_type!r}"
            if missing_node_type
            else message
        )

        super().__init__(
            sync_message,
            query_fragment=query_fragment,
            node_type=missing_node_type,
        )

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = (
            [f"missing_node_type={self.missing_node_type!r}"]
            if self.missing_node_type
            else []
        )
        if self.query_fragment:
            parts.append(f"query_fragment={self.query_fragment!r}")
        detail = ", ".join(parts)
        return (
            f"GrammarTransformerSyncError({detail})"
            if detail
            else f"GrammarTransformerSyncError({str(self)!r})"
        )


class CypherSyntaxError(SyntaxError):
    """User-friendly wrapper around Lark parse errors.

    Wraps low-level Lark ``UnexpectedInput`` exceptions into a message that
    shows the offending line, column position, and what the parser expected,
    without leaking internal terminal names like ``__ANON_0``.

    Attributes:
        query: The original query string.
        line: 1-based line number of the error.
        column: 1-based column number of the error.

    """

    def __init__(
        self,
        query: str,
        original: Exception,
    ) -> None:
        """Initialize from an original Lark exception.

        Args:
            query: The Cypher query that failed to parse.
            original: The Lark exception that triggered this error.

        """
        self.query = query
        self.original = original

        # Extract line/column from Lark exceptions when available
        raw_line = getattr(original, "line", None)
        raw_col = getattr(original, "column", None)
        # Lark uses -1 for "unknown" position; treat as None
        self.line: int | None = (
            raw_line if raw_line is not None and raw_line > 0 else None
        )
        self.column: int | None = (
            raw_col if raw_col is not None and raw_col > 0 else None
        )

        # Build user-friendly message
        parts: list[str] = ["Cypher syntax error"]
        if self.line is not None:
            parts.append(f" at line {self.line}")
            if self.column is not None:
                parts.append(f", column {self.column}")

        # Show the offending line with a pointer
        if self.line is not None:
            lines = query.splitlines()
            if 0 < self.line <= len(lines):
                parts.append(f"\n  {lines[self.line - 1]}")
                if self.column is not None and self.column > 0:
                    parts.append(f"\n  {' ' * (self.column - 1)}^")

        # Extract expected tokens, filtering out internal names.
        # Lark uses "expected" for UnexpectedToken and "allowed" for
        # UnexpectedCharacters — check both.
        _FRIENDLY_NAMES: dict[str, str] = {
            "LPAR": "(",
            "RPAR": ")",
            "LBRACE": "{",
            "RBRACE": "}",
            "LSQB": "[",
            "RSQB": "]",
            "COMMA": ",",
            "COLON": ":",
            "SEMICOLON": ";",
            "DOT": ".",
            "VBAR": "|",
            "PLUS": "+",
            "MINUS": "-",
            "STAR": "*",
            "SLASH": "/",
            "PERCENT": "%",
            "EQ": "=",
            "NEQ": "<>",
            "LT": "<",
            "GT": ">",
            "LTE": "<=",
            "GTE": ">=",
            "ARROW_LEFT": "<-",
            "ARROW_RIGHT": "->",
            "DASH": "-",
        }
        expected = getattr(original, "expected", None) or getattr(
            original, "allowed", None
        )
        if expected:
            clean = sorted(
                set(
                    _FRIENDLY_NAMES.get(t.strip("\"'"), t.strip("\"'"))
                    for t in expected
                    if not t.startswith("__") and not t.startswith("_")
                )
            )
            if clean:
                parts.append(f"\nExpected: {', '.join(clean)}")

        super().__init__("".join(parts))

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"CypherSyntaxError(line={self.line}, column={self.column})"


class VariableNotFoundError(ValueError):
    """Exception raised when a query variable is not found in the binding context.

    This exception is thrown when attempting to access a variable that hasn't
    been defined in the current query scope or binding frame.

    Attributes:
        variable_name: The name of the missing variable.
        available_variables: List of variables that are available in current scope.

    """

    def __init__(
        self,
        variable_name: str,
        available_variables: list[str],
        hint: str = "",
    ) -> None:
        """Initialize with variable name and available alternatives.

        Args:
            variable_name: The name of the variable that was not found.
            available_variables: Variables that are currently in scope.
            hint: Optional hint string from suggest_close_match (e.g., "  Did you mean 'person'?").

        """
        self.variable_name = variable_name
        self.available_variables = available_variables
        self.hint = hint

        if available_variables:
            available_str = ", ".join(available_variables)
            message = f"Variable '{variable_name}' is not defined. Available variables: {available_str}.{hint}"
        else:
            message = f"Variable '{variable_name}' is not defined. No variables are in scope.{hint}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"VariableNotFoundError(variable_name={self.variable_name!r}, "
            f"available_variables={self.available_variables!r})"
        )


class VariableTypeMismatchError(ValueError):
    """Exception raised when a variable has an unexpected type in the binding context.

    This exception is thrown when a variable exists but its type doesn't match
    what's expected for the operation being performed.

    Attributes:
        variable_name: The name of the variable with wrong type.
        expected_type: The type that was expected.
        actual_type: The actual type of the variable.
        suggestion: Optional suggestion for fixing the issue.

    """

    def __init__(
        self,
        variable_name: str,
        expected_type: str,
        actual_type: str,
        suggestion: str = "",
    ) -> None:
        """Initialize with type mismatch details.

        Args:
            variable_name: Name of the variable with wrong type.
            expected_type: The type that was expected.
            actual_type: The actual type found.
            suggestion: Optional suggestion for resolving the issue.

        """
        self.variable_name = variable_name
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.suggestion = suggestion

        message = (
            f"Variable '{variable_name}' has type '{actual_type}' "
            f"but '{expected_type}' expected."
        )

        if suggestion:
            message += f" {suggestion}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"VariableTypeMismatchError(variable_name={self.variable_name!r}, "
            f"expected_type={self.expected_type!r}, actual_type={self.actual_type!r})"
        )


class IncompatibleOperatorError(TypeError):
    """Exception raised when an operator cannot be applied between given types.

    This exception is thrown when attempting to use an operator (like +, -, etc.)
    between incompatible types that don't support the operation.

    Attributes:
        operator: The operator that couldn't be applied.
        left_type: Type of the left operand.
        right_type: Type of the right operand.
        suggestion: Optional suggestion for fixing the type issue.

    """

    def __init__(
        self,
        operator: str,
        left_type: str,
        right_type: str,
        suggestion: str = "",
    ) -> None:
        """Initialize with operator and type details.

        Args:
            operator: The operator that failed.
            left_type: Type of the left operand.
            right_type: Type of the right operand.
            suggestion: Optional suggestion for resolving the issue.

        """
        self.operator = operator
        self.left_type = left_type
        self.right_type = right_type
        self.suggestion = suggestion

        message = f"Operator '{operator}' incompatible between '{left_type}' and '{right_type}'"

        if suggestion:
            message += f". {suggestion}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"IncompatibleOperatorError(operator={self.operator!r}, "
            f"left_type={self.left_type!r}, right_type={self.right_type!r})"
        )


class TemporalArithmeticError(IncompatibleOperatorError):
    """Exception raised for temporal arithmetic type errors.

    This is a specialized IncompatibleOperatorError for temporal operations
    involving dates, times, and durations.

    Attributes:
        example: An example of correct temporal arithmetic usage.

    """

    def __init__(
        self,
        operator: str,
        left_type: str,
        right_type: str,
        example: str = "",
    ) -> None:
        """Initialize with temporal arithmetic error details.

        Args:
            operator: The temporal operator that failed.
            left_type: Type of the left temporal operand.
            right_type: Type of the right temporal operand.
            example: Example of correct temporal arithmetic.

        """
        self.example = example

        # Default examples for common temporal operations
        if not example:
            if operator in ["+", "-"]:
                example = "date('2024-01-01') + duration({days: 7})"
            else:
                example = "Use duration() for time arithmetic"

        suggestion = f"Example: {example}"
        super().__init__(operator, left_type, right_type, suggestion)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"TemporalArithmeticError(operator={self.operator!r}, "
            f"left_type={self.left_type!r}, right_type={self.right_type!r})"
        )


class UnsupportedFunctionError(ValueError):
    """Exception raised when an unsupported function is called.

    This exception is thrown when attempting to call a function that isn't
    implemented or available in the current context.

    Attributes:
        function_name: Name of the unsupported function.
        supported_functions: List of functions that are supported.
        category: Optional category of function (e.g., "aggregation", "scalar").

    """

    def __init__(
        self,
        function_name: str,
        supported_functions: list[str],
        category: str = "",
    ) -> None:
        """Initialize with function name and supported alternatives.

        Args:
            function_name: Name of the function that isn't supported.
            supported_functions: Functions that are available.
            category: Optional category for the function type.

        """
        self.function_name = function_name
        self.supported_functions = supported_functions
        self.category = category

        category_desc = f"{category} " if category else ""
        sorted_funcs = sorted(supported_functions)

        # For short lists (≤20), show all. For long lists, show a truncated
        # summary so the error message remains scannable instead of dumping
        # 100+ function names.
        _MAX_INLINE = 20
        if len(sorted_funcs) <= _MAX_INLINE:
            supported_str = ", ".join(sorted_funcs)
        else:
            preview = ", ".join(sorted_funcs[:_MAX_INLINE])
            supported_str = (
                f"{preview}, ... ({len(sorted_funcs)} total — "
                f"see exc.supported_functions for full list)"
            )

        message = (
            f"Unsupported {category_desc}function: '{function_name}'. "
            f"Supported: {supported_str}"
        )

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = [f"function_name={self.function_name!r}"]
        parts.append(f"supported_functions={self.supported_functions!r}")
        if self.category:
            parts.append(f"category={self.category!r}")
        return f"UnsupportedFunctionError({', '.join(parts)})"


class FunctionArgumentError(ValueError):
    """Exception raised when a function is called with wrong number of arguments.

    This exception is thrown when a function call has an incorrect number
    of arguments or the arguments don't match the expected signature.

    Attributes:
        function_name: Name of the function with argument error.
        expected_args: Number of expected arguments.
        actual_args: Number of actual arguments provided.
        argument_description: Description of expected arguments.

    """

    def __init__(
        self,
        function_name: str,
        expected_args: int,
        actual_args: int,
        argument_description: str = "",
    ) -> None:
        """Initialize with function argument mismatch details.

        Args:
            function_name: Name of the function with wrong arguments.
            expected_args: Expected number of arguments.
            actual_args: Actual number of arguments provided.
            argument_description: Description of what arguments are expected.

        """
        self.function_name = function_name
        self.expected_args = expected_args
        self.actual_args = actual_args
        self.argument_description = argument_description

        message = (
            f"Function '{function_name}' expects {expected_args} arguments "
            f"but {actual_args} provided"
        )

        if argument_description:
            message += f". Expected: {argument_description}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"FunctionArgumentError(function_name={self.function_name!r}, "
            f"expected_args={self.expected_args!r}, actual_args={self.actual_args!r})"
        )


class QueryTimeoutError(TimeoutError):
    """Exception raised when a query exceeds its execution time budget.

    This exception is thrown when a query's wall-clock execution time exceeds
    the configured timeout, preventing runaway queries from consuming resources
    indefinitely.

    Attributes:
        timeout_seconds: The timeout that was exceeded.
        elapsed_seconds: How long the query ran before being terminated.
        query_fragment: Truncated query text for diagnostics.

    """

    def __init__(
        self,
        timeout_seconds: float,
        elapsed_seconds: float = 0.0,
        query_fragment: str = "",
    ) -> None:
        """Initialize with timeout details.

        Args:
            timeout_seconds: The configured timeout in seconds.
            elapsed_seconds: Actual elapsed time before cancellation.
            query_fragment: Truncated query text for diagnostics.

        """
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds
        self.query_fragment = query_fragment

        message = f"Query exceeded {timeout_seconds}s timeout"
        if elapsed_seconds:
            message += f" (ran for {elapsed_seconds:.1f}s)"
        if query_fragment:
            short = (
                query_fragment[:80] + "..."
                if len(query_fragment) > 80
                else query_fragment
            )
            message += f". Query: {short!r}"

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"QueryTimeoutError(timeout_seconds={self.timeout_seconds!r}, "
            f"elapsed_seconds={self.elapsed_seconds!r})"
        )


class QueryMemoryBudgetError(MemoryError):
    """Exception raised when a query's estimated memory exceeds the budget.

    This exception is thrown during query planning when the estimated memory
    consumption exceeds the configured budget, preventing OOM crashes.

    Attributes:
        estimated_bytes: Estimated memory consumption in bytes.
        budget_bytes: The configured memory budget in bytes.
        suggestion: Actionable suggestion for reducing memory usage.

    """

    def __init__(
        self,
        estimated_bytes: int,
        budget_bytes: int,
        suggestion: str = "",
    ) -> None:
        """Initialize with memory budget details.

        Args:
            estimated_bytes: Estimated memory consumption.
            budget_bytes: The configured memory budget.
            suggestion: Actionable suggestion for reducing memory.

        """
        self.estimated_bytes = estimated_bytes
        self.budget_bytes = budget_bytes
        self.suggestion = suggestion

        est_mb = estimated_bytes / (1024 * 1024)
        budget_mb = budget_bytes / (1024 * 1024)
        message = (
            f"Estimated memory {est_mb:.0f}MB exceeds budget {budget_mb:.0f}MB"
        )

        if suggestion:
            message += f". {suggestion}"
        else:
            message += (
                ". To reduce memory: add LIMIT clause, add WHERE filters, "
                "or use SKIP/LIMIT to process in batches. "
                "To allow larger queries: increase memory_budget_bytes."
            )

        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"QueryMemoryBudgetError(estimated_bytes={self.estimated_bytes!r}, "
            f"budget_bytes={self.budget_bytes!r})"
        )


class MissingParameterError(ValueError):
    """Exception raised when a required query parameter is missing.

    This exception is thrown when a parameterized query references a parameter
    that wasn't provided in the parameters dictionary.

    Attributes:
        parameter_name: Name of the missing parameter.
        example_usage: Example of how to provide the parameter.

    """

    def __init__(self, parameter_name: str, example_usage: str = "") -> None:
        """Initialize with missing parameter details.

        Args:
            parameter_name: Name of the parameter that was missing.
            example_usage: Example of correct parameter usage.

        """
        self.parameter_name = parameter_name
        self.example_usage = (
            example_usage
            or f"execute_query(..., parameters={{'{parameter_name}': value}})"
        )

        message = f"Parameter '${parameter_name}' not provided. Use: {self.example_usage}"
        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"MissingParameterError(parameter_name={self.parameter_name!r})"


class UnsupportedOperatorError(ValueError):
    """Raised when an operator string is not in the supported dispatch table.

    This is a :class:`ValueError` subclass so that existing code catching
    ``ValueError`` continues to work.  It provides structured access to the
    operator that was rejected and the set of supported alternatives.

    Attributes:
        operator: The operator string that was not recognised.
        supported_operators: Sorted list of operators that *are* recognised.
        category: Optional label for the operator family (e.g. "comparison").

    """

    def __init__(
        self,
        operator: str,
        supported_operators: list[str],
        category: str = "",
    ) -> None:
        """Initialise with the unsupported operator and available alternatives.

        Args:
            operator: The operator string that was not recognised.
            supported_operators: Operators that are available.
            category: Optional label for the kind of operator.

        """
        self.operator = operator
        self.supported_operators = supported_operators
        self.category = category

        category_desc = f"{category} " if category else ""
        supported_str = ", ".join(sorted(supported_operators))
        message = (
            f"Unsupported {category_desc}operator: {operator!r}. "
            f"Supported: {supported_str}"
        )
        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        parts = [f"operator={self.operator!r}"]
        parts.append(f"supported_operators={self.supported_operators!r}")
        if self.category:
            parts.append(f"category={self.category!r}")
        return f"UnsupportedOperatorError({', '.join(parts)})"


class PatternComprehensionError(ValueError):
    """Raised when a pattern comprehension has an invalid structure.

    This is a :class:`ValueError` subclass so that existing code catching
    ``ValueError`` continues to work.

    Attributes:
        detail: Description of what is wrong with the pattern.

    """

    def __init__(self, detail: str) -> None:
        """Initialise with a description of the structural issue.

        Args:
            detail: Human-readable description of the pattern error.

        """
        self.detail = detail
        super().__init__(detail)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return f"PatternComprehensionError(detail={self.detail!r})"


class QueryComplexityError(ValueError):
    """Raised when a query's complexity score exceeds the configured limit.

    Attributes:
        score: The computed complexity score.
        limit: The configured maximum.
        breakdown: Per-feature score breakdown.

    """

    def __init__(
        self,
        score: int,
        limit: int,
        breakdown: dict[str, int] | None = None,
    ) -> None:
        """Initialize with score details.

        Args:
            score: The computed complexity score.
            limit: The configured maximum.
            breakdown: Per-feature score breakdown.

        """
        self.score = score
        self.limit = limit
        self.breakdown = breakdown or {}

        top_contributors = sorted(
            self.breakdown.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:3]
        top_str = ", ".join(f"{k}={v}" for k, v in top_contributors)

        message = (
            f"Query complexity score {score} exceeds limit {limit}. "
            f"Top contributors: {top_str}. "
            f"Simplify the query or increase max_complexity_score."
        )
        super().__init__(message)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"QueryComplexityError(score={self.score!r}, limit={self.limit!r})"
        )


class CyclicDependencyError(ValueError):
    """Raised when a circular dependency is detected in a multi-query graph.

    This is a :class:`ValueError` subclass so that existing code catching
    ``ValueError`` continues to work.

    Attributes:
        remaining_nodes: The set of node IDs involved in the cycle.

    """

    def __init__(
        self,
        remaining_nodes: set[str],
        message: str = "",
    ) -> None:
        """Initialise with the nodes involved in the cycle.

        Args:
            remaining_nodes: Node IDs that could not be topologically sorted.
            message: Optional detail message; if omitted a default is used.

        """
        self.remaining_nodes = remaining_nodes
        detail = message or (
            f"Circular dependency detected in query graph. "
            f"Remaining nodes: {remaining_nodes}"
        )
        super().__init__(detail)

    def __repr__(self) -> str:
        """Return a repr exposing structured attributes for REPL inspection."""
        return (
            f"CyclicDependencyError(remaining_nodes={self.remaining_nodes!r})"
        )


class SecurityError(Exception):
    """Raised when a security violation is detected.

    This includes SQL injection attempts, path traversal attacks,
    dangerous DuckDB table function calls, and SSRF attempts.

    """
