"""Core AST infrastructure: base classes, enums, and validation framework.

This module provides the foundational types used by all other AST model
modules: enums (``RelationshipDirection``, ``JoinType``), the validation
framework (``ValidationSeverity``, ``ValidationIssue``, ``ValidationResult``),
and the abstract base classes ``ASTNode`` and ``Algebraizable``.
"""

from __future__ import annotations

import hashlib
import secrets
from abc import ABC
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


def random_hash() -> str:
    """Generate a cryptographically secure random hash string for column naming.

    Creates a unique identifier using cryptographically secure random bytes
    and SHA256 hashing. Used to generate collision-resistant column names
    during algebraic operations.

    Returns:
        str: A 64-character hexadecimal hash string (SHA256 digest).

    Security:
        - Uses secrets.token_bytes() for cryptographically secure randomness
        - Uses SHA256 instead of broken MD5 hash function
        - 32 bytes of entropy provide 2^256 possible values

    """
    # Generate 32 bytes of cryptographically secure random data
    # This provides 256 bits of entropy (2^256 possible values)
    random_bytes = secrets.token_bytes(32)

    # Use SHA256 instead of broken MD5
    return hashlib.sha256(random_bytes).hexdigest()


class RelationshipDirection(StrEnum):
    """Enumeration of relationship directions in graph patterns.

    Attributes:
        LEFT: Left-directed relationship (e.g., <-[:REL]-).
        RIGHT: Right-directed relationship (e.g., -[:REL]->).
        UNDIRECTED: Undirected relationship (e.g., -[:REL]-).

    """

    LEFT = "<-"
    RIGHT = "->"
    UNDIRECTED = "-"  # We won't support this right away


class JoinType(StrEnum):
    """Enumeration of supported SQL join types.

    Attributes:
        INNER: Inner join - returns only matching rows from both tables.
        LEFT: Left outer join - returns all rows from left table.
        RIGHT: Right outer join - returns all rows from right table.
        FULL: Full outer join - returns all rows from both tables.

    """

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


# ============================================================================
# Validation Framework
# ============================================================================


class ValidationSeverity(StrEnum):
    """Severity levels for validation issues."""

    ERROR = "error"  # Query will likely fail or produce wrong results
    WARNING = "warning"  # Query will work but may have performance/correctness issues
    INFO = "info"  # Suggestions for improvement


class ValidationIssue(BaseModel):
    """A single validation issue found in the AST."""

    severity: ValidationSeverity
    message: str
    node_type: Any | None = None  # Can be string or ASTNode
    suggestion: Any | None = None  # Can be string or other types
    node: Any | None = None  # For compatibility with tests
    code: str | None = None  # For compatibility with tests

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __init__(
        self,
        severity: ValidationSeverity | dict | None = None,
        message: str | None = None,
        node: Any | None = None,
        code: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ValidationIssue with positional or keyword arguments."""
        # If first argument is dict and message is None, Pydantic is calling with dict
        match severity:
            case dict():
                super().__init__(**severity)
            case _ if severity is not None or message is not None:
                # Positional or mixed arguments - convert to kwargs
                if severity is not None:
                    kwargs["severity"] = severity
                if message is not None:
                    kwargs["message"] = message
                if node is not None:
                    kwargs["node"] = node
                if code is not None:
                    kwargs["code"] = code
                super().__init__(**kwargs)
            case _:
                # All keyword arguments
                super().__init__(**kwargs)

    def __str__(self) -> str:
        parts = [f"[{self.severity.value.upper()}] {self.message}"]
        if self.node_type:
            parts.append(f" (in {self.node_type})")
        if self.suggestion:
            parts.append(f"\n  💡 {self.suggestion}")
        return "".join(parts)

    def __repr__(self) -> str:
        """String representation for debugging."""
        node_info = f" in {self.node.__class__.__name__}" if self.node else ""
        code_info = f" [{self.code}]" if self.code else ""
        return f"{self.severity.value.upper()}{code_info}: {self.message}{node_info}"


class ValidationResult(BaseModel):
    """Result of validating an AST."""

    issues: list[ValidationIssue] = Field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Check if there are any error-level issues."""
        return any(
            issue.severity == ValidationSeverity.ERROR for issue in self.issues
        )

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warning-level issues."""
        return any(
            issue.severity == ValidationSeverity.WARNING
            for issue in self.issues
        )

    @property
    def is_valid(self) -> bool:
        """Check if AST is valid (no errors)."""
        return not self.has_errors

    @property
    def errors(self) -> list[ValidationIssue]:
        """Get only error-level issues."""
        return [
            i for i in self.issues if i.severity == ValidationSeverity.ERROR
        ]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Get only warning-level issues."""
        return [
            i for i in self.issues if i.severity == ValidationSeverity.WARNING
        ]

    @property
    def infos(self) -> list[ValidationIssue]:
        """Get only info-level issues."""
        return [
            i for i in self.issues if i.severity == ValidationSeverity.INFO
        ]

    def add_issue(
        self,
        severity: ValidationSeverity,
        message: str,
        node_type: str | None = None,
        suggestion: str | None = None,
        node: Any | None = None,
        code: str | None = None,
    ) -> None:
        """Add a validation issue with the given severity.

        General-purpose method for recording validation issues. For
        convenience, use ``add_error``, ``add_warning``, or ``add_info``
        to add issues at a predetermined severity level.

        Args:
            severity: The severity level of the issue.
            message: Human-readable description of the validation problem.
            node_type: Optional AST node type name where the issue occurred.
            suggestion: Optional suggested fix or corrective action.
            node: Optional reference to the AST node that triggered the issue.
            code: Optional machine-readable issue identifier for programmatic
                handling.

        """
        self.issues.append(
            ValidationIssue(
                severity=severity,
                message=message,
                node_type=node_type,
                suggestion=suggestion,
                node=node,
                code=code,
            ),
        )

    def add_error(
        self,
        message: str,
        node_type: str | None = None,
        suggestion: str | None = None,
        node: Any | None = None,
        code: str | None = None,
    ) -> None:
        """Add an error-level validation issue.

        Convenience wrapper around :meth:`add_issue` that sets severity to
        :attr:`ValidationSeverity.ERROR`. Errors indicate problems that make
        the query invalid and must be resolved before execution.

        Args:
            message: Human-readable description of the validation error.
            node_type: Optional AST node type name where the error occurred.
            suggestion: Optional suggested fix or corrective action.
            node: Optional reference to the AST node that triggered the error.
            code: Optional machine-readable issue identifier for programmatic
                handling.

        """
        self.issues.append(
            ValidationIssue(
                severity=ValidationSeverity.ERROR,
                message=message,
                node_type=node_type,
                suggestion=suggestion,
                node=node,
                code=code,
            ),
        )

    def add_warning(
        self,
        message: str,
        node_type: str | None = None,
        suggestion: str | None = None,
        node: Any | None = None,
        code: str | None = None,
    ) -> None:
        """Add a warning-level validation issue.

        Convenience wrapper around :meth:`add_issue` that sets severity to
        :attr:`ValidationSeverity.WARNING`. Warnings indicate potential
        problems that do not prevent execution but may produce unexpected
        results.

        Args:
            message: Human-readable description of the validation warning.
            node_type: Optional AST node type name where the warning occurred.
            suggestion: Optional suggested fix or corrective action.
            node: Optional reference to the AST node that triggered the warning.
            code: Optional machine-readable issue identifier for programmatic
                handling.

        """
        self.issues.append(
            ValidationIssue(
                severity=ValidationSeverity.WARNING,
                message=message,
                node_type=node_type,
                suggestion=suggestion,
                node=node,
                code=code,
            ),
        )

    def add_info(
        self,
        message: str,
        node_type: str | None = None,
        suggestion: str | None = None,
        node: Any | None = None,
        code: str | None = None,
    ) -> None:
        """Add an informational validation issue.

        Convenience wrapper around :meth:`add_issue` that sets severity to
        :attr:`ValidationSeverity.INFO`. Informational issues are
        non-actionable observations about query structure, useful for
        debugging or optimization hints.

        Args:
            message: Human-readable description of the observation.
            node_type: Optional AST node type name relevant to the observation.
            suggestion: Optional suggested improvement or optimization.
            node: Optional reference to the AST node related to the observation.
            code: Optional machine-readable issue identifier for programmatic
                handling.

        """
        self.issues.append(
            ValidationIssue(
                severity=ValidationSeverity.INFO,
                message=message,
                node_type=node_type,
                suggestion=suggestion,
                node=node,
                code=code,
            ),
        )

    def __bool__(self) -> bool:
        """True if validation passed (no errors)."""
        return self.is_valid

    def __str__(self) -> str:
        if not self.issues:
            return "Validation passed: No issues found"

        status = "passed" if self.is_valid else "failed"
        lines = [f"Validation {status} with {len(self.issues)} issue(s):"]
        for i, issue in enumerate(self.issues, 1):
            lines.append(f"  {i}. {issue}")
        return "\n".join(lines)


# ============================================================================
# Base Classes
# ============================================================================


class ASTNode(BaseModel, ABC):
    """Base class for all AST nodes."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def validate_ast(self, strict: bool = False) -> ValidationResult:
        """Validate this AST node and its children.

        Args:
            strict: If True, treat warnings as errors.

        Returns:
            ValidationResult containing verification details.

        """
        from pycypher.ast_models.validation import validate_ast

        return validate_ast(self, strict)

    def traverse(self, depth: int = 0) -> Iterator[ASTNode]:
        """Traverse the AST depth-first, yielding all nodes.

        Args:
            depth: Current depth in the tree (used internally).

        Yields:
            ASTNode: Each node in the tree.

        Raises:
            SecurityError: If traversal exceeds ``MAX_QUERY_NESTING_DEPTH``.

        """
        from pycypher.config import MAX_QUERY_NESTING_DEPTH

        if depth > MAX_QUERY_NESTING_DEPTH:
            from pycypher.exceptions import SecurityError

            msg = (
                f"AST traversal exceeded maximum nesting depth "
                f"({MAX_QUERY_NESTING_DEPTH}). The query is too deeply "
                f"nested. Adjust PYCYPHER_MAX_QUERY_NESTING_DEPTH to increase."
            )
            raise SecurityError(msg)

        yield self
        for child in self._get_children():
            if child is not None:
                yield from child.traverse(depth + 1)

    def find_all(
        self,
        predicate: type | Callable[[ASTNode], bool],
    ) -> list[ASTNode]:
        """Find all nodes matching a predicate or type.

        Args:
            predicate: Either a type to match or a callable predicate function

        Returns:
            List of matching nodes

        """
        if callable(predicate) and not isinstance(predicate, type):
            return [node for node in self.traverse() if predicate(node)]
        return [
            node for node in self.traverse() if isinstance(node, predicate)
        ]

    def find_first(
        self,
        predicate: type | Callable[[ASTNode], bool],
    ) -> ASTNode | None:
        """Find the first node matching a predicate or type.

        Args:
            predicate: Either a type to match or a callable predicate function

        Returns:
            First matching node or None

        """
        if callable(predicate) and not isinstance(predicate, type):
            for node in self.traverse():
                if predicate(node):
                    return node
        else:
            for node in self.traverse():
                if isinstance(node, predicate):
                    return node
        return None

    def _get_children(self) -> list[ASTNode]:
        """Get all child nodes. Override in subclasses."""
        children: list[ASTNode] = []

        def collect(value: Any) -> None:
            """Recursively collect ASTNode children from a field value."""
            match value:
                case ASTNode():
                    children.append(value)
                case list() | tuple():
                    for item in value:
                        collect(item)
                case dict():
                    for item in value.values():
                        collect(item)

        for field_value in self.__dict__.values():
            collect(field_value)

        return children

    def pretty(self, indent: int = 0) -> str:
        """Pretty print the AST node.

        Args:
            indent: Current indentation level

        Returns:
            Formatted string representation

        """
        prefix = "  " * indent
        lines = [f"{prefix}{self.__class__.__name__}"]

        for field_name, field_value in self.__dict__.items():
            match field_value:
                case None:
                    continue
                case ASTNode():
                    lines.append(f"{prefix}  {field_name}:")
                    lines.append(field_value.pretty(indent + 2))
                case list() if field_value:
                    if all(isinstance(item, ASTNode) for item in field_value):
                        lines.append(f"{prefix}  {field_name}: [")
                        for item in field_value:
                            lines.append(item.pretty(indent + 2))
                        lines.append(f"{prefix}  ]")
                    else:
                        lines.append(f"{prefix}  {field_name}: {field_value}")
                case _ if (
                    not isinstance(field_value, (dict, list)) or field_value
                ):
                    lines.append(f"{prefix}  {field_name}: {field_value}")

        return "\n".join(lines)

    def to_dict(self) -> dict[str, Any]:
        """Convert back to dictionary representation."""
        result: dict[str, Any] = {"type": self.__class__.__name__}

        for field_name, field_value in self.__dict__.items():
            match field_value:
                case None:
                    continue
                case ASTNode():
                    result[field_name] = field_value.to_dict()
                case list():
                    result[field_name] = [
                        item.to_dict() if isinstance(item, ASTNode) else item
                        for item in field_value
                    ]
                case _:
                    result[field_name] = field_value

        return result

    def clone(self) -> ASTNode:
        """Create a deep copy of this node."""
        return self.__class__(**self.to_dict())

    def cypher_validate(self) -> ValidationResult:
        """Validate this AST node and return any issues found.

        Runs a suite of validators to check for:
        - Undefined variable references
        - Unused variables
        - Missing labels (performance issues)
        - Unreachable conditions
        - Type mismatches
        - And other common query anti-patterns

        Returns:
            ValidationResult containing all issues found

        Example:
            >>> result = query.cypher_validate()
            >>> if not result.is_valid:
            ...     print(result)

        """
        from pycypher.ast_models.validation import (
            _validate_delete_without_detach,
            _validate_expensive_patterns,
            _validate_missing_labels,
            _validate_return_all_with_limit,
            _validate_undefined_variables,
            _validate_unreachable_conditions,
            _validate_unused_variables,
        )

        result = ValidationResult()

        # Run all validators
        _validate_undefined_variables(self, result)
        _validate_unused_variables(self, result)
        _validate_missing_labels(self, result)
        _validate_unreachable_conditions(self, result)
        _validate_return_all_with_limit(self, result)
        _validate_delete_without_detach(self, result)
        _validate_expensive_patterns(self, result)

        return result


class Algebraizable(ASTNode):
    """AST node that can be translated into a relational algebra operator.

    Subclasses (PatternPath, NodePattern, RelationshipPattern, PatternIntersection)
    implement a ``to_relation()`` method consumed by the STAR translator to build
    the query execution plan.
    """
