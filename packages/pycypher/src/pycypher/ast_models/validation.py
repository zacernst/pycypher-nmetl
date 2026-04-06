"""AST validation functions and utility helpers.

This module implements the validation pipeline that checks Cypher ASTs for
common issues: undefined/unused variables, missing labels, unreachable
conditions, contradictory comparisons, and expensive patterns. It also
provides convenience wrappers for AST conversion, traversal, and searching.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Set, cast

from pycypher.ast_models.clauses import (
    Create,
    Delete,
    Match,
    NodePattern,
    Query,
    RelationshipPattern,
    Return,
    ReturnAll,
    Unwind,
    With,
)
from pycypher.ast_models.core import (
    ASTNode,
    ValidationResult,
    ValidationSeverity,
)
from pycypher.ast_models.expressions import (
    BooleanLiteral,
    PropertyLookup,
    Variable,
)

if TYPE_CHECKING:
    from collections.abc import Iterator


def _collect_defined_variables(node: ASTNode) -> Set[str]:
    """Collect all variables defined in patterns and other binding contexts."""
    defined = set()

    # Variables from MATCH patterns
    for match in node.find_all(Match):
        if cast("Match", match).pattern:
            for path in getattr(cast("Match", match).pattern, "paths", []):
                # Node variables
                for elem in path.elements:
                    if (isinstance(elem, NodePattern) and elem.variable) or (
                        isinstance(elem, RelationshipPattern) and elem.variable
                    ):
                        defined.add(elem.variable.name)
                # Path variable
                if path.variable:
                    defined.add(path.variable.name)

    # Variables from CREATE patterns
    for create in node.find_all(Create):
        if cast("Create", create).pattern:
            for path in getattr(cast("Create", create).pattern, "paths", []):
                for elem in path.elements:
                    if (isinstance(elem, NodePattern) and elem.variable) or (
                        isinstance(elem, RelationshipPattern) and elem.variable
                    ):
                        defined.add(elem.variable.name)

    # Variables from UNWIND
    for unwind in node.find_all(Unwind):
        if cast("Unwind", unwind).alias:
            defined.add(cast("Unwind", unwind).alias)

    # Variables from WITH (creates new scope)
    for with_clause in node.find_all(With):
        for item in cast("With", with_clause).items:
            if item.alias:
                defined.add(item.alias)
            elif isinstance(item.expression, Variable):
                defined.add(item.expression.name)

    return defined


def _collect_definition_ids(node: ASTNode) -> Set[int]:
    """Collect IDs of Variable nodes that act as definitions."""
    ids = set()
    for match in node.find_all(Match):
        if cast("Match", match).pattern:
            for path in getattr(cast("Match", match).pattern, "paths", []):
                for elem in path.elements:
                    if (isinstance(elem, NodePattern) and elem.variable) or (
                        isinstance(elem, RelationshipPattern) and elem.variable
                    ):
                        ids.add(id(elem.variable))
                if path.variable:
                    ids.add(id(path.variable))
    for create in node.find_all(Create):
        if cast("Create", create).pattern:
            for path in getattr(cast("Create", create).pattern, "paths", []):
                for elem in path.elements:
                    if (isinstance(elem, NodePattern) and elem.variable) or (
                        isinstance(elem, RelationshipPattern) and elem.variable
                    ):
                        ids.add(id(elem.variable))
    return ids


def extract_referenced_variables(node: ASTNode) -> set[str]:
    """Extract all variable names referenced in an AST expression.

    Uses the AST's built-in ``find_all(Variable)`` visitor to collect variable
    names.  This traversal is complete (handles all AST node types) and
    depth-limited (raises :exc:`SecurityError` if nesting exceeds the
    configured maximum).

    For ``PropertyLookup`` nodes (e.g. ``p.name``), extracts the base variable
    name (``p``).

    This is the **canonical** variable extraction function -- all call sites
    that need variable names from expression ASTs should use this function
    rather than implementing ad-hoc traversal.

    Args:
        node: An AST node (typically a WHERE predicate or expression).

    Returns:
        Set of variable name strings.

    """
    refs: set[str] = set()
    for var in node.find_all(Variable):
        refs.add(var.name)
    return refs


def _collect_referenced_variables(node: ASTNode) -> Set[str]:
    """Collect all variables referenced in expressions."""
    referenced = set()
    definition_ids = _collect_definition_ids(node)

    for var in node.find_all(Variable):
        if id(var) not in definition_ids:
            referenced.add(cast("Variable", var).name)

    # Also check property lookups -- extract base variable from expression
    for prop in node.find_all(PropertyLookup):
        pl = cast("PropertyLookup", prop)
        expr = pl.expression
        if isinstance(expr, Variable):
            if id(expr) not in definition_ids:
                referenced.add(expr.name)

    return referenced


def _validate_undefined_variables(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for references to undefined variables."""
    if not isinstance(node, Query):
        return

    defined = _collect_defined_variables(node)
    referenced = _collect_referenced_variables(node)

    undefined = referenced - defined
    for var in sorted(undefined):
        result.add_issue(
            ValidationSeverity.ERROR,
            f"Variable '{var}' is used but never defined",
            suggestion=f"Add '{var}' to a MATCH or CREATE pattern, or define it with UNWIND/WITH",
            code="UNDEFINED_VAR",
        )


def _validate_unused_variables(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for variables that are defined but never used."""
    if not isinstance(node, Query):
        return

    defined = _collect_defined_variables(node)
    referenced = _collect_referenced_variables(node)

    # Also check what's returned
    returned = set()
    for ret in node.find_all(Return):
        for item in cast("Return", ret).items:
            if isinstance(item.expression, Variable):
                returned.add(item.expression.name)

    used = referenced | returned
    unused = defined - used

    for var in sorted(unused):
        result.add_issue(
            ValidationSeverity.WARNING,
            f"Variable '{var}' is defined but never used",
            suggestion=f"Remove '{var}' from the pattern or use it in WHERE/RETURN",
            code="UNUSED_VAR",
        )


def _validate_missing_labels(node: ASTNode, result: ValidationResult) -> None:
    """Check for MATCH patterns without labels (potential performance issue)."""
    for match in node.find_all(Match):
        if not cast("Match", match).pattern:
            continue

        for path in getattr(cast("Match", match).pattern, "paths", []):
            for elem in path.elements:
                if isinstance(elem, NodePattern):
                    if not elem.labels and not elem.properties:
                        var_name = (
                            elem.variable.name
                            if elem.variable
                            else "(anonymous)"
                        )
                        result.add_issue(
                            ValidationSeverity.WARNING,
                            f"Node pattern '{var_name}' has no labels or properties",
                            node_type="Match",
                            suggestion="Add a label to improve query performance via index usage",
                            code="MISSING_LABEL",
                        )


def _validate_unreachable_conditions(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for unreachable WHERE conditions (like WHERE false)."""
    for match in node.find_all(Match):
        if cast("Match", match).where:
            if isinstance(
                cast("Match", match).where,
                BooleanLiteral,
            ) and not getattr(cast("Match", match).where, "value", None):
                result.add_issue(
                    ValidationSeverity.WARNING,
                    "WHERE clause is always false - this query will never return results",
                    node_type="Match",
                    suggestion="Remove the WHERE false condition or fix the logic",
                    code="UNREACHABLE_MATCH",
                )

            # Check for contradictory literal comparisons
            _check_contradictory_comparisons(
                cast("Match", match).where,
                result,
            )


def _check_contradictory_comparisons(
    expr: Any,
    result: ValidationResult,
) -> None:
    """Recursively check for contradictory comparisons with literals."""
    # Check for Comparison class (not ComparisonExpression)
    if expr.__class__.__name__ == "Comparison":
        # Check if both sides are literals
        left_is_literal = expr.left.__class__.__name__ in (
            "IntegerLiteral",
            "FloatLiteral",
            "StringLiteral",
            "BooleanLiteral",
        )
        right_is_literal = expr.right.__class__.__name__ in (
            "IntegerLiteral",
            "FloatLiteral",
            "StringLiteral",
            "BooleanLiteral",
        )

        if left_is_literal and right_is_literal:
            left_val = expr.left.value
            right_val = expr.right.value
            op = expr.operator

            # Evaluate the comparison
            try:
                if op == ">":
                    always_false = not (left_val > right_val)
                elif op == "<":
                    always_false = not (left_val < right_val)
                elif op == ">=":
                    always_false = not (left_val >= right_val)
                elif op == "<=":
                    always_false = not (left_val <= right_val)
                elif op == "=":
                    always_false = left_val != right_val
                elif op == "<>":
                    always_false = left_val == right_val
                else:
                    always_false = False

                if always_false:
                    result.add_issue(
                        ValidationSeverity.ERROR,
                        f"Contradictory comparison: {left_val} {op} {right_val} is always false",
                        node_type="ComparisonExpression",
                        suggestion="Review the comparison logic",
                        code="CONTRADICTORY_COMPARISON",
                    )
            except (TypeError, ValueError):  # fmt: skip  # parens required for Python <3.14
                # Can't compare these types
                pass

    # Recursively check child expressions
    if hasattr(expr, "__dict__"):
        for attr_value in expr.__dict__.values():
            if isinstance(attr_value, list):
                for item in attr_value:
                    _check_contradictory_comparisons(item, result)
            elif hasattr(attr_value, "__dict__"):
                _check_contradictory_comparisons(attr_value, result)


def _validate_return_all_with_limit(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for RETURN * with LIMIT (may return unexpected results)."""
    for ret in node.find_all(Return):
        ret_clause = cast("Return", ret)
        has_return_all = any(
            isinstance(item.expression, ReturnAll) for item in ret_clause.items
        )
        if has_return_all and cast("Return", ret).limit:
            result.add_issue(
                ValidationSeverity.INFO,
                "Using RETURN * with LIMIT may return arbitrary results",
                node_type="Return",
                suggestion="Consider adding ORDER BY to make results deterministic",
                code="NONDETERMINISTIC_LIMIT",
            )


def _validate_delete_without_detach(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for DELETE of nodes that might have relationships."""
    for delete in node.find_all(Delete):
        if (
            not cast("Delete", delete).detach
            and cast("Delete", delete).expressions
        ):
            # Check if we're deleting node variables (not properties)
            for expr in cast("Delete", delete).expressions:
                if isinstance(expr, Variable):
                    result.add_issue(
                        ValidationSeverity.WARNING,
                        f"Deleting node '{expr.name}' without DETACH may fail if it has relationships",
                        node_type="Delete",
                        suggestion="Use DETACH DELETE to automatically remove relationships",
                        code="MISSING_DETACH",
                    )


def _validate_expensive_patterns(
    node: ASTNode,
    result: ValidationResult,
) -> None:
    """Check for potentially expensive query patterns."""
    # Check for multiple MATCH clauses without connecting variables
    matches = node.find_all(Match)
    if len(matches) >= 2:
        match_vars = []
        for match in matches:
            if cast("Match", match).pattern:
                vars_in_match = set()
                for path in getattr(cast("Match", match).pattern, "paths", []):
                    for elem in path.elements:
                        if (
                            isinstance(elem, NodePattern) and elem.variable
                        ) or (
                            isinstance(elem, RelationshipPattern)
                            and elem.variable
                        ):
                            vars_in_match.add(elem.variable.name)
                match_vars.append(vars_in_match)

        # Check if consecutive matches share variables
        for i in range(len(match_vars) - 1):
            if not match_vars[i] & match_vars[i + 1]:
                result.add_issue(
                    ValidationSeverity.WARNING,
                    "Multiple MATCH clauses with no shared variables may create a Cartesian product",
                    node_type="Match",
                    suggestion="Ensure MATCH patterns share variables or add WHERE conditions to connect them",
                    code="CARTESIAN_PRODUCT",
                )
                break  # Only report once

    # Check for variable-length paths without upper bound
    for rel in node.find_all(RelationshipPattern):
        if cast("RelationshipPattern", rel).length and getattr(
            cast("RelationshipPattern", rel).length,
            "unbounded",
            False,
        ):
            result.add_issue(
                ValidationSeverity.WARNING,
                "Unbounded variable-length relationship may cause performance issues",
                node_type="RelationshipPattern",
                suggestion="Add an upper bound to the relationship length (e.g., *1..10)",
                code="UNBOUNDED_RELATIONSHIP",
            )


# ============================================================================
# Utility Functions
# ============================================================================


def convert_ast(raw_ast: Any) -> ASTNode | None:
    """Convert a dictionary-based AST to typed Pydantic models.

    Args:
        raw_ast: Dictionary or other value from grammar parser

    Returns:
        Typed ASTNode or None

    Example:
        >>> from pycypher.grammar_parser import GrammarParser
        >>> parser = GrammarParser()
        >>> raw_ast = parser.parse_to_ast("MATCH (n) RETURN n")
        >>> typed_ast = convert_ast(raw_ast)
        >>> print(typed_ast.pretty())

    """
    from pycypher.ast_converter import ASTConverter as _ASTConverter

    converter = _ASTConverter()
    return converter.convert(raw_ast)


def traverse_ast(node: ASTNode) -> Iterator[ASTNode]:
    """Traverse an AST depth-first.

    Args:
        node: Root AST node

    Yields:
        Each node in the tree

    Example:
        >>> for ast_node in traverse_ast(typed_ast):
        ...     print(ast_node.__class__.__name__)

    """
    return node.traverse()


def find_nodes(node: ASTNode, node_type: type) -> list[ASTNode]:
    """Find all nodes of a specific type.

    Args:
        node: Root AST node
        node_type: Type to search for

    Returns:
        List of matching nodes

    Example:
        >>> matches = find_nodes(typed_ast, Match)
        >>> print(f"Found {len(matches)} MATCH clauses")

    """
    return node.find_all(node_type)


def print_ast(node: ASTNode, indent: int = 0) -> None:
    """Pretty print an AST.

    Args:
        node: AST node to print
        indent: Initial indentation level

    Example:
        >>> print_ast(typed_ast)

    """


# ============================================================================
# Validation Framework - More Cowbell!
# ============================================================================


def validate_ast(node: ASTNode, strict: bool = False) -> ValidationResult:
    """Validate an AST for common issues and anti-patterns.

    This function performs comprehensive validation including:
    - Undefined variable detection
    - Unreachable code detection
    - Performance anti-patterns
    - Type consistency checks

    Args:
        node: Root AST node to validate
        strict: If True, treat warnings as errors

    Returns:
        ValidationResult containing all issues found

    Example:
        >>> result = validate_ast(typed_ast)
        >>> if not result.is_valid:
        ...     print(result)
        ...     for error in result.errors:
        ...         print(f"Error: {error.message}")

    """
    result = ValidationResult()

    # Run all validators
    _validate_undefined_variables(node, result)
    _validate_unused_variables(node, result)
    _validate_missing_labels(node, result)
    _validate_unreachable_conditions(node, result)
    _validate_return_all_with_limit(node, result)
    _validate_delete_without_detach(node, result)
    _validate_expensive_patterns(node, result)

    # Convert warnings to errors in strict mode
    if strict:
        for issue in result.issues:
            if issue.severity == ValidationSeverity.WARNING:
                issue.severity = ValidationSeverity.ERROR

    return result
