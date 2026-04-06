"""Cypher expression AST nodes.

This module defines all expression types: literals, variables, operators,
property access, function calls, comprehensions, quantifiers, CASE, and
other expression constructs.
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from pydantic import Field

from pycypher.ast_models.core import ASTNode

if TYPE_CHECKING:
    from pycypher.ast_models.clauses import Pattern, Query


# ============================================================================
# Expressions
# ============================================================================


class Expression(ASTNode, ABC):
    """Abstract base for all Cypher expression nodes.

    Expressions produce values when evaluated against a binding table row.
    Subclasses cover literals, variables, operators, property access, function
    calls, and advanced constructs (comprehensions, quantifiers, CASE, REDUCE).
    """


class BinaryExpression(Expression, ABC):
    """Abstract base for expressions with a left operand, operator, and right operand.

    Logical operators (Or, And, Xor) additionally use an *operands* list to
    represent chains of the same operator without deep nesting.
    """

    operator: str
    left: Expression | None = None
    right: Expression | None = None


class Or(BinaryExpression):
    """Logical OR -- true when any operand is true (Kleene three-valued logic)."""

    operands: list[Expression] = Field(default_factory=list)
    operator: str = "OR"


class Xor(BinaryExpression):
    """Logical XOR -- true when exactly one operand is true (Kleene three-valued logic)."""

    operands: list[Expression] = Field(default_factory=list)
    operator: str = "XOR"


class And(BinaryExpression):
    """Logical AND -- true when all operands are true (Kleene three-valued logic)."""

    operands: list[Expression] = Field(default_factory=list)
    operator: str = "AND"


class Not(Expression):
    """Logical NOT -- negates its operand (Kleene three-valued logic: NOT NULL = NULL)."""

    operand: Expression | None = None


class Comparison(BinaryExpression):
    """Comparison expression (=, <>, <, >, <=, >=)."""


class StringPredicate(BinaryExpression):
    """String predicate (STARTS WITH, ENDS WITH, CONTAINS, =~, IN)."""


class NullCheck(Expression):
    """IS NULL or IS NOT NULL check."""

    operator: str  # "IS NULL" or "IS NOT NULL"
    operand: Expression | None = None


class LabelPredicate(Expression):
    """Label predicate check: ``n:Person`` or ``n:Person:Employee``.

    Returns ``True`` when the node bound to *operand* has all the specified
    labels, ``False`` otherwise.  In the current single-label entity model,
    ``n:A:B`` (two different labels) always returns ``False``.

    Attributes:
        operand: The expression being tested (typically a ``Variable``).
        labels: One or more label names that must all match.

    """

    operand: Expression | None = None
    labels: list[str] = []


class Arithmetic(BinaryExpression):
    r"""Arithmetic expression (+, -, \*, /, %, ^)."""


class Unary(Expression):
    """Unary expression (+, -)."""

    operator: str
    operand: Expression | None = None


# ============================================================================
# Property Access and Indexing
# ============================================================================


class PropertyLookup(Expression):
    """Property access (e.g., n.name).

    .. deprecated::
        The ``variable`` field is deprecated. Use ``expression`` instead.
    """

    expression: Expression | None = None
    property: str | None = None
    # Legacy field for backward compatibility - deprecated, use expression instead
    variable: Variable | None = None

    def model_post_init(self, __context: Any) -> None:
        """Emit deprecation warning when legacy ``variable`` field is used."""
        super().model_post_init(__context)
        if self.variable is not None:
            from shared.deprecation import emit_deprecation

            emit_deprecation(
                "PropertyLookup.variable",
                since="0.0.19",
                removed_in="0.1.0",
                alternative="PropertyLookup.expression",
            )


class IndexLookup(Expression):
    """Array/string indexing (e.g., list[0])."""

    expression: Expression | None = None
    index: Expression | None = None


class Slicing(Expression):
    """Array/string slicing (e.g., list[1..3])."""

    expression: Expression | None = None
    start: Expression | None = None
    end: Expression | None = None


# ============================================================================
# Literals
# ============================================================================


class Literal(Expression, ABC):
    """Base class for literal values."""

    value: Any

    def evaluate(self) -> Any:
        """Evaluate the literal to its value."""
        return self.value


class IntegerLiteral(Literal):
    """Integer literal."""

    value: int


class FloatLiteral(Literal):
    """Float literal."""

    value: float


class StringLiteral(Literal):
    """String literal."""

    value: str


class BooleanLiteral(Literal):
    """Boolean literal."""

    value: bool


class NullLiteral(Literal):
    """NULL literal."""

    value: None = None


class ListLiteral(Literal):
    """List literal."""

    value: list[Any] = Field(default_factory=list)
    elements: list[Expression] = Field(default_factory=list)


class MapLiteral(Literal):
    """Map literal."""

    value: dict[str, Any] = Field(default_factory=dict)
    entries: dict[str, Expression] = Field(default_factory=dict)


# ============================================================================
# Variables and Parameters
# ============================================================================


class Variable(Expression):
    """Variable reference in Cypher queries.

    Represents a variable name used in expressions, patterns, and bindings.
    Variables are used throughout the AST to reference nodes, relationships,
    paths, and other values.

    Attributes:
        name: The variable name (e.g., 'n', 'person', 'rel')

    Note:
        As of the latest refactoring, all variable references in the AST
        (including in patterns, comprehensions, and other binding contexts)
        are represented as Variable instances, not plain strings. This ensures
        consistency throughout the AST structure.

    Examples:
        >>> # In patterns
        >>> node = NodePattern(variable=Variable(name="n"), labels=["Person"])
        >>>
        >>> # In expressions
        >>> return_item = ReturnItem(expression=Variable(name="n"))
        >>>
        >>> # In comprehensions
        >>> comp = ListComprehension(variable=Variable(name="x"), ...)

    """

    name: str

    def __hash__(self) -> int:
        """Hash based on variable name."""
        return hash("__VARIABLE__" + self.name)


class Parameter(Expression):
    """Query parameter ($param)."""

    name: str


# ============================================================================
# Functions
# ============================================================================


class FunctionInvocation(Expression):
    """Scalar or aggregation function call (e.g., ``toUpper(n.name)``, ``count(*)``).

    The *name* field may be a plain string or a namespace dict for qualified
    calls like ``date.truncate``. Use the :attr:`function_name` property to
    get the normalised string form for registry lookup.
    """

    name: str | dict[str, str]  # Simple name or {namespace, name}
    arguments: dict[str, Any] | list[ASTNode | None] | None = None
    distinct: bool = False

    @property
    def function_name(self) -> str:
        """Return the normalised function name as a plain string.

        ``name`` may be either a bare string (e.g. ``"toUpper"``) or a
        namespace dict produced by the grammar for qualified calls such as
        ``date.truncate`` (represented as ``{"namespace": "date",
        "name": "truncate"}``).  This property returns the qualified name
        ``"date.truncate"`` so the scalar-function registry can look it up
        directly.
        """
        if isinstance(self.name, dict):
            namespace = self.name.get("namespace", "")
            bare = self.name.get("name", "")
            return f"{namespace}.{bare}" if namespace else bare
        return self.name


class CountStar(Expression):
    """COUNT(*) function."""


# ============================================================================
# Advanced Expressions
# ============================================================================


class Exists(Expression):
    """EXISTS subquery -- tests whether a pattern or subquery produces any rows.

    Returns ``True`` if *content* (a :class:`Pattern` or :class:`Query`)
    matches at least one result, ``False`` otherwise. Evaluated using batch
    semantics in the binding evaluator.
    """

    content: Pattern | Query | None = None


class ListComprehension(Expression):
    """List comprehension expression: [x IN list WHERE pred | expr].

    Represents Cypher list comprehension syntax for transforming lists.

    Attributes:
        variable: Variable instance for the iteration variable (e.g., Variable(name="x"))
        list_expr: Expression that evaluates to a list
        where: Optional filter predicate
        map_expr: Optional transformation expression

    Example:
        >>> # [x IN [1,2,3] WHERE x > 1 | x * 2]
        >>> comp = ListComprehension(
        ...     variable=Variable(name="x"),
        ...     list_expr=ListLiteral(value=[1,2,3]),
        ...     where=Comparison(operator=">", left=Variable(name="x"), right=IntegerLiteral(value=1)),
        ...     map_expr=Arithmetic(operator="*", left=Variable(name="x"), right=IntegerLiteral(value=2))
        ... )

    """

    variable: Variable | None = None
    list_expr: Expression | None = None
    where: Expression | None = None
    map_expr: Expression | None = None


class PatternComprehension(Expression):
    """Pattern comprehension expression: [path = pattern WHERE pred | expr].

    Represents Cypher pattern comprehension for matching and transforming graph patterns.

    Attributes:
        variable: Variable instance for the path binding (e.g., Variable(name="path"))
        pattern: Graph pattern to match
        where: Optional filter predicate
        map_expr: Optional transformation expression

    Example:
        >>> # [p = (a)-[:KNOWS]->(b) WHERE b.age > 30 | b.name]
        >>> comp = PatternComprehension(
        ...     variable=Variable(name="p"),
        ...     pattern=Pattern(...),
        ...     where=Comparison(...),
        ...     map_expr=PropertyLookup(...)
        ... )

    """

    variable: Variable | None = None
    pattern: Pattern | None = None
    where: Expression | None = None
    map_expr: Expression | None = None


class MapProjection(Expression):
    """Map projection expression: node{.prop, computed: expr}.

    Projects a map/object with selected or computed properties.

    Attributes:
        variable: Variable instance for the source object (e.g., Variable(name="node"))
        elements: List of MapElement items defining projections
        include_all: Whether to include all properties (.* syntax)

    Example:
        >>> # person{.name, .age, adult: person.age >= 18}
        >>> proj = MapProjection(
        ...     variable=Variable(name="person"),
        ...     elements=[
        ...         MapElement(property="name"),
        ...         MapElement(property="age"),
        ...         MapElement(property="adult", expression=Comparison(...))
        ...     ]
        ... )

    """

    variable: Variable | None = None
    elements: list[MapElement] = Field(default_factory=list)
    include_all: bool = False


class MapElement(ASTNode):
    """Element in map projection."""

    property: str | None = None
    expression: Expression | None = None
    all_properties: bool = False


class CaseExpression(Expression):
    """CASE expression -- conditional value selection.

    When *expression* is set (simple form), each WHEN condition is compared to it
    for equality. When *expression* is None (searched form), each WHEN condition
    is evaluated as a boolean. The first matching WHEN returns its result;
    *else_expr* provides a fallback (NULL if absent).
    """

    expression: Expression | None = None  # For simple CASE
    when_clauses: list[WhenClause] = Field(default_factory=list)
    else_expr: Expression | None = None


class WhenClause(ASTNode):
    """WHEN clause in CASE expression."""

    condition: Expression | None = None
    result: Expression | None = None


class Reduce(Expression):
    """REDUCE expression for list aggregation.

    Iterates over a list, accumulating values using a custom expression.

    Attributes:
        accumulator: Variable instance for the accumulator (e.g., Variable(name="sum"))
        initial: Initial value expression for the accumulator
        variable: Variable instance for the iteration variable (e.g., Variable(name="x"))
        list_expr: Expression that evaluates to a list
        map_expr: Expression to compute new accumulator value

    Example:
        >>> # REDUCE(sum = 0, x IN [1,2,3] | sum + x)
        >>> reduce_expr = Reduce(
        ...     accumulator=Variable(name="sum"),
        ...     initial=IntegerLiteral(value=0),
        ...     variable=Variable(name="x"),
        ...     list_expr=ListLiteral(value=[1,2,3]),
        ...     map_expr=Arithmetic(operator="+", left=Variable(name="sum"), right=Variable(name="x"))
        ... )

    """

    accumulator: Variable | None = None
    initial: Expression | None = None
    variable: Variable | None = None
    list_expr: Expression | None = None
    map_expr: Expression | None = None


class Quantifier(Expression):
    """Quantifier expression: ALL, ANY, NONE, SINGLE.

    Tests whether a predicate holds for elements in a list.

    Attributes:
        quantifier: Type of quantifier ("ALL", "ANY", "NONE", "SINGLE")
        variable: Variable instance for the iteration variable (e.g., Variable(name="x"))
        list_expr: Expression that evaluates to a list
        where: Predicate expression to test

    Examples:
        >>> # ALL(x IN [1,2,3] WHERE x > 0)
        >>> all_expr = Quantifier(
        ...     quantifier="ALL",
        ...     variable=Variable(name="x"),
        ...     list_expr=ListLiteral(value=[1,2,3]),
        ...     where=Comparison(operator=">", left=Variable(name="x"), right=IntegerLiteral(value=0))
        ... )
        >>>
        >>> # ANY(x IN list WHERE x = value)
        >>> any_expr = Quantifier(
        ...     quantifier="ANY",
        ...     variable=Variable(name="x"),
        ...     list_expr=Variable(name="list"),
        ...     where=Comparison(operator="=", left=Variable(name="x"), right=Variable(name="value"))
        ... )

    """

    quantifier: str  # "ALL", "ANY", "NONE", "SINGLE"
    variable: Variable | None = None
    list_expr: Expression | None = None
    where: Expression | None = None


# ============================================================================
# Special Functions
# ============================================================================


class ShortestPath(Expression):
    """SHORTESTPATH function."""

    pattern: Pattern | None = None


class AllShortestPaths(Expression):
    """ALLSHORTESTPATHS function."""

    pattern: Pattern | None = None
