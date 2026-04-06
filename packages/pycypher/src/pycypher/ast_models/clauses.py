"""Cypher clause and pattern AST nodes.

This module defines the AST node types for Cypher clauses (MATCH, RETURN,
CREATE, etc.) and graph patterns (NodePattern, RelationshipPattern, etc.).
"""

from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from pydantic import Field

from pycypher.ast_models.core import Algebraizable, ASTNode

if TYPE_CHECKING:
    from pycypher.ast_models.expressions import Expression, Variable


# ============================================================================
# Query Structure
# ============================================================================


class Clause(ASTNode, ABC):
    """Abstract base for Cypher query clauses.

    A clause is a top-level statement component (MATCH, RETURN, WITH, CREATE,
    etc.) that reads or modifies the binding table during query execution.
    """


class Query(ASTNode):
    """Root node of a parsed Cypher query.

    Contains the ordered list of clauses that make up the query pipeline
    (e.g., MATCH -> WHERE -> WITH -> RETURN). Clauses are executed sequentially,
    with each clause consuming and producing a binding table.
    """

    clauses: list[Clause] = Field(default_factory=list)


class UnionQuery(ASTNode):
    """Two or more queries joined by UNION [ALL].

    ``statements`` is the ordered list of component :class:`Query` objects.
    ``all_flags`` is a parallel list of booleans -- ``all_flags[i]`` is
    ``True`` when the join between ``statements[i]`` and ``statements[i+1]``
    uses ``UNION ALL`` (no deduplication).
    """

    statements: list[Query] = Field(default_factory=list)
    all_flags: list[bool] = Field(default_factory=list)


class PatternIntersection(Algebraizable):
    """Intersection of multiple Patterns, implicit Join on shared variables."""

    pattern_list: list[Algebraizable]


# ============================================================================
# Reading Clauses
# ============================================================================


class Match(Clause):
    """MATCH clause -- binds graph patterns to the binding table.

    Searches the graph for subgraphs matching *pattern* and adds one row per
    match. When *optional* is True (OPTIONAL MATCH), rows with no match are
    retained with NULL-filled columns. An optional *where* expression filters
    matches before they enter the binding table.
    """

    optional: bool = False
    pattern: Pattern | None = None
    where: Expression | None = None


class Unwind(Clause):
    """UNWIND clause -- explodes a list into one row per element.

    Evaluates *expression* (which must yield a list) and produces one output
    row per element, binding each element to *alias*.
    """

    expression: Expression | None = None
    alias: str | None = None


class Call(Clause):
    """CALL clause -- invokes a registered procedure.

    Calls the procedure identified by *procedure_name* with *arguments*,
    and optionally binds selected output columns via *yield_items*.
    A *where* filter may further restrict the yielded rows.
    """

    procedure_name: str | None = None
    arguments: list[Expression] = Field(default_factory=list)
    yield_items: list[YieldItem] = Field(default_factory=list)
    where: Expression | None = None


class YieldItem(ASTNode):
    """YIELD item in CALL clause."""

    variable: Variable | None = None
    alias: str | None = None


# ============================================================================
# Projection Clauses
# ============================================================================


class Return(Clause):
    """RETURN clause -- specifies the query output.

    Projects expressions from the binding table into the result set, analogous
    to SQL SELECT. Supports DISTINCT deduplication, ORDER BY sorting, and
    SKIP/LIMIT pagination.
    """

    distinct: bool = False
    items: list[ReturnItem] = Field(default_factory=list)
    order_by: list[OrderByItem] | None = None
    #: Integer literal or an :class:`Expression` AST node (e.g. ``Parameter``).
    skip: Any | None = None
    #: Integer literal or an :class:`Expression` AST node (e.g. ``Parameter``).
    limit: Any | None = None


class With(Clause):
    """WITH clause -- intermediate projection and scope boundary.

    Acts as a pipeline stage between clauses, projecting selected expressions
    into the downstream binding table. Supports the same modifiers as RETURN
    (DISTINCT, ORDER BY, SKIP, LIMIT) plus an optional WHERE filter. Variables
    not projected by WITH are not visible to subsequent clauses.
    """

    distinct: bool = False
    items: list[ReturnItem] = Field(default_factory=list)
    where: Expression | None = None
    order_by: list[OrderByItem] | None = None
    #: Integer literal or an :class:`Expression` AST node (e.g. ``Parameter``).
    skip: Any | None = None
    #: Integer literal or an :class:`Expression` AST node (e.g. ``Parameter``).
    limit: Any | None = None


class ReturnItem(ASTNode):
    """Item in RETURN or WITH clause."""

    expression: Expression | None = None
    alias: str | None = None


class OrderByItem(ASTNode):
    """ORDER BY item.

    ``nulls_placement`` controls where null values appear in the sorted output:
    ``"first"`` puts nulls before non-nulls; ``"last"`` (or ``None``) puts them
    after -- matching Neo4j's default NULLS LAST behaviour.
    """

    expression: Expression | None = None
    ascending: bool = True
    nulls_placement: str | None = None  # "first", "last", or None (= last)


class ReturnAll(ASTNode):
    r"""RETURN \* or WITH \*."""


# ============================================================================
# Writing Clauses
# ============================================================================


class Create(Clause):
    """CREATE clause -- inserts new nodes and relationships into the graph.

    Creates the entities described by *pattern*. If a variable in the pattern
    is already bound, the existing entity is reused; otherwise a new entity is
    created with any specified labels and properties.
    """

    pattern: Pattern | None = None


class Merge(Clause):
    """MERGE clause -- creates a pattern only if it does not already exist.

    Performs an upsert: if *pattern* matches existing data, the match is bound;
    otherwise the pattern is created. Optional *on_create* and *on_match* SET
    actions run conditionally depending on whether a new entity was created.
    """

    pattern: Pattern | None = None
    on_create: list[SetItem] | None = None
    on_match: list[SetItem] | None = None


class Delete(Clause):
    """DELETE clause -- removes nodes or relationships from the graph.

    Deletes the entities referenced by *expressions*. When *detach* is True
    (DETACH DELETE), any relationships connected to deleted nodes are
    automatically removed first.
    """

    detach: bool = False
    expressions: list[Expression] = Field(default_factory=list)


class Set(Clause):
    """SET clause -- modifies properties or labels on existing entities.

    Contains a list of :class:`SetItem` subclasses that specify individual
    property assignments, label additions, or bulk property replacements.
    All mutations within a single SET are applied atomically per row.
    """

    items: list[SetItem] = Field(default_factory=list)


class SetItem(ASTNode):
    """Base class for items in SET clause."""

    variable: Variable | None = None
    property: str | None = None
    expression: Expression | None = None
    labels: list[str] = Field(default_factory=list)


class SetPropertyItem(SetItem):
    """SET n.property = value item."""

    variable: Variable
    property: str
    value: Expression


class SetLabelsItem(SetItem):
    """SET n:Label item."""

    variable: Variable
    labels: list[str]


class SetAllPropertiesItem(SetItem):
    """SET n = {map} item."""

    variable: Variable
    properties: Expression


class AddAllPropertiesItem(SetItem):
    """SET n += {map} item."""

    variable: Variable
    properties: Expression


class Remove(Clause):
    """REMOVE clause -- removes properties or labels from existing entities.

    Contains a list of :class:`RemoveItem` entries specifying which properties
    or labels to strip from the referenced variables.
    """

    items: list[RemoveItem] = Field(default_factory=list)


class RemoveItem(ASTNode):
    """Item in REMOVE clause."""

    variable: Variable | None = None
    property: str | None = None
    labels: list[str] = Field(default_factory=list)


class Foreach(Clause):
    """FOREACH clause -- iterative list mutation.

    Iterates over every element of *list_expression*, binds it to *variable*,
    and executes the inner *clauses* (SET, CREATE, MERGE, DELETE, REMOVE) for
    each element.  This is a side-effect-only construct; *variable* does not
    escape into the outer query scope.
    """

    variable: str | None = None
    list_expression: Expression | None = None
    clauses: list[Clause] = Field(default_factory=list)


# ============================================================================
# Patterns
# ============================================================================


class Pattern(ASTNode):
    """Pattern containing path components."""

    paths: list[PatternPath] = Field(default_factory=list)


class PatternPath(Algebraizable):
    """A single path in a pattern.

    Represents a complete path pattern that may have an optional binding variable.
    For example: p = (a)-[r]->(b)

    Attributes:
        variable: Variable instance for path binding (e.g., Variable(name="p")), optional
        elements: List of NodePattern and RelationshipPattern elements forming the path

    Example:
        >>> path = PatternPath(
        ...     variable=Variable(name="path"),
        ...     elements=[
        ...         NodePattern(variable=Variable(name="a")),
        ...         RelationshipPattern(variable=Variable(name="r")),
        ...         NodePattern(variable=Variable(name="b"))
        ...     ]
        ... )

    """

    variable: Variable | None = None
    elements: list[NodePattern | RelationshipPattern] = Field(
        default_factory=list,
    )
    #: ``"none"`` for normal paths, ``"one"`` for ``shortestPath``,
    #: ``"all"`` for ``allShortestPaths``.
    shortest_path_mode: str = "none"


class NodePattern(Algebraizable):
    """Node pattern in MATCH/CREATE clauses.

    Represents a node pattern like (n:Person {name: 'Alice'}) in Cypher queries.

    Attributes:
        variable: Variable instance representing the node's binding name (e.g., Variable(name="n"))
        labels: List of label names applied to this node
        properties: Property map as dict (optional)

    Example:
        >>> node = NodePattern(
        ...     variable=Variable(name="person"),
        ...     labels=["Person"],
        ...     properties={"name": "Alice"}
        ... )
        >>> print(node.variable.name)  # "person"

    """

    variable: Variable | None = None
    labels: list[str] = Field(default_factory=list)
    properties: dict[str, Any] = {}


class RelationshipPattern(Algebraizable):
    """Relationship pattern in MATCH/CREATE clauses.

    Represents a relationship pattern like -[r:KNOWS]-> in Cypher queries.

    Attributes:
        variable: Optional Variable instance representing the relationship's binding name (e.g., Variable(name="r"))
        labels: List of relationship type names
        properties: Property map as dict (optional)
        direction: Relationship direction token from RelationshipDirection ("<-", "->", or "-")
        length: PathLength specification for variable-length relationships (optional)
        where: WHERE condition for relationship patterns (optional)

    Example:
        >>> rel = RelationshipPattern(
        ...     variable=Variable(name="knows"),
        ...     labels=["KNOWS"],
        ...     direction=RelationshipDirection.RIGHT
        ... )
        >>> print(rel.variable.name)  # "knows"

    """

    variable: Variable | None = None
    labels: list[str] = Field(default_factory=list)
    properties: dict[str, Any] | None = None
    direction: RelationshipDirection
    length: PathLength | None = None
    where: Expression | None = None


class PathLength(ASTNode):
    """Variable-length path specification."""

    min: int | None = None
    max: int | None = None
    unbounded: bool = False


# We need RelationshipDirection available here for type annotations
from pycypher.ast_models.core import RelationshipDirection as RelationshipDirection  # noqa: E402
