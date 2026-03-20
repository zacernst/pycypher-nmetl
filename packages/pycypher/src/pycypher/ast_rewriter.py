"""AST Rewriting Engine for multi-query composition.

Provides utilities for creating, cloning, merging, and serializing Cypher
ASTs.  The core operations are:

- **create_with_star()** — creates a ``WITH *`` clause AST node
- **strip_return()** — removes RETURN clauses from a Query AST (immutable)
- **merge_queries()** — combines multiple Query ASTs into one with ``WITH *``
  separators and intermediate RETURN stripping
- **to_cypher()** — serializes a Query AST back to a valid Cypher string

All operations are **immutable** — input ASTs are never mutated.
"""

from __future__ import annotations

from typing import Any

from pycypher.ast_models import (
    BooleanLiteral,
    Comparison,
    Create,
    Delete,
    FloatLiteral,
    IntegerLiteral,
    Match,
    Merge,
    NodePattern,
    NullLiteral,
    Parameter,
    Pattern,
    PatternPath,
    PropertyLookup,
    Query,
    RelationshipPattern,
    Return,
    ReturnItem,
    Set,
    StringLiteral,
    Unwind,
    Variable,
    With,
)


class ASTRewriter:
    """Rewrite, merge, and serialize Cypher ASTs.

    All methods are stateless and produce new AST objects without
    mutating the originals.
    """

    # ------------------------------------------------------------------
    # Node creation
    # ------------------------------------------------------------------

    def create_with_star(self) -> With:
        """Create a ``WITH *`` clause AST node.

        The parser represents ``WITH *`` as a :class:`With` node with an
        empty ``items`` list.

        Returns:
            A new :class:`With` AST node representing ``WITH *``.

        """
        return With(items=[])

    # ------------------------------------------------------------------
    # RETURN stripping
    # ------------------------------------------------------------------

    def strip_return(self, query: Query) -> Query:
        """Return a copy of *query* with all RETURN clauses removed.

        The original AST is **not** mutated.

        Args:
            query: The Query AST to strip.

        Returns:
            A new :class:`Query` with RETURN clauses removed.

        """
        stripped_clauses = [
            c for c in query.clauses if not isinstance(c, Return)
        ]
        return query.model_copy(update={"clauses": stripped_clauses})

    # ------------------------------------------------------------------
    # Query merging
    # ------------------------------------------------------------------

    def merge_queries(self, queries: list[Query]) -> Query:
        """Merge multiple Query ASTs into a single Query.

        Between each pair of queries a ``WITH *`` clause is inserted so
        that variables from earlier queries are visible to later ones.
        Intermediate RETURN clauses are stripped (only the final query's
        RETURN is preserved).

        Args:
            queries: Ordered list of Query ASTs to merge.

        Returns:
            A single :class:`Query` combining all input clauses.

        """
        if not queries:
            return Query(clauses=[])

        all_clauses: list[Any] = []

        for i, q in enumerate(queries):
            is_last = i == len(queries) - 1

            # Strip RETURN from non-final queries
            effective = q if is_last else self.strip_return(q)

            # Insert WITH * separator between queries
            if i > 0:
                all_clauses.append(self.create_with_star())

            all_clauses.extend(effective.clauses)

        return Query(clauses=all_clauses)

    # ------------------------------------------------------------------
    # Cypher serialization (AST → string)
    # ------------------------------------------------------------------

    def to_cypher(self, query: Query) -> str:
        """Serialize a Query AST back to a valid Cypher string.

        Args:
            query: The Query AST to serialize.

        Returns:
            A Cypher query string.

        """
        parts: list[str] = []
        for clause in query.clauses:
            parts.append(self._clause_to_cypher(clause))
        return "\n".join(parts)

    def _clause_to_cypher(self, clause: Any) -> str:
        """Serialize a single clause AST node to Cypher."""
        if isinstance(clause, Create):
            return f"CREATE {self._pattern_to_cypher(clause.pattern)}"

        if isinstance(clause, Match):
            keyword = "OPTIONAL MATCH" if clause.optional else "MATCH"
            result = f"{keyword} {self._pattern_to_cypher(clause.pattern)}"
            if clause.where is not None:
                result += f" WHERE {self._expr_to_cypher(clause.where)}"
            return result

        if isinstance(clause, Merge):
            return f"MERGE {self._pattern_to_cypher(clause.pattern)}"

        if isinstance(clause, Return):
            return self._return_like_to_cypher("RETURN", clause)

        if isinstance(clause, With):
            return self._return_like_to_cypher("WITH", clause)

        if isinstance(clause, Set):
            items_str = ", ".join(
                self._expr_to_cypher(item) for item in clause.items
            )
            return f"SET {items_str}"

        if isinstance(clause, Delete):
            exprs = ", ".join(
                self._expr_to_cypher(e) for e in clause.expressions
            )
            keyword = "DETACH DELETE" if clause.detach else "DELETE"
            return f"{keyword} {exprs}"

        if isinstance(clause, Unwind):
            alias = (
                clause.alias.name
                if isinstance(clause.alias, Variable)
                else str(clause.alias)
            )
            return (
                f"UNWIND {self._expr_to_cypher(clause.expression)} AS {alias}"
            )

        # Fallback for unrecognized clause types
        return str(clause)

    def _return_like_to_cypher(
        self,
        keyword: str,
        clause: Return | With,
    ) -> str:
        """Serialize RETURN or WITH clause."""
        parts: list[str] = [keyword]

        if clause.distinct:
            parts.append("DISTINCT")

        if not clause.items:
            parts.append("*")
        else:
            items_str = ", ".join(
                self._return_item_to_cypher(item) for item in clause.items
            )
            parts.append(items_str)

        result = " ".join(parts)

        # WHERE (WITH clause only)
        if isinstance(clause, With) and clause.where is not None:
            result += f" WHERE {self._expr_to_cypher(clause.where)}"

        # ORDER BY
        if clause.order_by:
            order_parts = []
            for item in clause.order_by:
                expr_str = self._expr_to_cypher(item.expression)
                if item.ascending is False:
                    expr_str += " DESC"
                order_parts.append(expr_str)
            result += " ORDER BY " + ", ".join(order_parts)

        # SKIP / LIMIT
        if clause.skip is not None:
            result += f" SKIP {self._expr_to_cypher(clause.skip)}"
        if clause.limit is not None:
            result += f" LIMIT {self._expr_to_cypher(clause.limit)}"

        return result

    def _return_item_to_cypher(self, item: ReturnItem) -> str:
        """Serialize a single ReturnItem."""
        expr_str = self._expr_to_cypher(item.expression)
        if item.alias:
            return f"{expr_str} AS {item.alias}"
        return expr_str

    def _pattern_to_cypher(self, pattern: Pattern | None) -> str:
        """Serialize a Pattern (collection of PatternPaths)."""
        if pattern is None:
            return ""
        return ", ".join(self._path_to_cypher(path) for path in pattern.paths)

    def _path_to_cypher(self, path: PatternPath) -> str:
        """Serialize a single PatternPath."""
        parts: list[str] = []
        for element in path.elements:
            if isinstance(element, NodePattern):
                parts.append(self._node_to_cypher(element))
            elif isinstance(element, RelationshipPattern):
                parts.append(self._relationship_to_cypher(element))
        return "".join(parts)

    def _node_to_cypher(self, node: NodePattern) -> str:
        """Serialize a NodePattern."""
        inner_parts: list[str] = []

        if node.variable is not None:
            inner_parts.append(node.variable.name)

        if node.labels:
            inner_parts.append(":" + ":".join(node.labels))

        if node.properties:
            props_str = self._props_to_cypher(node.properties)
            inner_parts.append(f" {{{props_str}}}")

        return "(" + "".join(inner_parts) + ")"

    def _relationship_to_cypher(self, rel: RelationshipPattern) -> str:
        """Serialize a RelationshipPattern."""
        inner_parts: list[str] = []

        if rel.variable is not None:
            inner_parts.append(rel.variable.name)

        if rel.labels:
            inner_parts.append(":" + "|".join(rel.labels))

        bracket = "[" + "".join(inner_parts) + "]" if inner_parts else ""

        # Direction handling
        left = "<-" if getattr(rel, "direction", None) == "LEFT" else "-"
        right = "->" if getattr(rel, "direction", None) != "LEFT" else "-"

        if bracket:
            return f"{left}{bracket}{right}"
        return f"{left}{right}"

    def _props_to_cypher(self, props: dict[str, Any]) -> str:
        """Serialize a properties dict."""
        parts = []
        for key, val in props.items():
            parts.append(f"{key}: {self._expr_to_cypher(val)}")
        return ", ".join(parts)

    def _expr_to_cypher(self, expr: Any) -> str:
        """Serialize an expression AST node to Cypher."""
        if expr is None:
            return "NULL"

        if isinstance(expr, Variable):
            return expr.name

        if isinstance(expr, PropertyLookup):
            base = self._expr_to_cypher(expr.expression)
            return f"{base}.{expr.property}"

        if isinstance(expr, IntegerLiteral):
            return str(expr.value)

        if isinstance(expr, FloatLiteral):
            return str(expr.value)

        if isinstance(expr, StringLiteral):
            escaped = expr.value.replace("'", "\\'")
            return f"'{escaped}'"

        if isinstance(expr, BooleanLiteral):
            return "true" if expr.value else "false"

        if isinstance(expr, NullLiteral):
            return "NULL"

        if isinstance(expr, Parameter):
            return f"${expr.name}"

        if isinstance(expr, Comparison):
            left = self._expr_to_cypher(expr.left)
            right = self._expr_to_cypher(expr.right)
            return f"{left} {expr.operator} {right}"

        if isinstance(expr, (int, float)):
            return str(expr)

        # Fallback
        return str(expr)
