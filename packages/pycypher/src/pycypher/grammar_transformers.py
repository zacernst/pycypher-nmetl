"""Specialized AST transformers for openCypher grammar parsing.

This module contains focused transformer classes that replace the monolithic
CypherASTTransformer with specialized, single-responsibility transformers.

The transformers are organized by the type of AST nodes they handle:
- LiteralTransformer: Handles literal values (strings, numbers, booleans)
- ExpressionTransformer: Handles expressions (arithmetic, logical, comparisons)
- PatternTransformer: Handles graph patterns (nodes, relationships, paths)
- StatementTransformer: Handles Cypher statements and clauses
"""

from __future__ import annotations

from typing import Any

from lark import Transformer
from shared.logger import LOGGER


class LiteralTransformer(Transformer):
    """Transforms Lark parse-tree nodes for literal values into Python objects.

    Handles number literals (signed/unsigned, hex, octal, float, inf, NaN),
    string literals (with escape-sequence processing), and boolean literals.
    """

    def number_literal(self, args: list[Any]) -> int | float:
        """Transform number literal node."""
        # This is placeholder - actual implementation would be moved from CypherASTTransformer
        return args[0] if args else 0

    def signed_number(self, args: list[Any]) -> int | float | str:
        """Transform signed number literals into Python int or float values."""
        s = str(args[0])
        try:
            if (
                "." in s
                or "e" in s.lower()
                or "f" in s.lower()
                or "d" in s.lower()
            ):
                return float(s.rstrip("fFdD"))
            return int(s.replace("_", ""))
        except ValueError:
            if "inf" in s.lower():
                return float("inf") if s[0] != "-" else float("-inf")
            if "nan" in s.lower():
                return float("nan")
            return s

    def unsigned_number(self, args: list[Any]) -> int | float | str:
        """Transform unsigned number literals into Python int or float values."""
        s = str(args[0])
        try:
            if (
                "." in s
                or "e" in s.lower()
                or "f" in s.lower()
                or "d" in s.lower()
            ):
                return float(s.rstrip("fFdD"))
            if s.startswith(("0x", "0X")):
                return int(s.replace("_", ""), 16)
            if s.startswith(("0o", "0O")):
                return int(s[2:].replace("_", ""), 8)
            return int(s.replace("_", ""))
        except ValueError:
            if "inf" in s.lower():
                return float("inf")
            if "nan" in s.lower():
                return float("nan")
            return s

    def string_literal(self, args: list[Any]) -> dict[str, Any]:
        """Transform string literals into structured AST nodes."""
        s = str(args[0])
        # Remove quotes and handle escape sequences
        if s.startswith(("'", '"')):
            s = s[1:-1]
        # Basic escape sequence handling
        s = s.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
        s = s.replace("\\\\", "\\").replace("\\'", "'").replace('\\"', '"')
        return {"type": "StringLiteral", "value": s}

    def true(self, args: list[Any]) -> bool:
        """Transform boolean true literal."""
        return True

    def false(self, args: list[Any]) -> bool:
        """Transform boolean false literal."""
        return False


class ExpressionTransformer(Transformer):
    r"""Transforms Lark parse-tree nodes for expressions into AST dicts.

    Handles arithmetic operators (+, -, \*, /, %, ^), unary operators,
    and property lookups. Each method extracts the operator token or
    builds a typed dict consumed by the ASTConverter.
    """

    def add_op(self, args: list[Any]) -> str:
        """Transform addition and subtraction operators."""
        return str(args[0])

    def mult_op(self, args: list[Any]) -> str:
        """Transform multiplication, division, and modulo operators."""
        return str(args[0])

    def pow_op(self, args: list[Any]) -> str:
        """Transform power operator."""
        return str(args[0])

    def unary_op(self, args: list[Any]) -> str:
        """Transform unary operator."""
        return args[0] if args else "-"

    def property_lookup(self, args: list[Any]) -> dict[str, Any]:
        """Transform property lookup expression."""
        return {
            "type": "PropertyLookup",
            "property": str(args[0]) if args else None,
        }


class PatternTransformer(Transformer):
    """Transforms Lark parse-tree nodes for graph patterns into AST dicts.

    Handles inline property maps on nodes and relationships, converting
    ``{key: value}`` syntax into structured dictionaries for ASTConverter.
    """

    def property_list(self, args: list[Any]) -> dict[str, dict[str, Any]]:
        """Transform property list in patterns."""
        result = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"props": result}

    def property_key_value(self, args: list[Any]) -> dict[str, Any]:
        """Transform property key-value pair."""
        return {
            "key": args[0] if len(args) > 0 else "",
            "value": args[1] if len(args) > 1 else None,
        }

    def property_name(self, args: list[Any]) -> str:
        """Transform property name."""
        return args[0] if args else ""


class StatementTransformer(Transformer):
    """Transforms Lark parse-tree nodes for Cypher statements and clauses.

    Handles the top-level query structure (MATCH, RETURN, WITH, WHERE, ORDER BY,
    SKIP, LIMIT, UNION) and produces the typed dict AST consumed by ASTConverter.
    This is the largest of the specialized transformers, covering all clause types.
    """

    def cypher_query(self, args: list[Any]) -> dict[str, Any]:
        """Transform the root query node.

        This is the entry point for all Cypher queries. It wraps all statements
        in a Query container, which is necessary for handling multi-statement
        queries (e.g., multiple queries joined by UNION).

        Args:
            args: List of statement nodes from the statement_list rule.

        Returns:
            Dict with type "Query" or "UnionQuery" containing all statements.

        """
        sl = args[0] if args else []
        if isinstance(sl, dict) and sl.get("type") == "UnionStatementList":
            return {
                "type": "UnionQuery",
                "stmts": sl["stmts"],
                "all_flags": sl["all_flags"],
            }
        return {"type": "Query", "statements": args}

    def statement_list(self, args: list[Any]) -> Any:
        """Transform list of statements, preserving UNION connectors.

        Args:
            args: Alternating statement nodes and UnionOp dicts.

        Returns:
            A plain list (single statement) or a UnionStatementList dict.

        """
        # Separate statements from union operator markers
        stmts = [
            a
            for a in args
            if not (isinstance(a, dict) and a.get("type") == "UnionOp")
        ]
        ops = [
            a
            for a in args
            if isinstance(a, dict) and a.get("type") == "UnionOp"
        ]

        if not ops:
            # Single statement — return as plain list (legacy behaviour)
            return list(args)

        # Build a structured union list.  `ops[i]` connects `stmts[i]` and
        # `stmts[i+1]`.  Store one all_flag per *connection* (N-1 elements).
        return {
            "type": "UnionStatementList",
            "stmts": stmts,
            "all_flags": [op["all"] for op in ops],
        }

    def union_op(self, args: list[Any]) -> dict[str, Any]:
        """Transform a UNION [ALL] connector between two query statements.

        Args:
            args: ``[True]`` when the operator is ``UNION ALL``; ``[]`` when
                it is plain ``UNION``.

        Returns:
            Dict with ``type='UnionOp'`` and ``all`` flag.

        """
        return {"type": "UnionOp", "all": bool(args)}

    def query_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform query statement."""
        return {"type": "QueryStatement", "clauses": args}

    def statement(self, args: list[Any]) -> Any | None:
        """Transform generic statement."""
        return args[0] if args else None

    def match_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform MATCH clause."""
        # Parse args to separate pattern, optional WHERE clause, and optional flag
        pattern = None
        where = None
        optional = False

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "Pattern":
                    pattern = arg
                elif arg.get("type") == "WhereClause":
                    where = arg
                elif arg.get("type") == "OptionalKeyword":
                    optional = True

        result = {
            "type": "MatchClause",
            "pattern": pattern,
            "optional": optional,
        }
        if where:
            result["where"] = where

        return result

    def return_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform RETURN clause for query output specification.

        RETURN determines what a query outputs, similar to SQL SELECT. It can
        return specific expressions (with aliases), or * for all variables.
        DISTINCT, ORDER BY, SKIP, and LIMIT control the result set.
        """
        # Extract DISTINCT flag
        distinct = False
        for a in args:
            if (
                (hasattr(a, "type") and a.type == "DISTINCT")
                or (isinstance(a, str) and a.upper() == "DISTINCT")
                or (hasattr(a, "value") and str(a.value).upper() == "DISTINCT")
            ):
                distinct = True

        # Extract clause components
        body = None
        order = None
        skip = None
        limit = None

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "OrderClause":
                    order = arg
                elif arg.get("type") == "SkipClause":
                    skip = arg
                elif arg.get("type") == "LimitClause":
                    limit = arg
                elif body is None:
                    body = arg
            elif isinstance(arg, list) and body is None:
                # return_body returns a list of items
                body = {"type": "ReturnBody", "items": arg}
            elif arg == "*":
                body = "*"

        return {
            "type": "ReturnStatement",
            "distinct": distinct,
            "body": body,
            "order": order,
            "skip": skip,
            "limit": limit,
        }

    def optional_keyword(self, args: list[Any]) -> dict[str, Any]:
        """Transform OPTIONAL keyword in MATCH clauses."""
        return {"type": "OptionalKeyword"}

    def where_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform WHERE clause."""
        return {"type": "WhereClause", "condition": args[0] if args else None}

    def order_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform an ORDER BY clause for result sorting."""
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "OrderClause", "items": items.get("items", [])}

    def skip_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a SKIP clause for result pagination."""
        value = args[0] if args else None
        return {"type": "SkipClause", "value": value}

    def limit_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a LIMIT clause for result size limiting."""
        value = args[0] if args else None
        return {"type": "LimitClause", "value": value}


class CompositeTransformer(Transformer):
    """Composite transformer that delegates to specialized transformers.

    This class maintains the same interface as the original monolithic
    CypherASTTransformer but delegates method calls to focused, specialized
    transformer instances. For methods not yet migrated, it falls back to
    the original CypherASTTransformer implementation.
    """

    def __init__(self) -> None:
        """Initialize composite transformer with specialized delegates."""
        super().__init__()
        self._literal_transformer = LiteralTransformer()
        self._expression_transformer = ExpressionTransformer()
        self._pattern_transformer = PatternTransformer()
        self._statement_transformer = StatementTransformer()

        # Fallback will be set externally to avoid circular imports
        self._fallback_transformer = None

    def __getattr__(self, name: str) -> Any:
        """Delegate method calls to appropriate specialized transformer."""
        # Try specialized transformers first
        for transformer in [
            self._literal_transformer,
            self._expression_transformer,
            self._pattern_transformer,
            self._statement_transformer,
        ]:
            if hasattr(transformer, name):
                LOGGER.debug(
                    "CompositeTransformer: delegating %r to %s",
                    name,
                    type(transformer).__name__,
                )
                return getattr(transformer, name)

        # Fall back to original implementation for unmigrated methods
        if self._fallback_transformer and hasattr(
            self._fallback_transformer,
            name,
        ):
            LOGGER.debug(
                "CompositeTransformer: falling back to %s for %r",
                type(self._fallback_transformer).__name__,
                name,
            )
            return getattr(self._fallback_transformer, name)

        # If no transformer handles this method, raise AttributeError
        LOGGER.debug("CompositeTransformer: no handler for method %r", name)
        msg = f"No transformer handles method '{name}'"
        raise AttributeError(msg)

    # Methods that need to be on the composite directly
    def transform(self, tree: Any) -> Any:
        """Transform a parse tree to AST using specialized transformers."""
        return super().transform(tree)

    def _ambig(self, args: list[Any]) -> Any:
        """Handle ambiguous parse cases."""
        # Handle Lark ambiguity resolution
        if len(args) == 1:
            return args[0]
        # For now, return first option - this would need more sophisticated logic
        return args[0]

    def set_fallback_transformer(self, fallback: Any) -> None:
        """Set the fallback transformer for unmigrated methods."""
        self._fallback_transformer = fallback
