"""Converts dictionary-based AST to typed Pydantic models.

Extracted from ``ast_models.py`` to separate the ~60 conversion methods from the
76 Pydantic model definitions, reducing cognitive load and enabling independent
testing of the converter logic.

The public entry point is :meth:`ASTConverter.from_cypher` which parses a Cypher
query string and returns a typed :class:`~pycypher.ast_models.ASTNode`.

Usage::

    from pycypher.ast_converter import ASTConverter

    ast = ASTConverter.from_cypher("MATCH (n:Person) RETURN n.name")
"""

from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, cast

from shared.logger import LOGGER

import pycypher.ast_models as _ast_mod
from pycypher.ast_models import (
    And,
    Arithmetic,
    ASTNode,
    BooleanLiteral,
    Call,
    CaseExpression,
    Clause,
    Comparison,
    CountStar,
    Create,
    Delete,
    Exists,
    FloatLiteral,
    Foreach,
    FunctionInvocation,
    IndexLookup,
    IntegerLiteral,
    LabelPredicate,
    ListComprehension,
    ListLiteral,
    MapElement,
    MapLiteral,
    MapProjection,
    Match,
    Merge,
    NodePattern,
    Not,
    NullCheck,
    NullLiteral,
    Or,
    OrderByItem,
    PathLength,
    Pattern,
    PatternComprehension,
    PatternPath,
    PropertyLookup,
    Quantifier,
    Query,
    Reduce,
    RelationshipDirection,
    RelationshipPattern,
    Remove,
    RemoveItem,
    Return,
    ReturnAll,
    ReturnItem,
    Set,
    SetItem,
    Slicing,
    StringLiteral,
    StringPredicate,
    Unary,
    UnionQuery,
    Unwind,
    Variable,
    WhenClause,
    With,
    Xor,
    YieldItem,
)

if TYPE_CHECKING:
    import lark

    from pycypher.ast_models import Expression

# ============================================================================
# AST Converter
# ============================================================================


def _friendly_parse_error(exc: Exception, query: str) -> str:
    """Translate a Lark parse exception into a user-friendly error message.

    Detects common mistakes (misspelled keywords, missing delimiters) and
    returns an enhanced message with the original Lark output plus actionable
    suggestions.

    Args:
        exc: The Lark exception (UnexpectedCharacters, UnexpectedToken, etc.).
        query: The original Cypher query string.

    Returns:
        A human-readable error string.

    """
    import difflib

    raw = str(exc)

    # --- Misspelled keyword detection ---
    # Lark's message format: "No terminal matches '<char>' ... at line N col M"
    # Extract the token around the error position if available.
    col: int | None = getattr(exc, "column", None)
    line: int | None = getattr(exc, "line", None)
    expected: set[str] | None = getattr(exc, "expected", None) or getattr(
        exc,
        "allowed",
        None,
    )

    if col is not None and line is not None and expected:
        # Get the line of query text where the error is.
        query_lines = query.split("\n")
        if 1 <= line <= len(query_lines):
            error_line = query_lines[line - 1]
            # Extract the word at the error position (1-indexed col).
            pos = col - 1
            # Walk back to start of word
            start = pos
            while start > 0 and error_line[start - 1].isalpha():
                start -= 1
            # Walk forward to end of word
            end = pos
            while end < len(error_line) and error_line[end].isalpha():
                end += 1
            bad_word = error_line[start:end].upper()

            if bad_word:
                # Compare against expected terminals (Lark names are like MATCH, RETURN, etc.)
                keyword_candidates = [
                    t
                    for t in expected
                    if t.isalpha() and t.isupper() and len(t) > 1
                ]
                matches = difflib.get_close_matches(
                    bad_word,
                    keyword_candidates,
                    n=1,
                    cutoff=0.6,
                )
                if matches:
                    return (
                        f"Syntax error at line {line}, column {col}: "
                        f"unexpected '{bad_word}'. Did you mean '{matches[0]}'?\n\n{raw}"
                    )

    # --- Missing closing delimiter detection ---
    open_parens = query.count("(") - query.count(")")
    open_brackets = query.count("[") - query.count("]")
    open_braces = query.count("{") - query.count("}")

    hints: list[str] = []
    if open_parens > 0:
        hints.append(f"Missing {open_parens} closing parenthesis ')'")
    elif open_parens < 0:
        hints.append(f"Extra {-open_parens} closing parenthesis ')'")
    if open_brackets > 0:
        hints.append(f"Missing {open_brackets} closing bracket ']'")
    if open_braces > 0:
        hints.append(f"Missing {open_braces} closing brace '}}'")

    if hints:
        hint_str = "; ".join(hints)
        return f"Syntax error: {hint_str}.\n\n{raw}"

    # Fallback: return the raw Lark message as-is.
    return raw


@functools.lru_cache(maxsize=512)
def _parse_cypher_cached(cypher: str) -> ASTNode:
    """Parse a Cypher query string to a typed AST, with LRU caching.

    The Lark Earley parser is the dominant cost in query execution (~56ms per
    call on a typical query).  Since AST nodes are read-only during execution,
    caching the parsed result by query string is safe and eliminates redundant
    parses for repeated query strings — the common case in ETL pipelines.

    This function is module-level (not a method) so ``functools.lru_cache``
    works without any metaclass or descriptor complications.

    Args:
        cypher: The Cypher query string to parse.

    Returns:
        A typed :class:`ASTNode` for *cypher*.

    Raises:
        ASTConversionError: If *cypher* is syntactically invalid or the grammar
            transformer produces a node type with no :class:`ASTNode` class.
            The message includes position info, close-match suggestions for
            misspelled keywords, and delimiter-mismatch hints.

    """
    from pycypher.grammar_parser import get_default_parser

    converter = ASTConverter()
    parser = get_default_parser()
    try:
        tree: lark.tree.Tree = parser.parse(query=cypher)
    except Exception as exc:
        # Lark raises UnexpectedInput (or subclasses) for syntax errors.
        # We import lark at runtime to avoid circular import issues
        # (lark is only in TYPE_CHECKING at module level).
        import lark as _lark

        from pycypher.exceptions import ASTConversionError, CypherSyntaxError

        if isinstance(exc, CypherSyntaxError):
            raise ASTConversionError(
                str(exc),
                query_fragment=cypher,
            ) from exc
        if isinstance(exc, _lark.exceptions.UnexpectedInput):
            msg = _friendly_parse_error(exc, cypher)
            raise ASTConversionError(msg, query_fragment=cypher) from exc
        raise
    ast_dict: dict = parser.transformer.transform(tree)
    ast_node: ASTNode | None = converter.convert(node=ast_dict)
    if ast_node is None:
        from pycypher.exceptions import GrammarTransformerSyncError

        msg = "Failed to convert parsed AST to typed model."
        raise GrammarTransformerSyncError(
            msg,
            missing_node_type="unknown",
            query_fragment=cypher,
        )
    return ast_node


class ASTConverter:
    """Converts dictionary-based AST to Pydantic models."""

    @classmethod
    def from_cypher(cls, cypher: str) -> ASTNode:
        """Parse Cypher query and convert to typed AST.

        Delegates to :func:`_parse_cypher_cached`, which caches the result by
        query string so the expensive Earley parse is performed at most once per
        unique query in the lifetime of the process.

        Args:
            cypher: Cypher query string.

        Returns:
            Typed :class:`ASTNode`.  The same object is returned for identical
            *cypher* strings (the result is shared — do not mutate it).

        Raises:
            ValueError: If AST conversion fails (grammar/model mismatch).
            lark.exceptions.UnexpectedInput: If *cypher* is syntactically invalid.

        """
        return _parse_cypher_cached(cypher)

    def convert(self, node: Any) -> ASTNode | None:
        """Convert a dictionary-based AST node to a Pydantic model.

        Args:
            node: Dictionary, list, or primitive value from grammar parser

        Returns:
            Typed ASTNode or None

        """
        if node is None:
            return None

        # Handle Lark Tree objects (convert to string representation for now)
        if hasattr(node, "__class__") and "Tree" in node.__class__.__name__:
            # This is a Lark Tree, try to extract data
            if hasattr(node, "children") and node.children:
                # Get the first child which should be the actual data
                return self.convert(
                    node.children[0] if node.children else None,
                )
            return None

        if not isinstance(node, dict):
            # Primitive value
            result = self._convert_primitive(node)
            # Wrap primitives in appropriate AST nodes when in expression context
            match result:
                case str() if result:  # Non-empty string
                    return Variable(name=result)
                case bool():
                    return BooleanLiteral(value=result)
                case int():
                    return IntegerLiteral(value=result)
                case float():
                    return FloatLiteral(value=result)
                case _:
                    return result

        node_type = node.get("type")
        if not node_type:
            # Empty dict should return None
            if not node:
                return None
            # Dict without "type" field might be a primitive map literal
            return self._convert_primitive(node)

        # Map type names to classes
        converter_method = getattr(self, f"_convert_{node_type}", None)
        if converter_method:
            return converter_method(node)

        # Generic fallback
        return self._convert_generic(node, node_type)

    def _convert_generic(
        self,
        node: dict,
        node_type: str,
    ) -> ASTNode | None:
        """Generic converter for nodes without specific handler."""
        # Try to find the class in globals
        match cls := getattr(_ast_mod, node_type, None):
            case type() if issubclass(cls, ASTNode):
                try:
                    # remove 'type' from args
                    args = {k: v for k, v in node.items() if k != "type"}

                    # Recursively convert fields that look like AST dicts
                    converted_args = {}
                    for k, v in args.items():
                        if isinstance(v, dict) and "type" in v:
                            converted_args[k] = self.convert(v)
                        elif isinstance(v, list):
                            new_list = []
                            for item in v:
                                if isinstance(item, dict) and "type" in item:
                                    new_list.append(self.convert(item))
                                elif isinstance(item, dict):
                                    # Convert dicts that might be AST nodes but missing type?
                                    # Or just primitives.
                                    new_list.append(
                                        self._convert_primitive(item),
                                    )
                                else:
                                    new_list.append(item)
                            converted_args[k] = new_list
                        else:
                            converted_args[k] = v

                    return cls(**converted_args)
                except Exception as e:
                    from pycypher.exceptions import ASTConversionError

                    msg = f"AST conversion failed: {e}"
                    raise ASTConversionError(
                        msg,
                        node_type=node_type,
                    ) from e

        LOGGER.warning(f"No converter found for node type: {node_type}")
        return None

    def _convert_primitive(self, value: Any) -> Any:
        """Convert primitive values, returning them as-is for simple types.

        For primitive types (bool, int, float, str, None), returns the value directly.
        For complex types (list, dict), converts to AST nodes.
        """
        # Return primitives as-is using match-case pattern matching
        match value:
            case None:
                return None
            case bool():
                return value
            case int():
                return value
            case float():
                return value
            case str():
                return value
            case list():
                # Convert all lists (including empty) to ListLiteral so that
                # expressions like UNWIND [] AS x parse correctly.
                elements = [self.convert(item) for item in value]
                return ListLiteral(
                    value=value,
                    elements=cast(
                        "list[Expression]",
                        [e for e in elements if e],
                    ),
                )
            case dict():
                # Return empty dict as-is
                if not value:
                    return {}
                # If it has 'type' field, it's an AST dict, not a primitive
                if "type" in value:
                    return None
                # Plain dictionary - convert to MapLiteral
                entries = {k: self.convert(v) for k, v in value.items()}
                return MapLiteral(
                    value=value,
                    entries=cast(
                        "dict[str, Expression]",
                        {k: v for k, v in entries.items() if v},
                    ),
                )
            case _:
                return None

    def _convert_Query(self, node: dict) -> Query:
        """Convert Query node."""
        # Handle both 'clauses' and 'statements' structure
        clauses = []

        if "clauses" in node:
            clauses = [self.convert(c) for c in node.get("clauses", [])]
        elif "statements" in node:
            # New structure with statements
            for stmt_list in node.get("statements", []):
                if isinstance(stmt_list, list):
                    for stmt in stmt_list:
                        converted = self.convert(stmt)
                        if converted:
                            # If it's a QueryStatement, extract its clauses
                            if hasattr(converted, "clauses"):
                                clauses.extend(
                                    converted.clauses
                                    if isinstance(converted.clauses, list)
                                    else [],
                                )
                            elif isinstance(converted, Clause):
                                clauses.append(converted)

        return Query(clauses=[c for c in clauses if isinstance(c, Clause)])

    def _convert_UnionQuery(self, node: dict) -> UnionQuery:
        """Convert UnionQuery node — multiple statements joined by UNION [ALL].

        ``all_flags[i]`` is ``True`` iff the connection between
        ``statements[i]`` and ``statements[i+1]`` uses ``UNION ALL``.
        """
        stmt_nodes: list[dict] = node.get("stmts", [])
        all_flags: list[bool] = node.get("all_flags", [])
        statements: list[Query] = []
        for stmt_node in stmt_nodes:
            converted = self.convert(stmt_node)
            if isinstance(converted, Query):
                statements.append(converted)
        return UnionQuery(statements=statements, all_flags=all_flags)

    def _convert_QueryStatement(self, node: dict) -> Query:
        """Convert QueryStatement node to Query with clauses."""
        clauses = []

        # Extract reading clauses (MATCH, etc.)
        for clause_item in node.get("clauses", []):
            converted = self.convert(clause_item)
            if converted and isinstance(converted, Clause):
                clauses.append(converted)

        # Extract return statement
        if node.get("return"):
            ret_clause = self.convert(node["return"])
            if ret_clause and isinstance(ret_clause, Clause):
                clauses.append(ret_clause)

        return Query(clauses=clauses)

    def _convert_UpdateStatement(self, node: dict) -> Query:
        """Convert UpdateStatement which contains CREATE/SET/DELETE/etc.

        Uses the ``clauses`` flat ordered list when available (produced by the
        updated transformer) so that multi-step pipelines like
        ``MATCH … SET … WITH … SET … RETURN`` are assembled in the correct
        execution order.  Falls back to the legacy prefix→updates→return
        ordering for AST dicts produced by older code paths.
        """
        clauses = []

        ordered = node.get("clauses")
        if ordered is not None:
            # Flat ordered list — preserves correct multi-step execution order.
            for clause_dict in ordered:
                converted = self.convert(clause_dict)
                if converted and isinstance(converted, Clause):
                    clauses.append(converted)
        else:
            # Legacy path: prefix clauses first, then update clauses, then return.
            for clause_dict in node.get("prefix", []):
                converted = self.convert(clause_dict)
                if converted and isinstance(converted, Clause):
                    clauses.append(converted)
            for clause_dict in node.get("updates", []):
                converted = self.convert(clause_dict)
                if converted and isinstance(converted, Clause):
                    clauses.append(converted)
            if node.get("return"):
                ret_clause = self.convert(node["return"])
                if ret_clause and isinstance(ret_clause, Clause):
                    clauses.append(ret_clause)

        return Query(clauses=clauses)

    def _convert_CreateClause(self, node: dict) -> Create:
        """Convert CreateClause node."""
        return Create(
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
        )

    def _convert_SetClause(self, node: dict) -> Set:
        """Convert SetClause node."""
        items = [self.convert(item) for item in node.get("items", [])]
        return Set(items=cast("list[SetItem]", [i for i in items if i]))

    def _convert_DeleteClause(self, node: dict) -> Delete:
        """Convert DeleteClause node."""
        items = node.get("items", [])
        exprs = [self.convert(e) for e in items]
        return Delete(
            detach=node.get("detach", False),
            expressions=cast("list[Expression]", [e for e in exprs if e]),
        )

    def _convert_RemoveClause(self, node: dict) -> Remove:
        """Convert RemoveClause node."""
        items = [self.convert(item) for item in node.get("items", [])]
        return Remove(items=cast("list[RemoveItem]", [i for i in items if i]))

    def _convert_MergeClause(self, node: dict) -> Merge:
        """Convert MergeClause node."""
        # Convert actions to on_create and on_match
        on_create = []
        on_match = []
        for action in node.get("actions", []):
            if isinstance(action, dict):
                if action.get("on") == "create":
                    set_clause = action.get("set")
                    if set_clause:
                        converted = self.convert(set_clause)
                        match converted:
                            case Set(items=set_items):
                                on_create.extend(set_items)
                            case SetItem() as set_item:
                                on_create.append(set_item)
                elif action.get("on") == "match":
                    set_clause = action.get("set")
                    if set_clause:
                        converted = self.convert(set_clause)
                        match converted:
                            case Set(items=set_items):
                                on_match.extend(set_items)
                            case SetItem() as set_item:
                                on_match.append(set_item)

        return Merge(
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
            on_create=cast("list[SetItem] | None", on_create)
            if on_create
            else None,
            on_match=cast("list[SetItem] | None", on_match)
            if on_match
            else None,
        )

    def _convert_MatchClause(self, node: dict) -> Match:
        """Convert MatchClause node."""
        where_cond = None
        if node.get("where"):
            where_dict = node["where"]
            if isinstance(where_dict, dict) and "condition" in where_dict:
                where_cond = self.convert(where_dict["condition"])
            else:
                where_cond = self.convert(where_dict)

        return Match(
            optional=node.get("optional", False),
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
            where=cast("Expression | None", where_cond),
        )

    def _convert_ReturnStatement(self, node: dict) -> Return:
        """Convert ReturnStatement node."""
        items = []

        # Handle body which contains return items
        body = node.get("body")
        if body:
            if isinstance(body, dict) and body.get("type") == "ReturnBody":
                items_list = body.get("items", [])
                items = [self.convert(item) for item in items_list]

        order_by = None
        if node.get("order"):
            order_items = (
                node["order"].get("items", [])
                if isinstance(node["order"], dict)
                else []
            )
            converted_order = [
                self.convert(item) for item in order_items if item is not None
            ]
            order_by = [
                o for o in converted_order if o is not None
            ]  # Filter out None values

        # Extract limit value — keep as int when possible, otherwise preserve
        # the Expression AST node (e.g. Parameter) for runtime evaluation.
        limit_val = None
        if node.get("limit"):
            limit_clause = node["limit"]
            if isinstance(limit_clause, dict):
                limit_val = limit_clause.get("value")
                # Handle Lark Tree objects
                if (
                    hasattr(limit_val, "__class__")
                    and limit_val.__class__.__name__ == "Tree"
                ):
                    # Get first child which should be the integer
                    if limit_val.children:
                        limit_val = limit_val.children[0]
                if isinstance(limit_val, int):
                    pass  # Already an int
                elif hasattr(limit_val, "value"):
                    try:
                        limit_val = int(limit_val.value)
                    except (ValueError, TypeError):
                        # Not a plain integer — convert to AST expression node
                        limit_val = self.convert(limit_clause.get("value"))
                elif isinstance(limit_val, dict):
                    # Non-int expression dict (e.g. Parameter) — convert to AST node
                    limit_val = self.convert(limit_val)
                else:
                    try:
                        limit_val = int(str(limit_val))
                    except (ValueError, TypeError):
                        limit_val = None

        # Extract skip value — same int-or-expression logic.
        skip_val = None
        if node.get("skip"):
            skip_clause = node["skip"]
            if isinstance(skip_clause, dict):
                skip_val = skip_clause.get("value")
                if isinstance(skip_val, int):
                    pass
                elif hasattr(skip_val, "value"):
                    try:
                        skip_val = int(skip_val.value)
                    except (ValueError, TypeError):
                        skip_val = self.convert(skip_clause.get("value"))
                elif isinstance(skip_val, dict):
                    skip_val = self.convert(skip_val)
                else:
                    try:
                        skip_val = int(str(skip_val))
                    except (ValueError, TypeError):
                        skip_val = None

        return Return(
            distinct=node.get("distinct", False),
            items=cast("list[ReturnItem]", [i for i in items if i]),
            order_by=cast("list[OrderByItem] | None", order_by)
            if order_by
            else None,
            skip=skip_val,
            limit=limit_val,
        )

    def _convert_Match(self, node: dict) -> Match:
        """Convert Match node."""
        return Match(
            optional=node.get("optional", False),
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_WithClause(self, node: dict) -> With:
        """Convert WithClause node.

        WITH is similar to RETURN but continues query processing.
        It filters and projects variables for the next query stage.

        Args:
            node: Dictionary with type "WithClause"

        Returns:
            With clause with items, WHERE, ORDER BY, SKIP, LIMIT

        """
        # Convert return items
        items = []
        items_list = node.get("items", [])
        if items_list:
            items = [self.convert(item) for item in items_list]

        # Handle WHERE clause
        where_cond = None
        if node.get("where"):
            where_dict = node["where"]
            if isinstance(where_dict, dict):
                if "condition" in where_dict:
                    where_cond = self.convert(where_dict["condition"])
                else:
                    where_cond = self.convert(where_dict)

        # Handle ORDER BY
        order_by = None
        if node.get("order"):
            order_items = (
                node["order"].get("items", [])
                if isinstance(node["order"], dict)
                else []
            )
            converted_order = [
                self.convert(item) for item in order_items if item is not None
            ]
            order_by = [o for o in converted_order if o is not None]

        # Extract SKIP value — int when possible, else preserve Expression.
        skip_val = None
        if node.get("skip"):
            skip_clause = node["skip"]
            if isinstance(skip_clause, dict):
                raw = skip_clause.get("value")
                if isinstance(raw, int):
                    skip_val = raw
                elif hasattr(raw, "value"):
                    try:
                        skip_val = int(raw.value)
                    except (ValueError, TypeError):
                        skip_val = self.convert(skip_clause.get("value"))
                elif isinstance(raw, dict):
                    skip_val = self.convert(raw)
                else:
                    try:
                        skip_val = int(str(raw))
                    except (ValueError, TypeError):
                        skip_val = None

        # Extract LIMIT value — same int-or-expression logic.
        limit_val = None
        if node.get("limit"):
            limit_clause = node["limit"]
            if isinstance(limit_clause, dict):
                raw = limit_clause.get("value")
                # Handle Lark Tree objects
                if (
                    hasattr(raw, "__class__")
                    and raw.__class__.__name__ == "Tree"
                ) and raw.children:
                    raw = raw.children[0]
                if isinstance(raw, int):
                    limit_val = raw
                elif hasattr(raw, "value"):
                    try:
                        limit_val = int(raw.value)
                    except (ValueError, TypeError):
                        limit_val = self.convert(limit_clause.get("value"))
                elif isinstance(raw, dict):
                    limit_val = self.convert(raw)
                else:
                    try:
                        limit_val = int(str(raw))
                    except (ValueError, TypeError):
                        limit_val = None

        return With(
            distinct=node.get("distinct", False),
            items=cast("list[ReturnItem]", [i for i in items if i]),
            where=cast("Expression | None", where_cond),
            order_by=cast("list[OrderByItem] | None", order_by),
            skip=skip_val,
            limit=limit_val,
        )

    def _convert_Return(self, node: dict) -> Return:
        """Convert Return node."""
        items = [self.convert(item) for item in node.get("items", [])]
        order_by = None
        if node.get("order_by"):
            order_by = [self.convert(item) for item in node["order_by"]]

        return Return(
            distinct=node.get("distinct", False),
            items=cast("list[ReturnItem]", [i for i in items if i]),
            order_by=cast("list[OrderByItem] | None", order_by),
            skip=node.get("skip"),
            limit=node.get("limit"),
        )

    def _convert_ReturnItem(self, node: dict) -> ReturnItem:
        """Convert ReturnItem node."""
        return ReturnItem(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            alias=node.get("alias"),
        )

    def _convert_ReturnAll(self, node: dict) -> ReturnAll:
        """Convert ReturnAll node."""
        return ReturnAll()

    def _convert_With(self, node: dict) -> With:
        """Convert With node."""
        items = [self.convert(item) for item in node.get("items", [])]
        order_by = None
        if node.get("order_by"):
            order_by = [self.convert(item) for item in node["order_by"]]

        return With(
            distinct=node.get("distinct", False),
            items=cast("list[ReturnItem]", [i for i in items if i]),
            where=cast("Expression | None", self.convert(node.get("where"))),
            order_by=cast("list[OrderByItem] | None", order_by),
            skip=node.get("skip"),
            limit=node.get("limit"),
        )

    def _convert_Create(self, node: dict) -> Create:
        """Convert Create node."""
        return Create(
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
        )

    def _convert_Merge(self, node: dict) -> Merge:
        """Convert Merge node."""
        on_create = None
        if node.get("on_create"):
            on_create = [self.convert(item) for item in node["on_create"]]

        on_match = None
        if node.get("on_match"):
            on_match = [self.convert(item) for item in node["on_match"]]

        return Merge(
            pattern=cast("Pattern | None", self.convert(node.get("pattern"))),
            on_create=cast("list[SetItem] | None", on_create),
            on_match=cast("list[SetItem] | None", on_match),
        )

    def _convert_Delete(self, node: dict) -> Delete:
        """Convert Delete node."""
        exprs = [self.convert(e) for e in node.get("expressions", [])]
        return Delete(
            detach=node.get("detach", False),
            expressions=cast("list[Expression]", [e for e in exprs if e]),
        )

    def _convert_Set(self, node: dict) -> Set:
        """Convert Set node."""
        items = [self.convert(item) for item in node.get("items", [])]
        return Set(items=cast("list[SetItem]", [i for i in items if i]))

    def _convert_SetItem(self, node: dict) -> SetItem:
        """Convert SetItem node."""
        var_name = node.get("variable")
        return SetItem(
            variable=Variable(name=var_name) if var_name else None,
            property=node.get("property"),
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            labels=node.get("labels", []),
        )

    def _convert_SetProperty(self, node: dict) -> SetItem:
        """Convert SetProperty node to SetItem."""
        # Extract property name from PropertyLookup dict if needed
        prop = node.get("property")
        if isinstance(prop, dict) and prop.get("type") == "PropertyLookup":
            prop = prop.get("property")
        var_name = node.get("variable")
        return SetItem(
            variable=Variable(name=var_name) if var_name else None,
            property=prop,
            expression=cast(
                "Expression | None",
                self.convert(node.get("value")),
            ),
            labels=[],
        )

    def _convert_SetLabels(self, node: dict) -> SetItem:
        """Convert SetLabels node to SetItem."""
        labels = node.get("labels", [])
        # Convert label expression if needed
        if isinstance(labels, dict):
            labels = [labels.get("name", "")]
        elif not isinstance(labels, list):
            labels = [str(labels)]
        var_name = node.get("variable")
        return SetItem(
            variable=Variable(name=var_name) if var_name else None,
            property=None,
            expression=None,
            labels=labels,
        )

    def _convert_SetAllProperties(self, node: dict) -> SetItem:
        """Convert SetAllProperties node to SetItem."""
        var_name = node.get("variable")
        return SetItem(
            variable=Variable(name=var_name) if var_name else None,
            property="*",  # Sentinel: set_all_properties
            expression=cast(
                "Expression | None",
                self.convert(node.get("value")),
            ),
            labels=[],
        )

    def _convert_AddAllProperties(self, node: dict) -> SetItem:
        """Convert AddAllProperties node (SET n += {map}) to SetItem."""
        var_name = node.get("variable")
        return SetItem(
            variable=Variable(name=var_name) if var_name else None,
            property="*+",  # Sentinel: add_all_properties (merge/upsert)
            expression=cast(
                "Expression | None",
                self.convert(node.get("value")),
            ),
            labels=[],
        )

    def _convert_Remove(self, node: dict) -> Remove:
        """Convert Remove node."""
        items = [self.convert(item) for item in node.get("items", [])]
        return Remove(items=cast("list[RemoveItem]", [i for i in items if i]))

    def _convert_RemoveItem(self, node: dict) -> RemoveItem:
        """Convert RemoveItem node."""
        var_name = node.get("variable")
        return RemoveItem(
            variable=Variable(name=var_name) if var_name else None,
            property=node.get("property"),
            labels=node.get("labels", []),
        )

    def _convert_RemoveProperty(self, node: dict) -> RemoveItem:
        """Convert RemoveProperty node to RemoveItem."""
        # Extract property name from PropertyLookup dict if needed
        prop = node.get("property")
        if isinstance(prop, dict) and prop.get("type") == "PropertyLookup":
            prop = prop.get("property")
        var_name = node.get("variable")
        return RemoveItem(
            variable=Variable(name=var_name) if var_name else None,
            property=prop,
            labels=[],
        )

    def _convert_RemoveLabels(self, node: dict) -> RemoveItem:
        """Convert RemoveLabels node to RemoveItem."""
        labels = node.get("labels", [])
        # Convert label expression if needed
        if isinstance(labels, dict):
            labels = [labels.get("name", "")]
        elif not isinstance(labels, list):
            labels = [str(labels)]
        var_name = node.get("variable")
        return RemoveItem(
            variable=Variable(name=var_name) if var_name else None,
            property=None,
            labels=labels,
        )

    def _convert_Unwind(self, node: dict) -> Unwind:
        """Convert Unwind node."""
        return Unwind(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            alias=node.get("alias"),
        )

    def _convert_UnwindClause(self, node: dict) -> Unwind:
        """Convert UnwindClause node."""
        return Unwind(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            alias=node.get(
                "variable",
            ),  # UnwindClause uses 'variable' instead of 'alias'
        )

    def _convert_ForeachClause(self, node: dict) -> Foreach:
        """Convert ForeachClause node.

        The grammar produces:
          {"type": "ForeachClause", "variable": str, "list_expression": expr,
           "clauses": [update_clause_dict, ...]}
        """
        raw_clauses = node.get("clauses", [])
        converted_clauses: list[Any] = []
        for c in raw_clauses:
            converted = self.convert(c)
            if converted is not None:
                converted_clauses.append(converted)
        return Foreach(
            variable=node.get("variable"),
            list_expression=cast(
                "Expression | None",
                self.convert(node.get("list_expression")),
            ),
            clauses=converted_clauses,
        )

    def _convert_Call(self, node: dict) -> Call:
        """Convert Call node."""
        args = [self.convert(a) for a in node.get("arguments", [])]
        yield_items = [self.convert(y) for y in node.get("yield_items", [])]

        procedure_name = self._normalize_procedure_name(
            node.get("procedure_name"),
        )

        return Call(
            procedure_name=procedure_name,
            arguments=cast("list[Expression]", [a for a in args if a]),
            yield_items=cast("list[YieldItem]", [y for y in yield_items if y]),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _normalize_procedure_name(self, value: Any) -> str | None:
        """Return dotted procedure name string for CALL clauses."""
        if value is None:
            return None

        if isinstance(value, dict):
            namespace = value.get("namespace")
            name = value.get("name")
            if namespace and name:
                return f"{namespace}.{name}"
            if name:
                return str(name)
            if namespace:
                return str(namespace)
            return None

        if isinstance(value, (list, tuple)):
            parts = [self._normalize_procedure_name(v) for v in value]
            joined = ".".join([p for p in parts if p])
            return joined or None

        return str(value)

    def _convert_Pattern(self, node: dict) -> Pattern:
        """Convert Pattern node."""
        paths = [self.convert(p) for p in node.get("paths", [])]
        # Filter out Nones and convert PatternElements to PatternPaths
        converted_paths = []
        for p in paths:
            if p is None:
                continue
            # PathPattern might come as PathPattern or need conversion from PatternElement
            if isinstance(p, PatternPath):
                converted_paths.append(p)
            elif isinstance(p, dict):
                # Try to convert as PathPattern
                pp = (
                    self._convert_PathPattern(p)
                    if p.get("type") == "PathPattern"
                    else None
                )
                if pp:
                    converted_paths.append(pp)

        return Pattern(paths=converted_paths)

    def _convert_PathPattern(self, node: dict) -> PatternPath:
        """Convert PathPattern node."""
        elements: list = []
        shortest_path_mode = "none"

        element = node.get("element")
        if element:
            if (
                isinstance(element, dict)
                and element.get("type") == "PatternElement"
            ):
                # Extract parts from PatternElement
                parts = element.get("parts", [])
                # Check whether the single part is a ShortestPath dict
                if (
                    len(parts) == 1
                    and isinstance(parts[0], dict)
                    and parts[0].get("type") == "ShortestPath"
                ):
                    sp_dict = parts[0]
                    shortest_path_mode = "all" if sp_dict.get("all") else "one"
                    for part in sp_dict.get("parts", []):
                        converted = self.convert(part)
                        if converted is not None:
                            elements.append(converted)
                else:
                    for part in parts:
                        converted = self.convert(part)
                        if converted:
                            elements.append(converted)
            elif (
                isinstance(element, dict)
                and element.get("type") == "ShortestPath"
            ):
                # Direct ShortestPath element (no PatternElement wrapper)
                shortest_path_mode = "all" if element.get("all") else "one"
                for part in element.get("parts", []):
                    converted = self.convert(part)
                    if converted is not None:
                        elements.append(converted)
            else:
                converted = self.convert(element)
                if converted:
                    elements.append(converted)

        var_name = node.get("variable")
        return PatternPath(
            variable=Variable(name=var_name) if var_name else None,
            elements=elements,
            shortest_path_mode=shortest_path_mode,
        )

    def _convert_ShortestPath(self, node: dict) -> PatternPath:
        """Convert a ShortestPath dict into a PatternPath with shortest_path_mode set.

        This handles the case where ShortestPath appears outside a PathPattern wrapper
        (e.g. directly inside a Pattern), ensuring it is never placed bare into
        PatternPath.elements where it would fail Pydantic validation.
        """
        shortest_path_mode = "all" if node.get("all") else "one"
        elements: list = []
        for part in node.get("parts", []):
            converted = self.convert(part)
            if converted is not None:
                elements.append(converted)
        return PatternPath(
            variable=None,
            elements=elements,
            shortest_path_mode=shortest_path_mode,
        )

    def _convert_PatternElement(self, node: dict) -> PatternPath:
        """Convert PatternElement to PatternPath."""
        elements = []
        for part in node.get("parts", []):
            converted = self.convert(part)
            if converted:
                elements.append(converted)

        return PatternPath(variable=None, elements=elements)

    def _convert_PatternPath(self, node: dict) -> PatternPath:
        """Convert PatternPath node."""
        elements = [self.convert(e) for e in node.get("elements", [])]
        var_name = node.get("variable")
        return PatternPath(
            variable=Variable(name=var_name) if var_name else None,
            elements=cast(
                "list[NodePattern | RelationshipPattern]",
                [e for e in elements if e],
            ),
        )

    def _convert_NodePattern(self, node: dict) -> NodePattern:
        """Convert NodePattern node."""
        var_name = node.get("variable")
        properties_raw = node.get("properties") or {}
        properties: dict[str, Any] = {}
        for key, value in properties_raw.items():
            converted_value = self.convert(value)
            properties[key] = (
                converted_value if converted_value is not None else value
            )
        return NodePattern(
            variable=Variable(name=var_name) if var_name else None,
            labels=node.get("labels", []),
            properties=properties,
        )

    def _convert_RelationshipPattern(self, node: dict) -> RelationshipPattern:
        """Convert RelationshipPattern node."""
        var_name = node.get("variable")
        direction_str = node.get("direction", "right")

        match direction_str:
            case "right":
                direction = RelationshipDirection.RIGHT
            case "left":
                direction = RelationshipDirection.LEFT
            case "both" | "any":
                direction = RelationshipDirection.UNDIRECTED
            case _:
                direction = RelationshipDirection(direction_str)

        return RelationshipPattern(
            variable=Variable(name=var_name) if var_name else None,
            labels=node.get("labels", []) or node.get("types", []),
            properties=node.get("properties") or {},
            direction=direction,
            length=cast(
                "PathLength | None",
                self.convert(node.get("length")),
            ),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_PathLength(self, node: dict) -> PathLength:
        """Convert PathLength node."""
        return PathLength(
            min=node.get("min"),
            max=node.get("max"),
            unbounded=node.get("unbounded", False),
        )

    def _convert_Or(self, node: dict) -> Or:
        """Convert Or node."""
        operands = [self.convert(op) for op in node.get("operands", [])]
        return Or(
            operands=cast("list[Expression]", [o for o in operands if o]),
            operator="OR",
        )

    def _convert_Xor(self, node: dict) -> Xor:
        """Convert Xor node."""
        operands = [self.convert(op) for op in node.get("operands", [])]
        return Xor(
            operands=cast("list[Expression]", [o for o in operands if o]),
            operator="XOR",
        )

    def _convert_And(self, node: dict) -> And:
        """Convert And node."""
        operands = [self.convert(op) for op in node.get("operands", [])]
        return And(
            operands=cast("list[Expression]", [o for o in operands if o]),
            operator="AND",
        )

    def _convert_Not(self, node: dict) -> Not:
        """Convert Not node."""
        return Not(
            operand=cast(
                "Expression | None",
                self.convert(node.get("operand")),
            ),
        )

    def _convert_Comparison(self, node: dict) -> Comparison:
        """Convert Comparison node."""
        return Comparison(
            operator=node.get("operator", "="),
            left=cast("Expression | None", self.convert(node.get("left"))),
            right=cast("Expression | None", self.convert(node.get("right"))),
        )

    def _convert_StringPredicate(self, node: dict) -> StringPredicate:
        """Convert StringPredicate node."""
        return StringPredicate(
            operator=node.get("operator", "CONTAINS"),
            left=cast("Expression | None", self.convert(node.get("left"))),
            right=cast("Expression | None", self.convert(node.get("right"))),
        )

    def _convert_StringLiteral(self, node: dict) -> StringLiteral:
        """Convert StringLiteral node from parser output."""
        return StringLiteral(value=node.get("value", ""))

    def _convert_IntegerLiteral(self, node: dict) -> IntegerLiteral:
        """Convert IntegerLiteral node from parser output."""
        return IntegerLiteral(value=node.get("value", 0))

    def _convert_FloatLiteral(self, node: dict) -> FloatLiteral:
        """Convert FloatLiteral node from parser output."""
        return FloatLiteral(value=node.get("value", 0.0))

    def _convert_BooleanLiteral(self, node: dict) -> BooleanLiteral:
        """Convert BooleanLiteral node from parser output."""
        return BooleanLiteral(value=node.get("value", False))

    def _convert_NullLiteral(self, node: dict) -> NullLiteral:
        """Convert NullLiteral node from parser output."""
        return NullLiteral(value=None)

    def _convert_MapLiteral(self, node: dict) -> MapLiteral:
        """Convert MapLiteral node produced by the grammar ``map_literal`` transformer.

        The transformer produces::

            {"type": "MapLiteral", "value": {"key": expr_dict, ...}}

        where each value in ``"value"`` is a raw AST expression dict.  This
        converter recursively converts those dicts into typed
        :class:`Expression` nodes and stores them in the ``entries`` field so
        the evaluator can operate on proper AST nodes rather than raw dicts.
        """
        raw_value: dict[str, Any] = node.get("value") or {}
        entries: dict[str, Expression] = {}
        for k, v in raw_value.items():
            converted = self.convert(v)
            if converted is not None:
                entries[k] = cast("Expression", converted)
        return MapLiteral(value=raw_value, entries=entries)

    def _convert_NullCheck(self, node: dict) -> NullCheck:
        """Convert NullCheck node."""
        return NullCheck(
            operator=node.get("operator", "IS NULL"),
            operand=cast(
                "Expression | None",
                self.convert(node.get("operand")),
            ),
        )

    def _convert_LabelPredicate(self, node: dict) -> LabelPredicate:
        """Convert LabelPredicate node (n:Label or n:Label1:Label2)."""
        return LabelPredicate(
            operand=cast(
                "Expression | None",
                self.convert(node.get("operand")),
            ),
            labels=list(node.get("labels", [])),
        )

    def _convert_Arithmetic(self, node: dict) -> Arithmetic:
        """Convert Arithmetic node."""
        return Arithmetic(
            operator=node.get("operator", "+"),
            left=cast("Expression | None", self.convert(node.get("left"))),
            right=cast("Expression | None", self.convert(node.get("right"))),
        )

    def _convert_Unary(self, node: dict) -> Unary:
        """Convert Unary node."""
        return Unary(
            operator=node.get("operator", "+"),
            operand=cast(
                "Expression | None",
                self.convert(node.get("operand")),
            ),
        )

    def _convert_PropertyLookup(self, node: dict) -> PropertyLookup:
        """Convert PropertyLookup node."""
        var_name = node.get("variable")
        return PropertyLookup(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            property=node.get("property"),
            variable=Variable(name=var_name)
            if var_name
            else None,  # Legacy support
        )

    def _convert_PropertyAccess(self, node: dict) -> PropertyLookup:
        """Convert PropertyAccess node (alternative name for PropertyLookup)."""
        return PropertyLookup(
            expression=cast(
                "Expression | None",
                self.convert(node.get("object")),
            ),
            property=node.get("property"),
        )

    def _convert_IndexLookup(self, node: dict) -> IndexLookup:
        """Convert IndexLookup node."""
        return IndexLookup(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            index=cast("Expression | None", self.convert(node.get("index"))),
        )

    def _convert_IndexAccess(self, node: dict) -> IndexLookup:
        """Convert IndexAccess node (postfix indexing, e.g. list[0])."""
        return IndexLookup(
            expression=cast(
                "Expression | None",
                self.convert(node.get("object")),
            ),
            index=cast("Expression | None", self.convert(node.get("index"))),
        )

    def _convert_Slicing(self, node: dict) -> Slicing:
        """Convert Slicing node (legacy key names)."""
        return Slicing(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            start=cast("Expression | None", self.convert(node.get("start"))),
            end=cast("Expression | None", self.convert(node.get("end"))),
        )

    def _convert_Slice(self, node: dict) -> Slicing:
        """Convert Slice node produced by the grammar postfix_expression transformer.

        The grammar transformer names this node ``"Slice"`` (not ``"Slicing"``)
        and uses ``"object"``, ``"from"``, and ``"to"`` keys::

            {"type": "Slice", "object": expr, "from": start_expr, "to": end_expr}

        Both ``from`` and ``to`` are optional (``None`` means open-ended).
        """
        return Slicing(
            expression=cast(
                "Expression | None",
                self.convert(node.get("object")),
            ),
            start=cast("Expression | None", self.convert(node.get("from"))),
            end=cast("Expression | None", self.convert(node.get("to"))),
        )

    def _convert_FunctionInvocation(self, node: dict) -> FunctionInvocation:
        """Convert FunctionInvocation node."""
        raw_arguments = node.get("arguments")

        # Convert arguments recursively if present
        converted_arguments = None
        if raw_arguments is not None:
            if isinstance(raw_arguments, dict):
                # Arguments dict may have 'arguments' list and 'distinct' flag
                converted_arguments = {}
                for key, value in raw_arguments.items():
                    if key == "arguments" and isinstance(value, list):
                        # Convert each argument expression in the list
                        converted_arguments[key] = [
                            self.convert(arg) for arg in value
                        ]
                    else:
                        converted_arguments[key] = value
            elif isinstance(raw_arguments, list):
                # Direct list of arguments
                converted_arguments = [
                    self.convert(arg) for arg in raw_arguments
                ]

        return FunctionInvocation(
            name=node.get("name", "unknown"),
            arguments=converted_arguments,
            distinct=(
                node.get("arguments", {}).get("distinct", False)
                if isinstance(node.get("arguments"), dict)
                else False
            ),
        )

    def _convert_CountStar(self, node: dict) -> CountStar:
        """Convert CountStar node."""
        return CountStar()

    def _convert_Exists(self, node: dict) -> Exists:
        """Convert Exists node."""
        return Exists(
            content=cast(
                "Pattern | Query | None",
                self.convert(node.get("content")),
            ),
        )

    def _convert_ExistsSubquery(self, node: dict) -> Query:
        """Convert ExistsSubquery node — full EXISTS { MATCH ... RETURN ... } form."""
        clauses = [self.convert(c) for c in node.get("clauses", [])]
        return Query(
            clauses=[cast("Clause", c) for c in clauses if c is not None]
        )

    def _convert_ListComprehension(self, node: dict) -> ListComprehension:
        """Convert ListComprehension node."""
        var_name = node.get("variable")
        return ListComprehension(
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast(
                "Expression | None",
                self.convert(node.get("in")),
            ),  # 'in' from grammar
            where=cast("Expression | None", self.convert(node.get("where"))),
            map_expr=cast(
                "Expression | None",
                self.convert(node.get("projection")),
            ),  # 'projection' from grammar
        )

    def _convert_PatternComprehension(
        self,
        node: dict,
    ) -> PatternComprehension:
        """Convert PatternComprehension node.

        The grammar transformer produces ``"PatternElement"`` dicts for the
        path inside the comprehension.  ``_convert_PatternElement`` converts
        each one to a ``PatternPath``, which must be wrapped in a ``Pattern``
        to satisfy the ``PatternComprehension.pattern`` field type.

        The map expression is stored under the ``"projection"`` key (not
        ``"map"``), matching the grammar rule name ``pattern_projection``.
        """
        var_name = node.get("variable")
        raw_pattern = self.convert(node.get("pattern"))
        if isinstance(raw_pattern, PatternPath):
            # Grammar produces a PatternElement → PatternPath; wrap in Pattern
            pattern = Pattern(paths=[raw_pattern])
        elif isinstance(raw_pattern, Pattern):
            pattern = raw_pattern
        else:
            pattern = None
        return PatternComprehension(
            variable=Variable(name=var_name) if var_name else None,
            pattern=pattern,
            where=cast("Expression | None", self.convert(node.get("where"))),
            map_expr=cast(
                "Expression | None",
                self.convert(node.get("projection") or node.get("map")),
            ),
        )

    def _convert_Quantifier(self, node: dict) -> Quantifier:
        """Convert Quantifier node."""
        var_name = node.get("variable")
        return Quantifier(
            quantifier=node.get("quantifier", "ALL"),
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast("Expression | None", self.convert(node.get("in"))),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_MapProjection(self, node: dict) -> MapProjection:
        """Convert MapProjection node.

        The grammar transformer returns element dicts in three shapes:
        - ``{"selector": "name"}``            — ``.name`` property selector
        - ``{"property": "k", "value": expr}`` — computed property ``k: expr``
        - ``{"all_properties": True}``         — ``.*`` wildcard

        These are *not* typed AST dicts (no ``"type"`` key), so they must be
        converted directly into :class:`MapElement` objects rather than going
        through the generic ``convert()`` dispatch.
        """
        raw_elements = node.get("elements", [])
        elements: list[MapElement] = []
        for e in raw_elements:
            if not isinstance(e, dict):
                continue
            if e.get("type") == "MapElement":
                # Typed AST dict produced by hand-constructed tests or legacy code.
                me = self._convert_MapElement(e)
                if me is not None:
                    elements.append(me)
            elif e.get("all_properties"):
                elements.append(MapElement(all_properties=True))
            elif "selector" in e:
                # .name — copy property with same name (grammar format)
                elements.append(MapElement(property=e["selector"]))
            elif "property" in e and "value" in e:
                # key: expression — computed property (grammar format)
                elements.append(
                    MapElement(
                        property=e["property"],
                        expression=cast(
                            "Expression | None",
                            self.convert(e["value"]),
                        ),
                    ),
                )
        var_name = node.get("variable")
        return MapProjection(
            variable=Variable(name=var_name) if var_name else None,
            elements=elements,
            include_all=node.get("include_all", False),
        )

    def _convert_MapElement(self, node: dict) -> MapElement:
        """Convert MapElement node."""
        return MapElement(
            property=node.get("property"),
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            all_properties=node.get("all_properties", False),
        )

    def _convert_CaseExpression(self, node: dict) -> CaseExpression:
        """Convert CaseExpression node."""
        when_clauses = [self.convert(w) for w in node.get("when_clauses", [])]
        return CaseExpression(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            when_clauses=cast(
                "list[WhenClause]",
                [w for w in when_clauses if w],
            ),
            else_expr=cast(
                "Expression | None",
                self.convert(node.get("else")),
            ),
        )

    def _convert_SearchedCase(self, node: dict) -> CaseExpression:
        """Convert SearchedCase to CaseExpression."""
        when_clauses = [self.convert(w) for w in node.get("when", [])]
        else_node = node.get("else")
        else_expr = None
        if else_node:
            if isinstance(else_node, dict) and else_node.get("type") == "Else":
                else_expr = self.convert(else_node.get("value"))
            else:
                else_expr = self.convert(else_node)
        return CaseExpression(
            expression=None,  # Searched case has no test expression
            when_clauses=cast(
                "list[WhenClause]",
                [w for w in when_clauses if w],
            ),
            else_expr=cast("Expression | None", else_expr),
        )

    def _convert_SimpleCase(self, node: dict) -> CaseExpression:
        """Convert SimpleCase to CaseExpression."""
        when_clauses = [self.convert(w) for w in node.get("when", [])]
        else_node = node.get("else")
        else_expr = None
        if else_node:
            if isinstance(else_node, dict) and else_node.get("type") == "Else":
                else_expr = self.convert(else_node.get("value"))
            else:
                else_expr = self.convert(else_node)
        return CaseExpression(
            expression=cast(
                "Expression | None",
                self.convert(node.get("operand")),
            ),
            when_clauses=cast(
                "list[WhenClause]",
                [w for w in when_clauses if w],
            ),
            else_expr=cast("Expression | None", else_expr),
        )

    def _convert_Else(self, node: dict) -> Any:
        """Convert Else node - just extract the value."""
        return self.convert(node.get("value"))

    def _convert_SimpleWhen(self, node: dict) -> WhenClause:
        """Convert SimpleWhen to WhenClause.

        The grammar transformer stores comparison values under ``"operands"``
        (a list) rather than ``"value"``.  For the common single-operand case
        we use ``operands[0]``.  For multi-operand WHEN clauses the first
        operand is used as the condition; a full OR-of-comparisons expansion
        would be needed for complete compliance, but single-value WENs cover
        virtually all real-world usage.
        """
        # Support both legacy "value" key and new "operands" list key.
        raw_value = node.get("value")
        if raw_value is None:
            operands = node.get("operands") or []
            raw_value = operands[0] if operands else None
        return WhenClause(
            condition=cast("Expression | None", self.convert(raw_value)),
            result=cast(
                "Expression | None",
                self.convert(node.get("result")),
            ),
        )

    def _convert_SearchedWhen(self, node: dict) -> WhenClause:
        """Convert SearchedWhen to WhenClause."""
        return WhenClause(
            condition=cast(
                "Expression | None",
                self.convert(node.get("condition")),
            ),
            result=cast(
                "Expression | None",
                self.convert(node.get("result")),
            ),
        )

    def _convert_Reduce(self, node: dict) -> Reduce:
        """Convert Reduce node.

        The grammar transformer produces::

            {
                "type": "Reduce",
                "accumulator": {"variable": <acc_name>, "init": <init_expr>},
                "variable": <iter_var_name>,
                "in": <list_expr>,
                "step": <step_expr>,
            }

        Legacy hand-constructed dicts may use the flat format::

            {
                "accumulator": <acc_name_str>,
                "initial": <init_expr>,
                "variable": <iter_var_name>,
                "list": <list_expr>,
                "map": <step_expr>,
            }

        Both formats are accepted.
        """
        acc_raw = node.get("accumulator")
        if isinstance(acc_raw, dict):
            # Grammar-produced nested format: {"variable": name, "init": expr}
            acc_name = acc_raw.get("variable")
            acc_init = acc_raw.get("init")
        else:
            # Flat/legacy format: accumulator is a plain name string
            acc_name = acc_raw
            acc_init = node.get("initial")

        var_name = node.get("variable")
        # "in" key (grammar) takes precedence; fall back to "list" (legacy)
        list_node = (
            node.get("in") if node.get("in") is not None else node.get("list")
        )
        # "step" key (grammar) takes precedence; fall back to "map" (legacy)
        step_node = (
            node.get("step")
            if node.get("step") is not None
            else node.get("map")
        )

        return Reduce(
            accumulator=Variable(name=acc_name) if acc_name else None,
            initial=cast("Expression | None", self.convert(acc_init)),
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast("Expression | None", self.convert(list_node)),
            map_expr=cast("Expression | None", self.convert(step_node)),
        )

    def _convert_All(self, node: dict) -> Quantifier:
        """Convert All quantifier."""
        var_name = node.get("variable")
        return Quantifier(
            quantifier="ALL",
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast(
                "Expression | None",
                self.convert(node.get("list")),
            ),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_Any(self, node: dict) -> Quantifier:
        """Convert Any quantifier."""
        var_name = node.get("variable")
        return Quantifier(
            quantifier="ANY",
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast(
                "Expression | None",
                self.convert(node.get("list")),
            ),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_None(self, node: dict) -> Quantifier:
        """Convert None quantifier."""
        var_name = node.get("variable")
        return Quantifier(
            quantifier="NONE",
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast(
                "Expression | None",
                self.convert(node.get("list")),
            ),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_Single(self, node: dict) -> Quantifier:
        """Convert Single quantifier."""
        var_name = node.get("variable")
        return Quantifier(
            quantifier="SINGLE",
            variable=Variable(name=var_name) if var_name else None,
            list_expr=cast(
                "Expression | None",
                self.convert(node.get("list")),
            ),
            where=cast("Expression | None", self.convert(node.get("where"))),
        )

    def _convert_OrderByItem(self, node: dict) -> OrderByItem:
        """Convert OrderByItem node."""
        return OrderByItem(
            expression=cast(
                "Expression | None",
                self.convert(node.get("expression")),
            ),
            ascending=node.get("ascending", True),
            nulls_placement=node.get("nulls_placement"),
        )

    def _convert_YieldItem(self, node: dict) -> YieldItem:
        """Convert YieldItem node."""
        variable = self.convert(node.get("variable"))
        return YieldItem(
            variable=variable if isinstance(variable, Variable) else None,
            alias=node.get("alias"),
        )

    def _convert_WhereClause(self, node: dict) -> Expression | None:
        """Convert WhereClause - just return the condition."""
        return cast("Expression | None", self.convert(node.get("condition")))

    def _convert_ReturnBody(self, node: dict) -> ASTNode | None:
        """Convert ReturnBody - extract items."""
        # This is typically handled by ReturnStatement converter
        return None
