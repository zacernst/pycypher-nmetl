"""Mixin for expression and operator grammar rules.

Handles boolean, comparison, arithmetic, string predicate, null predicate,
label predicate, postfix, count star, EXISTS, and inline pattern expressions.
"""

from __future__ import annotations

from typing import Any

from lark import Token


class ExpressionRulesMixin:
    """Grammar rule methods for expressions, operators, and predicates.

    Handles:
    - Operator keywords (add_op, mult_op, pow_op, unary_op)
    - Boolean expressions (or, xor, and, not)
    - Comparison expressions
    - Arithmetic expressions (add, mult, power, unary)
    - String predicate expressions (STARTS WITH, ENDS WITH, CONTAINS, IN, =~)
    - Null predicate expressions (IS NULL, IS NOT NULL)
    - Label predicate expressions
    - Postfix expressions (property lookup, index, slicing)
    - Special expressions (count(*), EXISTS, inline pattern predicates)

    Note: Uses ``Token`` from lark for string_predicate_op processing.
    """

    # ========================================================================
    # Operator keywords
    # ========================================================================

    def add_op(self, args: list[Any]) -> str:
        """Extract operator from add_op rule."""
        return str(args[0])

    def mult_op(self, args: list[Any]) -> str:
        """Extract operator from mult_op rule."""
        return str(args[0])

    def pow_op(self, args: list[Any]) -> str:
        """Extract operator from pow_op rule."""
        return str(args[0])

    def unary_op(self, args: list[Any]) -> str:
        """Extract operator from unary_op rule."""
        return str(args[0])

    # ========================================================================
    # Expressions
    # ========================================================================

    # ========================================================================
    # Expressions
    # ========================================================================

    def or_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform OR boolean expression with short-circuit evaluation.

        OR has lowest precedence among boolean operators. Multiple OR operations
        are collected into a single node for easier optimization. Single operands
        pass through to avoid unnecessary wrapping.

        Args:
            args: One or more XOR expression operands.

        Returns:
            Single operand, or dict with type "Or" containing all operands.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "Or", "operands": list(args)}

    def xor_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform XOR (exclusive OR) boolean expression.

        XOR returns true only if operands differ. Multiple XORs are collected
        for easier analysis. This is less common than AND/OR but necessary for
        complete boolean logic support.

        Args:
            args: One or more AND expression operands.

        Returns:
            Single operand, or dict with type "Xor" containing all operands.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "Xor", "operands": list(args)}

    def and_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform AND boolean expression with short-circuit evaluation.

        AND has higher precedence than OR/XOR. Multiple ANDs are collected into
        a single node for easier optimization and execution planning.

        Args:
            args: One or more NOT expression operands.

        Returns:
            Single operand, or dict with type "And" containing all operands.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "And", "operands": list(args)}

    def not_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform NOT expression with proper boolean negation handling.

        NOT is a unary boolean operator that negates its operand. Multiple NOTs
        can be chained (e.g., NOT NOT x), so we count them and apply modulo 2
        logic: odd count = negate, even count = no-op (double negation cancels).

        The NOT_KEYWORD terminal has higher priority than IDENTIFIER in the grammar,
        ensuring "NOT" is parsed as a keyword rather than a variable name. These
        terminals are passed as Lark Token objects, so we filter them separately
        from the expression being negated.

        This careful handling is necessary because:
        1. NOT can be ambiguous with variable names in some contexts
        2. Terminal priorities must be explicit to avoid parse errors
        3. Multiple negations need semantic simplification

        Args:
            args: Mix of NOT_KEYWORD Token objects and the comparison expression.

        Returns:
            The expression unchanged (even NOTs), or wrapped in Not node (odd NOTs).

        """
        # NOT_KEYWORD terminals will be passed as Token objects
        from lark import Token

        not_count = sum(
            1 for a in args if isinstance(a, Token) and a.type == "NOT_KEYWORD"
        )
        # Expression is the non-Token arg
        expr = next((a for a in args if not isinstance(a, Token)), None)

        if not_count == 0:
            return expr
        if not_count % 2 == 1:
            return {"type": "Not", "operand": expr}
        return expr

    def comparison_expression(
        self,
        args: list[Any],
    ) -> Any | dict[str, Any]:
        """Transform comparison expression with operators like =, <>, <, >, <=, >=.

        Comparison expressions allow comparing values for equality or ordering.
        Multiple comparisons can be chained (e.g., a < b < c), which is transformed
        into nested comparison nodes. This structure is necessary for type checking
        and ensuring all operands are comparable.

        Single operands pass through to avoid unnecessary wrapping. This is necessary
        for efficient AST traversal and avoiding deep nesting for simple expressions.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Comparison"
            containing operator, left operand, and right operand for each comparison.

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Comparison",
                "operator": str(op),
                "left": result,
                "right": right,
            }
        return result

    def label_predicate_expression(
        self,
        args: list[Any],
    ) -> Any | dict[str, Any]:
        """Transform label predicate expression (n:Label or n:Label1:Label2).

        A label predicate checks whether the node bound to a variable has a
        specific label.  ``n:Person`` is true when ``n`` is a Person node.
        Multiple colon-separated labels (``n:Person:Employee``) mean the node
        must have ALL listed labels (AND semantics).

        When no labels follow the expression, this rule is transparent (passes
        through unchanged).

        Args:
            args: [expression] or [expression, label_name1, label_name2, ...].

        Returns:
            Expression unchanged if no labels, or dict with type
            ``"LabelPredicate"`` containing ``operand`` and ``labels``.

        """
        if len(args) == 1:
            return args[0]
        operand = args[0]
        labels = [str(lbl) for lbl in args[1:]]
        return {"type": "LabelPredicate", "operand": operand, "labels": labels}

    def null_predicate_expression(
        self,
        args: list[Any],
    ) -> Any | dict[str, Any]:
        """Transform IS NULL or IS NOT NULL predicate expressions.

        Null predicates check whether an expression evaluates to NULL, which is
        necessary because NULL cannot be compared with = or <> in SQL/Cypher
        semantics (NULL = NULL is false, not true). IS NULL and IS NOT NULL
        are the only correct ways to test for null values.

        This method wraps the expression in a NullCheck node only when a null
        operator is present. Otherwise, it passes through the expression unchanged
        to avoid unnecessary wrapper nodes in the AST.

        Args:
            args: [expression] or [expression, null_check_operator].

        Returns:
            Expression unchanged if no null check, or dict with type "NullCheck"
            containing the operator ("IS NULL" or "IS NOT NULL") and operand.

        """
        expr = args[0] if args else None
        if len(args) > 1:
            # Has a null check
            op_type = args[1]
            return {"type": "NullCheck", "operator": op_type, "operand": expr}
        return expr

    def null_check_op(self, args: list[Any]) -> None:
        """Handle IS NULL / IS NOT NULL operator token (consumed by parent rule)."""
        return

    def string_predicate_expression(
        self,
        args: list[Any],
    ) -> Any | dict[str, Any]:
        """Transform string predicate expressions (STARTS WITH, ENDS WITH, CONTAINS, =~, IN).

        String predicates provide specialized string matching operations that are more
        efficient and expressive than using regular expressions for common patterns.
        The IN operator tests set membership. These operations are necessary for
        text search, filtering, and pattern matching in graph queries.

        Multiple string predicates can be chained, creating nested nodes for
        complex string filtering logic. Single operands pass through to avoid
        unnecessary AST depth.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "StringPredicate"
            containing operator, left operand, and right operand for each predicate.

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = args[i] if i < len(args) else None
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "StringPredicate",
                "operator": str(op),
                "left": result,
                "right": right,
            }
        return result

    def string_predicate_op(self, args: list[Any]) -> str:
        """Extract and normalize string predicate operator keywords.

        String predicate operators can be multi-word keywords (STARTS WITH, ENDS WITH)
        or single tokens (CONTAINS, IN, =~). This method joins multi-word operators
        with spaces and uppercases them for consistent AST representation.

        Normalization to uppercase is necessary because Cypher is case-insensitive
        for keywords, and consistent casing enables reliable pattern matching and
        operator dispatch during query execution.

        Args:
            args: One or more keyword tokens forming the operator.

        Returns:
            Space-separated, uppercased operator string (e.g., "STARTS WITH").

        """
        parts: list[str] = []
        for item in args:
            value: str = ""
            if isinstance(item, Token):
                raw_value = str(getattr(item, "value", "") or "")
                token_type = str(getattr(item, "type", "") or "")
                value = raw_value or token_type
            else:
                value = str(item)
            value = value.strip()
            if value:
                parts.append(value.upper())
        return " ".join(parts)

    def starts_with_op(self, _args: list[Any]) -> str:
        """Return the normalized STARTS WITH operator string."""
        return "STARTS WITH"

    def ends_with_op(self, _args: list[Any]) -> str:
        """Return the normalized ENDS WITH operator string."""
        return "ENDS WITH"

    def contains_op(self, _args: list[Any]) -> str:
        """Return the normalized CONTAINS operator string."""
        return "CONTAINS"

    def regex_match_op(self, _args: list[Any]) -> str:
        """Return the normalized regular expression match operator."""
        return "=~"

    def in_op(self, _args: list[Any]) -> str:
        """Return the normalized IN operator string."""
        return "IN"

    def not_in_op(self, _args: list[Any]) -> str:
        """Return the normalized NOT IN operator string."""
        return "NOT IN"

    def is_null(self, args: list[Any]) -> str:
        """Return the IS NULL operator constant.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NULL".

        """
        return "IS NULL"

    def is_not_null(self, args: list[Any]) -> str:
        """Return the IS NOT NULL operator constant.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NOT NULL".

        """
        return "IS NOT NULL"

    def add_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform addition and subtraction arithmetic expressions.

        Addition and subtraction have equal precedence and associate left-to-right.
        This method builds a left-associative tree of Arithmetic nodes.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator ("+" or "-"), left operand, and right operand.

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "+"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def mult_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform multiplication, division, and modulo arithmetic expressions.

        Multiplication, division, and modulo have equal precedence, higher than
        addition/subtraction, and associate left-to-right.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator ("*", "/", or "%"), left operand, and right operand.

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = str(args[i]) if i < len(args) else "*"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def power_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform exponentiation (power) arithmetic expressions using ^ operator.

        Exponentiation has the highest precedence among arithmetic operators and
        associates left-to-right.

        Args:
            args: Alternating operands and operators [left, op1, right1, op2, right2, ...].

        Returns:
            Single operand unchanged, or nested dict nodes with type "Arithmetic"
            containing operator "^", left operand (base), and right operand (exponent).

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for i in range(1, len(args), 2):
            op = "^"
            right = args[i + 1] if i + 1 < len(args) else None
            result = {
                "type": "Arithmetic",
                "operator": op,
                "left": result,
                "right": right,
            }
        return result

    def unary_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform unary plus (+) and minus (-) expressions.

        Args:
            args: [operator, operand] for unary expressions, or [operand] without operator.

        Returns:
            Operand unchanged if no operator, or dict with type "Unary" containing
            operator ("+" or "-") and the operand expression.

        """
        if len(args) == 1:
            return args[0]
        sign = str(args[0])
        operand = args[1] if len(args) > 1 else None
        return {"type": "Unary", "operator": sign, "operand": operand}

    def postfix_expression(
        self,
        args: list[Any],
    ) -> Any | dict[str, Any]:
        """Transform postfix expressions (property access, indexing, slicing).

        Postfix operators apply after an expression and associate left-to-right,
        building a chain of access operations.

        Args:
            args: [atom_expression, postfix_op1, postfix_op2, ...].

        Returns:
            Single atom unchanged, or nested access nodes (PropertyAccess, IndexAccess,
            Slice) forming a left-to-right chain of operations.

        """
        if len(args) == 1:
            return args[0]
        result = args[0]
        for op in args[1:]:
            if isinstance(op, dict) and op.get("type") == "PropertyLookup":
                result = {
                    "type": "PropertyAccess",
                    "object": result,
                    "property": op.get("property"),
                }
            elif isinstance(op, dict) and op.get("type") == "IndexLookup":
                result = {
                    "type": "IndexAccess",
                    "object": result,
                    "index": op.get("index"),
                }
            elif isinstance(op, dict) and op.get("type") == "Slicing":
                result = {
                    "type": "Slice",
                    "object": result,
                    "from": op.get("from"),
                    "to": op.get("to"),
                }
        return result

    def postfix_op(self, args: list[Any]) -> Any | None:
        """Pass through postfix operator nodes without modification.

        Args:
            args: Single postfix operation node (PropertyLookup, IndexLookup, or Slicing).

        Returns:
            The postfix operation unchanged.

        """
        return args[0] if args else None

    def property_lookup(self, args: list[Any]) -> dict[str, Any]:
        """Transform property lookup syntax (.property_name) into intermediate node.

        Args:
            args: Property name string from property_name rule.

        Returns:
            Dict with type "PropertyLookup" and the property name.

        """
        return {
            "type": "PropertyLookup",
            "property": args[0] if args else None,
        }

    def index_lookup(self, args: list[Any]) -> dict[str, Any]:
        """Transform index lookup syntax ([index]) into intermediate node.

        Args:
            args: Index expression that evaluates to the position/key.

        Returns:
            Dict with type "IndexLookup" and the index expression.

        """
        return {"type": "IndexLookup", "index": args[0] if args else None}

    def slicing(self, args: list[Any]) -> dict[str, Any]:
        """Transform list slicing syntax ([from..to]) into intermediate node.

        Args:
            args: [from_expr, to_expr] where either can be None for open ranges.

        Returns:
            Dict with type "Slicing" containing from and to range expressions (may be None).

        """
        # args[0] = result of slice_start (None or expr)
        # args[1] = result of slice_end   (None or expr)
        from_expr = args[0] if len(args) > 0 else None
        to_expr = args[1] if len(args) > 1 else None
        return {"type": "Slicing", "from": from_expr, "to": to_expr}

    def slice_start(self, args: list[Any]) -> Any:
        """Return the start expression of a slice, or None if absent."""
        return args[0] if args else None

    def slice_end(self, args: list[Any]) -> Any:
        """Return the end expression of a slice, or None if absent."""
        return args[0] if args else None

    # ========================================================================
    # Count star
    # ========================================================================

    def count_star(self, args: list[Any]) -> dict[str, str]:
        """Transform COUNT(*) aggregate function into a special AST node.

        Args:
            args: Not used (COUNT(*) has no arguments, * is syntactic).

        Returns:
            Dict with type "CountStar" indicating a count-all operation.

        """
        return {"type": "CountStar"}

    # ========================================================================
    # EXISTS expression
    # ========================================================================

    def exists_expression(self, args: list[Any]) -> dict[str, Any]:
        """Transform EXISTS { ... } subquery expression.

        EXISTS evaluates to true if the subquery returns any results, false otherwise.

        Args:
            args: Single exists_content node containing the subquery specification.

        Returns:
            Dict with type "Exists" containing the subquery content.

        """
        content = args[0] if args else None
        return {"type": "Exists", "content": content}

    def inline_pattern_predicate(self, args: list[Any]) -> dict[str, Any]:
        """Transform an inline pattern predicate into an EXISTS expression.

        ``(a)-[:R]->(b)`` in a WHERE clause is shorthand for
        ``EXISTS { (a)-[:R]->(b) }``.  This transformer converts the parsed
        alternating node/relationship args into the same ``Exists`` dict that
        ``exists_expression`` produces when given a simple pattern body.

        Earley with ``ambiguity="explicit"`` can produce ``_ambig`` Tree nodes
        when an anonymous node ``()`` matches multiple grammar paths.  We
        resolve each such node by transforming the first valid alternative so
        that the result is always a plain dict.

        Args:
            args: Alternating node_pattern and relationship_pattern dicts (or
                ``_ambig`` Trees for anonymous nodes).  At least three elements
                (node, rel, node) are guaranteed by the grammar ``+``.

        Returns:
            Dict with ``type="Exists"`` whose ``content`` is a ``Pattern``
            wrapping a single ``PathPattern`` -> ``PatternElement``.

        """
        from lark import Tree

        resolved: list[Any] = []
        for arg in args:
            if isinstance(arg, Tree) and arg.data == "_ambig":
                # Pick the first alternative and transform it using the
                # existing node_pattern handler.
                first_alt = arg.children[0]
                if isinstance(first_alt, Tree):
                    resolved.append(
                        self.node_pattern(list(first_alt.children)),  # type: ignore[attr-defined]  # mixin; method from composing class
                    )
                else:
                    resolved.append(self.node_pattern([]))
            else:
                resolved.append(arg)

        pattern_element = {"type": "PatternElement", "parts": resolved}
        path_pattern = {
            "type": "PathPattern",
            "variable": None,
            "element": pattern_element,
        }
        pattern = {"type": "Pattern", "paths": [path_pattern]}
        return {"type": "Exists", "content": pattern}

    def exists_content(self, args: list[Any]) -> Any | None:
        """Extract the content of an EXISTS subquery.

        EXISTS content can be either:
        1. A simple pattern with optional WHERE clause (implicit match)
        2. A full query with MATCH/UNWIND/WITH clauses and optional RETURN

        Args:
            args: Parsed subquery content (pattern or query clauses).

        Returns:
            The subquery content unchanged, or a wrapped ExistsSubquery dict
            when the content is a full subquery with multiple clauses.

        """
        if not args:
            return None
        # If the first arg is a clause dict (MatchClause, ReturnStatement, etc.),
        # wrap all args in an ExistsSubquery node so the ASTConverter can build a Query.
        first = args[0]
        if isinstance(first, dict) and first.get("type") in (
            "MatchClause",
            "ReturnStatement",
            "With",
            "Unwind",
        ):
            return {"type": "ExistsSubquery", "clauses": list(args)}
        return first
