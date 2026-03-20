"""Mixin classes for CypherASTTransformer rule groups.

Splits the monolithic 178-method CypherASTTransformer into focused,
composable mixin classes.  Each mixin groups methods for a related set
of grammar rules.  ``CypherASTTransformer`` inherits from all mixins,
preserving Lark's method-name-based visitor pattern via Python MRO.

Architecture
------------

::

    LiteralRulesMixin          — number, string, boolean, null, list, map, parameter
    ExpressionRulesMixin       — operators, boolean/comparison/arithmetic/string/null expressions
    FunctionRulesMixin         — function invocation, CASE, list/pattern comprehension, reduce, quantifiers, map projection
    PatternRulesMixin          — node/relationship patterns, labels, properties, path lengths
    ClauseRulesMixin           — MATCH, RETURN, WITH, SET, DELETE, CREATE, MERGE, UNION, ORDER BY, etc.
    ┆
    CypherASTTransformer(ClauseRulesMixin, PatternRulesMixin, FunctionRulesMixin, ExpressionRulesMixin, LiteralRulesMixin, Transformer)
        └── grammar string + GrammarParser (grammar_parser.py)
"""

from __future__ import annotations

from typing import Any

from lark import Token, Tree


class LiteralRulesMixin:
    """Grammar rule methods for literal values, parameters, and variable names.

    Handles:
    - Number literals (signed, unsigned, hex, octal, float, Inf, NaN)
    - String literals (escape sequences)
    - Boolean literals (TRUE / FALSE)
    - NULL literal
    - List literals and list elements
    - Map literals, entries, and individual entries
    - Parameter references ($name, $0)
    - Variable name normalization (backtick stripping)
    """

    # ========================================================================
    # Number literals
    # ========================================================================

    def number_literal(self, args: list[Any]) -> int | float:
        """Transform number literals (integers and floats) into Python values.

        Pass-through — actual conversion happens in signed_number / unsigned_number.

        Args:
            args: Single numeric value from signed_number or unsigned_number.

        Returns:
            Python int or float value, or 0 as fallback.

        """
        return args[0] if args else 0

    def signed_number(self, args: list[Any]) -> int | float | str:
        """Transform signed number literals into Python int or float values.

        Supports integers (-42, +100, -0x2A), floats (-3.14, +2.5e10),
        special values (-INF, +INFINITY, -NAN), and underscore separators.

        Args:
            args: Single signed number token.

        Returns:
            Python int, float, or special float value (inf/-inf/nan).

        """
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
        """Transform unsigned number literals into Python int or float values.

        Supports decimal, hex (0x), octal (0o), float, special values,
        and underscore separators.

        Args:
            args: Single unsigned number token.

        Returns:
            Python int, float, or special float value (inf/nan).

        """
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

    # ========================================================================
    # String / Boolean / Null literals
    # ========================================================================

    def string_literal(self, args: list[Any]) -> dict[str, Any]:
        r"""Transform string literals into structured AST nodes.

        Handles quote removal and escape sequences (\\n, \\t, \\r, \\\\, \\', \\").

        Args:
            args: Single string token with quotes.

        Returns:
            Dict with ``type="StringLiteral"`` and the processed string value.

        """
        s = str(args[0])
        if s.startswith(("'", '"')):
            s = s[1:-1]
        s = s.replace("\\n", "\n").replace("\\t", "\t").replace("\\r", "\r")
        s = s.replace("\\\\", "\\").replace("\\'", "'").replace('\\"', '"')
        return {"type": "StringLiteral", "value": s}

    def true(self, args: list[Any]) -> bool:
        """Transform TRUE keyword into Python True."""
        return True

    def false(self, args: list[Any]) -> bool:
        """Transform FALSE keyword into Python False."""
        return False

    def null_literal(self, args: list[Any]) -> dict[str, Any]:
        """Transform NULL keyword into a NullLiteral AST dict.

        Returns a typed dict rather than Python None to distinguish explicit
        null expressions from absent/optional AST fields.

        """
        return {"type": "NullLiteral"}

    # ========================================================================
    # List literals
    # ========================================================================

    def list_literal(self, args: list[Any]) -> list[Any]:
        """Transform list literal syntax [...] into Python list.

        Args:
            args: list_elements node containing list of element expressions.

        Returns:
            Python list of element values (empty list if no elements).

        """
        return next((a for a in args if isinstance(a, list)), [])

    def list_elements(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated list elements into Python list.

        Args:
            args: Individual element expression nodes.

        Returns:
            Python list of element expressions.

        """
        return list(args) if args else []

    # ========================================================================
    # Map literals
    # ========================================================================

    def map_literal(self, args: list[Any]) -> dict[str, Any]:
        """Transform map literal syntax {...} into MapLiteral AST dict.

        Args:
            args: map_entries node containing dict of key-value pairs.

        Returns:
            Dict with ``type="MapLiteral"`` and ``"value"`` key.

        """
        entries = next(
            (a for a in args if isinstance(a, dict) and "entries" in str(a)),
            {"entries": {}},
        )
        return {"type": "MapLiteral", "value": entries.get("entries", {})}

    def map_entries(self, args: list[Any]) -> dict[str, dict[str, Any]]:
        """Transform comma-separated map entries into an entries wrapper dict.

        Args:
            args: Individual map_entry nodes with key and value.

        Returns:
            Dict with ``"entries"`` key containing the collected key-value map.

        """
        result: dict[str, Any] = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"entries": result}

    def map_entry(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single map entry (key: value) into a key-value dict.

        Args:
            args: [property_name, value_expression].

        Returns:
            Dict with ``"key"`` and ``"value"`` keys.

        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    # ========================================================================
    # Parameters
    # ========================================================================

    def parameter(self, args: list[Any]) -> dict[str, Any]:
        """Transform parameter reference ($param) into a Parameter AST node.

        Args:
            args: Parameter name (string identifier or integer index).

        Returns:
            Dict with ``type="Parameter"`` and the parameter name.

        """
        name = args[0] if args else None
        return {"type": "Parameter", "name": name}

    def parameter_name(self, args: list[Any]) -> int | str:
        """Extract and normalize parameter name or index.

        Numeric indices are parsed as integers; identifiers have backticks stripped.

        Args:
            args: Parameter name token (identifier or number).

        Returns:
            Integer for numeric indices, string for named parameters.

        """
        s = str(args[0])
        try:
            return int(s)
        except ValueError:
            return s.strip("`")

    # ========================================================================
    # Variable name
    # ========================================================================

    def variable_name(self, args: list[Any]) -> str:
        """Extract and normalize variable identifier names.

        Strips backticks from escaped identifiers and converts Token to str.

        Args:
            args: Single identifier token (may have backticks).

        Returns:
            Variable name as a string with backticks removed.

        """
        return str(args[0]).strip("`")


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

        This method is called when the grammar matches the IS NULL pattern.
        It returns a consistent string representation that can be used by
        null_predicate_expression to create the appropriate NullCheck node.

        Returning a constant string is necessary because the grammar rule produces
        this as an alias for null_check_op, and the parent expression handler needs
        a standardized operator value to construct the AST node.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NULL".

        """
        return "IS NULL"

    def is_not_null(self, args: list[Any]) -> str:
        """Return the IS NOT NULL operator constant.

        This method is called when the grammar matches the IS NOT NULL pattern.
        It returns a consistent string representation that can be used by
        null_predicate_expression to create the appropriate NullCheck node.

        Returning a constant string is necessary because the grammar rule produces
        this as an alias for null_check_op, and the parent expression handler needs
        a standardized operator value to construct the AST node. The distinction
        from IS NULL is critical for correct null checking semantics.

        Args:
            args: Not used (rule has no child nodes).

        Returns:
            String constant "IS NOT NULL".

        """
        return "IS NOT NULL"

    def add_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform addition and subtraction arithmetic expressions.

        Addition and subtraction have equal precedence and associate left-to-right.
        This method builds a left-associative tree of Arithmetic nodes, which is
        necessary for correct evaluation order (e.g., a - b + c = (a - b) + c).

        Multiple operations are chained by iterating through operator-operand pairs
        and nesting the previous result as the left operand. This structure enables
        type checking to verify that all operands are numeric, and allows optimization
        passes to simplify constant expressions.

        Single operands pass through unchanged to avoid wrapping simple values in
        unnecessary AST nodes, improving efficiency for common cases like literals
        or variables without arithmetic operations.

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
        addition/subtraction, and associate left-to-right. This method builds
        a left-associative tree to preserve evaluation order (e.g., a / b * c = (a / b) * c).

        Higher precedence than addition is enforced by the grammar structure where
        mult_expression is a child of add_expression. This precedence hierarchy is
        necessary to correctly parse expressions like 2 + 3 * 4 as 2 + (3 * 4).

        Division by zero and modulo by zero are runtime errors that cannot be
        detected during parsing, so the AST structure allows these operations and
        defers validation to execution time or static analysis passes.

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
        associates left-to-right (though mathematically it's often right-associative,
        Cypher follows left-to-right). This method builds a left-associative tree
        for expressions like a ^ b ^ c, evaluating as (a ^ b) ^ c.

        Higher precedence than multiplication is enforced by the grammar where
        power_expression is a child of mult_expression. This ensures 2 * 3 ^ 4
        is correctly parsed as 2 * (3 ^ 4), not (2 * 3) ^ 4.

        The ^ operator can produce very large numbers or complex mathematical edge
        cases (negative base with fractional exponent), so validation and runtime
        overflow handling may be needed during execution.

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

        Unary operators apply to a single operand and have the highest precedence
        among all operators (including exponentiation). The unary minus negates a
        value (e.g., -5, -x), while unary plus is a no-op that promotes to numeric
        type (e.g., +"5" might convert a string to number in some contexts).

        This method creates a Unary node only when a sign operator is present.
        Without a unary operator, it passes through the postfix expression unchanged,
        avoiding unnecessary wrapping. This is necessary for efficient AST structure
        since most expressions don't have unary operators.

        Multiple unary operators can be chained (e.g., --5, +-3), though this is
        rare in practice. The current implementation handles this by nesting Unary
        nodes, though optimization passes could simplify these (-- becomes identity).

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
        building a chain of access operations. Examples:
        - Property access: person.name
        - Index access: list[0]
        - Slicing: list[1..3]
        - Chained: person.addresses[0].city

        This method iteratively applies postfix operations to build a left-associative
        tree. Each operation uses the previous result as its base object. This
        structure is necessary for:
        1. Type checking - verifying each intermediate result supports the next operation
        2. Execution planning - determining optimal access paths
        3. Null safety - detecting where null pointer exceptions could occur

        The transformation converts intermediate PropertyLookup/IndexLookup/Slicing
        nodes into final PropertyAccess/IndexAccess/Slice nodes that include both
        the object being accessed and the accessor (property name, index, or range).

        Single operands without postfix operations pass through unchanged.

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

        The grammar defines postfix_op as a union of property_lookup, index_lookup,
        and slicing. This method acts as a simple pass-through to avoid adding
        unnecessary wrapper nodes in the AST.

        Pass-through is necessary because the actual semantic transformation happens
        in postfix_expression, which combines the operator with its target object.
        This separation of parsing (recognizing the operator) from transformation
        (building the access node) keeps the grammar clean and the AST well-structured.

        Args:
            args: Single postfix operation node (PropertyLookup, IndexLookup, or Slicing).

        Returns:
            The postfix operation unchanged.

        """
        return args[0] if args else None

    def property_lookup(self, args: list[Any]) -> dict[str, Any]:
        """Transform property lookup syntax (.property_name) into intermediate node.

        Property lookup accesses a named property on a node, relationship, or map.
        Example: person.name, edge.weight, config.timeout

        This method creates an intermediate PropertyLookup node containing just the
        property name. The actual PropertyAccess node (which includes the object
        being accessed) is created by postfix_expression when it combines this with
        the target object.

        This two-step transformation is necessary because:
        1. The grammar parses .name separately from the object
        2. Multiple property accesses can chain: obj.prop1.prop2
        3. postfix_expression needs to build the chain left-to-right

        The property name comes from the property_name rule, which has already
        stripped backticks and normalized the identifier.

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

        Index lookup accesses an element by position in a list or by key in a map.
        Examples: list[0], map['key'], items[i+1]

        The index expression is evaluated at runtime and can be:
        - Integer for list access (0-based indexing)
        - String for map key access
        - Any expression that evaluates to an appropriate index type

        This method creates an intermediate IndexLookup node containing just the
        index expression. The actual IndexAccess node (which includes the collection
        being indexed) is created by postfix_expression when combining with the target.

        The two-step approach is necessary for the same reasons as property_lookup:
        enabling chained access operations and left-to-right evaluation.

        Negative indices and out-of-bounds access are runtime errors that cannot
        be detected during parsing, so validation is deferred to execution time.

        Args:
            args: Index expression that evaluates to the position/key.

        Returns:
            Dict with type "IndexLookup" and the index expression.

        """
        return {"type": "IndexLookup", "index": args[0] if args else None}

    def slicing(self, args: list[Any]) -> dict[str, Any]:
        """Transform list slicing syntax ([from..to]) into intermediate node.

        Slicing extracts a sub-list from a list using range notation. Examples:
        - list[1..3] - elements at indices 1 and 2 (end exclusive)
        - list[..5] - first 5 elements (indices 0-4)
        - list[2..] - from index 2 to end
        - list[..] - entire list (copy)

        Both from and to expressions are optional. When omitted:
        - Missing from defaults to start of list (0)
        - Missing to defaults to end of list
        - Both missing creates a copy of the entire list

        This method creates an intermediate Slicing node with the range bounds.
        The actual Slice node (including the list being sliced) is created by
        postfix_expression. This separation is necessary for supporting chained
        operations like list[1..3][0] (get first element of a slice).

        Negative indices, reverse ranges, and out-of-bounds handling are runtime
        behaviors that vary by implementation and cannot be validated during parsing.

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

        COUNT(*) counts all rows/matches, including duplicates and null values.
        This is different from COUNT(expression) which excludes nulls. The special
        handling is necessary because * is not a regular expression - it's a syntactic
        marker meaning "count everything."

        This method creates a dedicated CountStar node rather than treating it as
        a regular function invocation. This distinction is important for:
        1. Query optimization - COUNT(*) can often be computed more efficiently
        2. Type checking - CountStar always returns an integer, no expression to validate
        3. Execution planning - Some databases have optimized COUNT(*) implementations

        COUNT(*) is typically used in aggregate queries with GROUP BY or as a simple
        row count: MATCH (n:Person) RETURN COUNT(*)

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
        This is essential for pattern existence checks without needing to collect
        actual matched data. Example:

        MATCH (person:Person)
        WHERE EXISTS { MATCH (person)-[:KNOWS]->(friend) }
        RETURN person

        The subquery can contain:
        - Pattern matching: EXISTS { (a)-[:KNOWS]->(b) }
        - Full queries: EXISTS { MATCH (n) WHERE n.age > 30 RETURN n }

        EXISTS is necessary for efficient existence checks because:
        1. It short-circuits on first match (doesn't need to find all results)
        2. It doesn't materialize data (no memory overhead for large result sets)
        3. It can use specialized indexes for existence tests

        This is analogous to SQL's EXISTS (SELECT ...) but uses Cypher pattern syntax.

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
            wrapping a single ``PathPattern`` → ``PatternElement``.

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

        This method passes through the parsed content without modification, as the
        structure has already been built by the appropriate clause transformers.
        Pass-through is necessary to avoid double-wrapping the subquery.

        The grammar allows both forms to provide flexibility:
        - Simple: EXISTS { (a)-[:KNOWS]->(b) WHERE b.age > 30 }
        - Full: EXISTS { MATCH (a)-[:KNOWS]->(b) WHERE b.age > 30 RETURN b }

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


class FunctionRulesMixin:
    """Grammar rule methods for function invocations and complex expressions.

    Handles:
    - Function invocation (built-in, user-defined, namespaced)
    - Function arguments and DISTINCT modifier
    - CASE expressions (simple and searched forms)
    - List comprehension (variable IN list WHERE cond | projection)
    - Pattern comprehension (graph pattern matching into lists)
    - REDUCE expression (fold/reduce over lists)
    - Quantifier expressions (ALL, ANY, SINGLE, NONE)
    - Map projection (property selection and transformation)
    """

    def function_invocation(self, args: list[Any]) -> dict[str, Any]:
        """Transform function invocation (built-in or user-defined functions).

        Functions are called with parentheses syntax: function_name(arg1, arg2, ...).
        Functions can be:
        - Built-in: count(), sum(), avg(), min(), max(), collect(), etc.
        - User-defined: custom functions registered in the database
        - Namespaced: db.labels(), apoc.create.node(), etc.

        This method creates a FunctionInvocation node containing:
        1. Function name (may include namespace for qualified names)
        2. Arguments (list of expressions, may include DISTINCT flag)

        The separation of name and arguments is necessary for:
        - Function resolution (finding the right function implementation)
        - Type checking (validating argument types match function signature)
        - Query optimization (some functions can be pre-computed or optimized)

        The "unknown" default for missing names handles edge cases in malformed queries
        and provides a fallback for error reporting.

        Args:
            args: [function_name, function_args] where args may be None for no arguments.

        Returns:
            Dict with type "FunctionInvocation" containing name and arguments.

        """
        name = args[0] if args else "unknown"
        func_args = args[1] if len(args) > 1 else None
        return {
            "type": "FunctionInvocation",
            "name": name,
            "arguments": func_args,
        }

    def function_args(
        self,
        args: list[Any],
    ) -> dict[str, bool | list[Any]]:
        """Transform function arguments with optional DISTINCT modifier.

        Function arguments can have a DISTINCT modifier for aggregation functions:
        - COUNT(DISTINCT n.name) - counts unique values only
        - COLLECT(DISTINCT n.label) - collects unique values into a list

        The DISTINCT modifier is only meaningful for certain aggregate functions
        (COUNT, COLLECT, SUM, AVG), but the parser allows it on any function.
        Semantic validation of appropriate DISTINCT usage happens during type checking.

        This method extracts:
        1. distinct flag - whether DISTINCT keyword is present
        2. arguments list - the actual expression arguments

        Separating these components is necessary because the execution behavior
        differs significantly: DISTINCT requires deduplication logic which affects
        performance and memory usage.

        Args:
            args: Mix of "DISTINCT" keyword string and function_arg_list.

        Returns:
            Dict with "distinct" boolean flag and "arguments" list.

        """
        # After the grammar change, distinct_keyword produces the string "DISTINCT"
        # as a named rule result.  Also accept bare Token/str for backwards compat.
        distinct = any(
            (isinstance(a, str) and a.upper() == "DISTINCT") for a in args
        )
        arg_list = next((a for a in args if isinstance(a, list)), [])
        return {"distinct": distinct, "arguments": arg_list}

    def function_arg_list(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated function argument expressions into a list.

        Function arguments are arbitrary expressions that can include:
        - Literals: sum(1, 2, 3)
        - Variables: max(n.age, m.age)
        - Nested function calls: round(avg(n.score), 2)
        - Complex expressions: count(n.x + n.y * 2)

        Converting to a list is necessary for:
        1. Consistent iteration during execution
        2. Arity checking (validating correct number of arguments)
        3. Type checking each argument against function signature

        Empty argument lists are represented as [] rather than None, which simplifies
        downstream code that needs to iterate over arguments (no null checks needed).

        Args:
            args: Individual expression nodes for each argument.

        Returns:
            List of argument expressions (empty list if no arguments).

        """
        return list(args)

    def function_name(self, args: list[Any]) -> str | dict[str, str]:
        """Transform function name with optional namespace qualification.

        Function names can be simple (sum, count) or namespaced (db.labels, apoc.create.node).
        Namespaces organize functions into logical groups and prevent naming conflicts:
        - db.* - database introspection functions
        - apoc.* - APOC procedure library (third-party)
        - custom.* - user-defined namespaces

        This method returns:
        - Simple string for unqualified names: "count"
        - Dict with namespace and name for qualified names: {namespace: "db", name: "labels"}

        The distinction is necessary for:
        1. Function resolution - different namespaces may have same function name
        2. Permission checking - namespaced functions may have different access controls
        3. Error reporting - qualified names provide better context in error messages

        The "unknown" fallback handles malformed queries gracefully.

        Args:
            args: [namespace_name, simple_name] or just [simple_name].

        Returns:
            Simple name string, or dict with namespace and name for qualified functions.

        """
        namespace = args[0] if len(args) > 1 else None
        simple_name = args[-1] if args else "unknown"
        return (
            {"namespace": namespace, "name": simple_name}
            if namespace
            else simple_name
        )

    def namespace_name(self, args: list[Any]) -> str:
        """Transform namespace path into a dot-separated string.

        Namespaces can be multi-level: db.schema.nodeTypeProperties
        The grammar parses these as multiple identifiers separated by dots.
        This method joins them with dots and strips backticks from each part.

        Joining is necessary to create a canonical namespace string for function
        lookup. Stripping backticks normalizes identifiers (backticks allow special
        characters but aren't part of the actual name).

        Example: `my-custom`.`my-function` becomes "my-custom.my-function"

        Args:
            args: List of identifier tokens forming the namespace path.

        Returns:
            Dot-separated namespace string with backticks removed.

        """
        return ".".join(str(a).strip("`") for a in args)

    def function_simple_name(self, args: list[Any]) -> str:
        """Extract the unqualified function name identifier.

        The simple name is the final component of a potentially namespaced function.
        For example, in db.labels(), "labels" is the simple name.

        Stripping backticks is necessary to normalize identifier representation.
        Backticks allow identifiers with special characters or reserved words,
        but the backticks themselves are not part of the semantic name.

        Converting to string handles both Token objects from the parser and any
        other string-like representations.

        Args:
            args: Single identifier token for the function name.

        Returns:
            Function name as a string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # Case expression
    # ========================================================================

    def case_expression(self, args: list[Any]) -> Any | None:
        """Transform CASE expression (simple or searched form).

        CASE expressions provide conditional logic similar to if-then-else or switch
        statements. There are two forms:

        1. Simple CASE - compares one expression against multiple values:
           CASE n.status WHEN 'active' THEN 1 WHEN 'pending' THEN 0 ELSE -1 END

        2. Searched CASE - evaluates multiple boolean conditions:
           CASE WHEN n.age < 18 THEN 'minor' WHEN n.age < 65 THEN 'adult' ELSE 'senior' END

        This method acts as a pass-through because the grammar has already dispatched
        to the appropriate specific handler (simple_case or searched_case). Pass-through
        is necessary to avoid adding unnecessary wrapper nodes in the AST.

        CASE expressions are essential for data transformation and conditional logic
        within queries, enabling computed columns and complex filtering.

        Args:
            args: Single node (SimpleCase or SearchedCase) from the specific rule.

        Returns:
            The SimpleCase or SearchedCase node unchanged.

        """
        return args[0] if args else None

    def simple_case(self, args: list[Any]) -> dict[str, Any]:
        """Transform simple CASE expression that matches an operand against values.

        Simple CASE syntax: CASE expression WHEN value1 THEN result1 [WHEN ...] [ELSE default] END

        The operand expression is evaluated once, then compared against each WHEN value
        sequentially until a match is found. The corresponding THEN result is returned.
        If no WHEN matches, the ELSE value is returned (or NULL if no ELSE clause).

        This is analogous to a switch statement in programming languages. It's more
        concise than searched CASE when you're comparing one expression against
        multiple constant values.

        The structure separates:
        - operand: the expression being compared (evaluated once)
        - when clauses: list of value-result pairs (evaluated sequentially)
        - else clause: default result if no matches (optional)

        This separation is necessary for:
        1. Optimized execution (operand evaluated only once)
        2. Type checking (all WHEN values must be comparable to operand)
        3. Short-circuit evaluation (stop at first match)

        Args:
            args: [operand_expression, when_clause1, when_clause2, ..., optional_else_clause].

        Returns:
            Dict with type "SimpleCase" containing operand, when clauses list, and optional else.

        """
        operand = args[0] if args else None
        when_clauses = [
            a
            for a in args[1:]
            if isinstance(a, dict) and a.get("type") == "SimpleWhen"
        ]
        else_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Else"
            ),
            None,
        )
        return {
            "type": "SimpleCase",
            "operand": operand,
            "when": when_clauses,
            "else": else_clause,
        }

    def searched_case(self, args: list[Any]) -> dict[str, Any]:
        """Transform searched CASE expression that evaluates boolean conditions.

        Searched CASE syntax: CASE WHEN condition1 THEN result1 [WHEN ...] [ELSE default] END

        Each WHEN clause contains a boolean condition that is evaluated sequentially.
        The first condition that evaluates to true determines the result. If no
        conditions are true, the ELSE value is returned (or NULL if no ELSE clause).

        This is analogous to if-else-if chains in programming languages. It's more
        flexible than simple CASE because each condition can be a completely different
        boolean expression (not just equality tests).

        The structure contains:
        - when clauses: list of condition-result pairs (evaluated sequentially)
        - else clause: default result if no conditions are true (optional)

        This separation is necessary for:
        1. Short-circuit evaluation (stop at first true condition)
        2. Type checking (all THEN results should have compatible types)
        3. Optimization (conditions can be reordered if independent)

        Args:
            args: [when_clause1, when_clause2, ..., optional_else_clause].

        Returns:
            Dict with type "SearchedCase" containing when clauses list and optional else.

        """
        when_clauses = [
            a
            for a in args
            if isinstance(a, dict) and a.get("type") == "SearchedWhen"
        ]
        else_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Else"
            ),
            None,
        )
        return {
            "type": "SearchedCase",
            "when": when_clauses,
            "else": else_clause,
        }

    def simple_when(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHEN clause in a simple CASE expression.

        Simple WHEN syntax: WHEN value1, value2, ... THEN result

        In simple CASE, a WHEN clause can match multiple values (comma-separated).
        The operand is compared against each value, and if any match, the result
        is returned. This is a shorthand for multiple WHEN clauses with the same result.

        Example::

            CASE n.status
              WHEN 'active', 'verified' THEN 'good'
              WHEN 'pending', 'new' THEN 'waiting'
            END

        The structure contains:

        - operands: list of values to compare against (evaluated left to right)
        - result: expression to return if any operand matches

        This separation is necessary for:

        1. Efficient matching (can use IN-style lookups for multiple values)
        2. Type checking (all operands must be comparable to CASE operand)
        3. Execution planning (can optimize multiple equality tests)

        Args:
            args: [when_operands_list, result_expression].

        Returns:
            Dict with type "SimpleWhen" containing operands list and result expression.

        """
        operands = args[0] if args else []
        result = args[1] if len(args) > 1 else None
        return {"type": "SimpleWhen", "operands": operands, "result": result}

    def searched_when(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHEN clause in a searched CASE expression.

        Searched WHEN syntax: WHEN condition THEN result

        In searched CASE, each WHEN clause has a boolean condition that is
        evaluated independently. The first WHEN with a true condition determines
        the result. This provides maximum flexibility for conditional logic.

        Example::

            CASE
              WHEN n.age < 18 THEN 'minor'
              WHEN n.age >= 65 THEN 'senior'
              WHEN n.employed = true THEN 'working adult'
              ELSE 'adult'
            END

        The structure contains:

        - condition: boolean expression to evaluate
        - result: expression to return if condition is true

        This separation is necessary for:

        1. Short-circuit evaluation (stop evaluating after first true)
        2. Independent condition evaluation (each can access different variables)
        3. Type checking (condition must be boolean, result type must match other WHENs)

        Args:
            args: [condition_expression, result_expression].

        Returns:
            Dict with type "SearchedWhen" containing condition and result expressions.

        """
        condition = args[0] if args else None
        result = args[1] if len(args) > 1 else None
        return {
            "type": "SearchedWhen",
            "condition": condition,
            "result": result,
        }

    def when_operands(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated operands in a simple CASE WHEN clause.

        Multiple operands allow matching against any of several values in one WHEN.
        This is a convenience feature that reduces verbosity when multiple values
        should produce the same result.

        Converting to a list is necessary for:
        1. Iteration during execution (test each value for match)
        2. Type checking (all values must be comparable to CASE operand)
        3. Optimization (can use set-based lookup for many values)

        Args:
            args: Individual expression nodes for each value to test.

        Returns:
            List of operand expressions.

        """
        return list(args)

    def else_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform the ELSE clause in a CASE expression.

        The ELSE clause provides a default value when no WHEN conditions match.
        If ELSE is omitted and no WHEN matches, the result is NULL.

        Wrapping in a typed dict is necessary to distinguish the ELSE value from
        regular expressions during AST traversal. The "Else" type marker helps
        the transformer identify and extract this special clause.

        The ELSE value can be any expression, including:
        - Literals: ELSE 'unknown'
        - Variables: ELSE n.default_value
        - Nested expressions: ELSE CASE ... END (nested CASE)

        Args:
            args: Single expression for the default value.

        Returns:
            Dict with type "Else" containing the default value expression.

        """
        return {"type": "Else", "value": args[0] if args else None}

    # ========================================================================
    # List comprehension
    # ========================================================================

    def list_comprehension(self, args: list[Any]) -> dict[str, Any]:
        """Transform list comprehension expression for filtering and mapping lists.

        List comprehension syntax: [variable IN list WHERE condition | projection]

        List comprehensions provide a concise way to transform lists by:
        1. Iterating over elements (variable IN list)
        2. Optionally filtering (WHERE condition)
        3. Optionally transforming (| projection)

        Examples:
        - [x IN [1,2,3] WHERE x > 1] -> [2, 3]  (filter only)
        - [x IN [1,2,3] | x * 2] -> [2, 4, 6]  (map only)
        - [x IN range(1,5) WHERE x % 2 = 0 | x^2] -> [4, 16]  (filter and map)

        This is similar to list comprehensions in Python/JavaScript and provides
        functional programming capabilities within Cypher. It's necessary for:
        - Data transformation without external functions
        - Inline filtering of collected results
        - Building computed lists in RETURN clauses

        The structure contains:
        - variable: iteration variable name
        - in: source list expression
        - where: optional filter condition
        - projection: optional transformation expression

        Args:
            args: [variable, source_list, optional_filter, optional_projection].

        Returns:
            Dict with type "ListComprehension" containing all components.

        """
        variable = args[0] if args else None
        source = args[1] if len(args) > 1 else None

        # list_filter and list_projection are both optional and both produce
        # expression dicts.  list_filter wraps its result as {"_lc_filter": e}
        # and list_projection as {"_lc_projection": e} so we can tell them
        # apart even when only one is present.
        filter_expr = None
        projection = None
        for arg in args[2:]:
            if isinstance(arg, dict) and "_lc_filter" in arg:
                filter_expr = arg["_lc_filter"]
            elif isinstance(arg, dict) and "_lc_projection" in arg:
                projection = arg["_lc_projection"]

        return {
            "type": "ListComprehension",
            "variable": variable,
            "in": source,
            "where": filter_expr,
            "projection": projection,
        }

    def list_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable name from a list comprehension.

        The list variable is the identifier used to reference each element during
        iteration. It's scoped to the comprehension and shadows any outer variable
        with the same name.

        Pass-through is necessary because variable_name has already normalized the
        identifier (stripped backticks, etc.), and we don't want to add extra wrapping.

        Args:
            args: Variable name string from variable_name rule.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    def list_filter(self, args: list[Any]) -> Any | None:
        """Extract the WHERE filter expression from a list comprehension.

        Wraps the expression in a sentinel dict so that
        :meth:`list_comprehension` can distinguish a bare filter from a bare
        projection when only one of the two optional clauses is present.

        Args:
            args: Boolean filter expression.

        Returns:
            ``{"_lc_filter": expr}`` sentinel dict.

        """
        return {"_lc_filter": args[0]} if args else None

    def list_projection(self, args: list[Any]) -> Any | None:
        """Extract the projection expression from a list comprehension.

        Wraps the expression in a sentinel dict so that
        :meth:`list_comprehension` can distinguish a bare projection from a
        bare filter when only one of the two optional clauses is present.

        Args:
            args: Projection expression to transform each element.

        Returns:
            ``{"_lc_projection": expr}`` sentinel dict.

        """
        return {"_lc_projection": args[0]} if args else None

    # ========================================================================
    # Pattern comprehension
    # ========================================================================

    def pattern_comprehension(self, args: list[Any]) -> dict[str, Any]:
        """Transform pattern comprehension for collecting results from pattern matching.

        Pattern comprehension syntax: [path_var = pattern WHERE condition | projection]

        Pattern comprehensions match a graph pattern multiple times and collect
        results into a list. This is essential for inline subqueries without OPTIONAL MATCH.

        Examples::

            [(person)-[:KNOWS]->(friend) | friend.name]
            [p = (a)-[:KNOWS*1..3]->(b) WHERE b.age > 30 | length(p)]

        The structure contains:

        - variable: optional variable for the entire path
        - pattern: graph pattern to match repeatedly
        - where: optional filter on matched patterns
        - projection: expression to collect (required, unlike list comprehension)

        This is necessary for:
        - Collecting related data without explicit MATCH/COLLECT
        - Nested pattern matching within expressions
        - Building complex aggregations inline

        Pattern comprehensions are more powerful than list comprehensions because
        they can match graph structures, not just iterate over lists.

        Args:
            args: Mix of optional variable, pattern element, optional where, and projection.

        Returns:
            Dict with type "PatternComprehension" containing all components.

        """
        variable = None
        pattern = None
        filter_expr = None
        projection = None

        for arg in args:
            if isinstance(arg, str) and variable is None:
                variable = arg
            elif isinstance(arg, dict):
                if arg.get("type") == "PatternElement" and pattern is None:
                    pattern = arg
                elif (
                    arg.get("type") == "PatternFilter" and filter_expr is None
                ):
                    # Tagged sentinel from pattern_filter — extract inner expression
                    filter_expr = arg.get("expression")
                elif projection is None:
                    projection = arg

        return {
            "type": "PatternComprehension",
            "variable": variable,
            "pattern": pattern,
            "where": filter_expr,
            "projection": projection,
        }

    def pattern_comp_variable(self, args: list[Any]) -> Any | None:
        """Extract the optional path variable from a pattern comprehension.

        The path variable captures the entire matched path, which can be useful
        for computing path properties (length, nodes, relationships) in the projection.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    def pattern_filter(self, args: list[Any]) -> Any | None:
        """Extract the WHERE filter from a pattern comprehension.

        Wraps the expression in a ``{"type": "PatternFilter", "expression": ...}``
        sentinel so that ``pattern_comprehension`` can distinguish the WHERE
        expression from the projection expression when both are dicts.

        Args:
            args: Boolean filter expression.

        Returns:
            Tagged dict ``{"type": "PatternFilter", "expression": expr}`` or
            ``None`` if no args.

        """
        if not args:
            return None
        return {"type": "PatternFilter", "expression": args[0]}

    def pattern_projection(self, args: list[Any]) -> Any | None:
        """Extract the projection expression from a pattern comprehension.

        The projection specifies what to collect from each matched pattern.
        It's required (unlike list comprehension where it's optional) because
        collecting the entire pattern match isn't meaningful by default.

        Pass-through is necessary to avoid double-wrapping.

        Args:
            args: Projection expression.

        Returns:
            Projection expression unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Reduce expression
    # ========================================================================

    def reduce_expression(self, args: list[Any]) -> dict[str, Any]:
        """Transform REDUCE expression for list aggregation with accumulator.

        REDUCE syntax: REDUCE(accumulator = initial, variable IN list | step_expression)

        REDUCE is a functional programming construct that aggregates a list into
        a single value by applying a step expression iteratively. It's analogous
        to fold/reduce in functional languages.

        Example:
        REDUCE(sum = 0, x IN [1,2,3,4,5] | sum + x) -> 15
        REDUCE(product = 1, x IN [1,2,3,4] | product * x) -> 24
        REDUCE(max = -999999, x IN numbers | CASE WHEN x > max THEN x ELSE max END)

        The structure contains:
        - accumulator: {variable: name, init: initial_value} for the aggregated result
        - variable: iteration variable name for each list element
        - in: source list expression
        - step: expression that computes next accumulator value (can reference both variables)

        This is necessary for:
        - Custom aggregation logic not provided by built-in functions
        - Stateful list processing (each step can use previous result)
        - Complex computations that require iteration context

        The accumulator is updated in each iteration: accumulator = step_expression,
        where step_expression can reference both the current accumulator value and
        the current list element.

        Args:
            args: [accumulator_dict, iteration_variable, source_list, step_expression].

        Returns:
            Dict with type "Reduce" containing accumulator, variable, source, and step.

        """
        accumulator = args[0] if args else None
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        step = args[3] if len(args) > 3 else None
        return {
            "type": "Reduce",
            "accumulator": accumulator,
            "variable": variable,
            "in": source,
            "step": step,
        }

    def reduce_accumulator(self, args: list[Any]) -> dict[str, Any]:
        """Transform the accumulator declaration in a REDUCE expression.

        Accumulator syntax: variable_name = initial_expression

        The accumulator holds the running result during iteration. It's initialized
        before the first iteration and updated after each step. The final accumulator
        value becomes the result of the entire REDUCE expression.

        The structure contains:
        - variable: accumulator variable name
        - init: initial value expression (evaluated once before iteration)

        Separating variable and initialization is necessary for:
        1. Scoping - accumulator variable is local to REDUCE
        2. Type inference - initial value determines accumulator type
        3. Execution - initialization happens exactly once

        Args:
            args: [variable_name, initialization_expression].

        Returns:
            Dict with variable name and init expression.

        """
        variable = args[0] if args else None
        init = args[1] if len(args) > 1 else None
        return {"variable": variable, "init": init}

    def reduce_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable from a REDUCE expression.

        The iteration variable represents each element from the source list during
        iteration. It's scoped to the REDUCE expression and can be referenced in
        the step expression.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Quantifier expressions
    # ========================================================================

    def quantifier_expression(self, args: list[Any]) -> dict[str, Any]:
        """Transform quantifier expressions (ALL, ANY, SINGLE, NONE) for predicate testing.

        Quantifier syntax: QUANTIFIER(variable IN list WHERE predicate)

        Quantifiers test whether a predicate holds for list elements:
        - ALL: true if predicate is true for every element (universal quantification)
        - ANY: true if predicate is true for at least one element (existential quantification)
        - SINGLE: true if predicate is true for exactly one element
        - NONE: true if predicate is false for all elements (negation of ANY)

        Examples:
        - ALL(x IN [2,4,6,8] WHERE x % 2 = 0) -> true
        - ANY(x IN [1,3,5,6] WHERE x % 2 = 0) -> true
        - SINGLE(x IN [1,2,3,4] WHERE x > 3) -> true
        - NONE(x IN [1,3,5,7] WHERE x % 2 = 0) -> true

        These are essential for:
        - Collection validation (checking constraints on all/some elements)
        - Existence tests (ANY is more efficient than collecting and checking length)
        - Uniqueness checking (SINGLE ensures exactly one match)

        The structure contains:
        - quantifier: which quantifier (ALL/ANY/SINGLE/NONE)
        - variable: iteration variable name
        - in: source list expression
        - where: predicate to test for each element

        This is similar to SQL's ALL/ANY operators and mathematical quantifiers (∀, ∃).

        Args:
            args: [quantifier_keyword, variable, source_list, predicate_expression].

        Returns:
            Dict with type "Quantifier" containing quantifier type, variable, source, and predicate.

        """
        quantifier = args[0] if args else "ALL"
        variable = args[1] if len(args) > 1 else None
        source = args[2] if len(args) > 2 else None
        predicate = args[3] if len(args) > 3 else None
        return {
            "type": "Quantifier",
            "quantifier": quantifier,
            "variable": variable,
            "in": source,
            "where": predicate,
        }

    def quantifier(self, args: list[Any]) -> str:
        """Extract and normalize the quantifier keyword (ALL, ANY, SINGLE, NONE).

        Quantifier keywords are case-insensitive in Cypher, so normalization to
        uppercase is necessary for consistent matching during execution.

        The default of "ALL" handles edge cases (though grammatically, a quantifier
        keyword is required).

        Args:
            args: Quantifier keyword token.

        Returns:
            Uppercased quantifier string: "ALL", "ANY", "SINGLE", or "NONE".

        """
        if not args:
            return "ALL"
        return str(args[0]).upper()

    def quantifier_variable(self, args: list[Any]) -> Any | None:
        """Extract the iteration variable from a quantifier expression.

        The iteration variable represents each element being tested against the
        predicate. It's scoped to the quantifier expression.

        Pass-through is necessary to avoid extra wrapping.

        Args:
            args: Variable name string.

        Returns:
            Variable name unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # Map projection
    # ========================================================================

    def map_projection(self, args: list[Any]) -> dict[str, Any]:
        """Transform map projection for selecting/transforming object properties.

        Map projection syntax: variable { property1, .property2, property3: expression, ...}

        Map projections create new maps by selecting and optionally transforming
        properties from a node, relationship, or map. This is essential for:
        - Shaping output data (selecting only needed properties)
        - Property transformation (renaming, computing derived values)
        - Creating anonymous objects in RETURN clauses

        Examples:
        - person { .name, .age } -> {name: person.name, age: person.age}
        - person { .*, age: person.age + 1 } -> all properties plus computed age
        - node { id: id(node), labels: labels(node) } -> custom object

        Elements can be:
        - Property selector: .name (copies property with same name)
        - Computed property: name: expression (evaluates expression for value)
        - Variable: other_var (includes all properties from other_var)
        - Wildcard: .* (includes all properties from base variable)

        The structure contains:
        - variable: base object to project from
        - elements: list of property selections/transformations

        This is similar to JavaScript object destructuring and provides a declarative
        way to shape data without manual property copying.

        Args:
            args: [variable_name, list_of_map_elements].

        Returns:
            Dict with type "MapProjection" containing variable and elements list.

        """
        variable = args[0] if args else None
        elements = args[1] if len(args) > 1 else []
        return {
            "type": "MapProjection",
            "variable": variable,
            "elements": elements,
        }

    def map_elements(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated map projection elements into a list.

        Map elements define what properties to include in the projected map.
        Converting to a list is necessary for iteration during map construction.

        Empty elements list creates an empty map {}.

        Args:
            args: Individual map_element nodes.

        Returns:
            List of map element specifications (empty list if no elements).

        """
        return list(args) if args else []

    def map_element(
        self,
        args: list[Any],
    ) -> dict[str, Any] | Any | None:
        """Transform a single map projection element.

        Map elements have different forms:
        1. Selector (string): property name or .* wildcard -> {"selector": name}
        2. Computed property (2 args): name: expression -> {"property": name, "value": expr}
        3. Pass-through: other forms parsed by grammar

        The different representations are necessary for execution to distinguish:
        - Which properties to copy (selectors)
        - Which properties to compute (computed)
        - Special operations (wildcard, variable inclusion)

        Args:
            args: Either [selector_string] or [property_name, value_expression] or other.

        Returns:
            Dict with appropriate structure for the element type.

        """
        match args:
            case [str() as selector]:
                return {"selector": selector}
            case [property_name, value]:
                return {"property": property_name, "value": value}
            case [single_arg]:
                return single_arg
            case _:
                return None


class PatternRulesMixin:
    """Grammar rule methods for graph pattern matching constructs.

    Handles:
    - Pattern and path pattern construction
    - Pattern elements and shortest path
    - Node patterns (labels, properties, inline WHERE)
    - Relationship patterns (direction, types, variable-length paths)
    - Label expressions (AND, OR, NOT on labels)
    - Property maps and property key-value pairs
    - Path length ranges for variable-length relationships
    """

    def pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a graph pattern (one or more paths).

        Patterns describe graph structures to match or create. Multiple comma-separated
        paths can be specified in one pattern (e.g., (a)-[]->(b), (c)-[]->(d)).

        This wrapper is necessary to distinguish pattern collections from individual
        paths during matching and creation operations.

        Args:
            args: List of path_pattern nodes.

        Returns:
            Dict with type "Pattern" containing list of paths.

        """
        return {"type": "Pattern", "paths": list(args)}

    def path_pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single path pattern with optional variable assignment.

        Paths can be assigned to variables (e.g., p = (a)-[]->(b)) for later reference.
        This is necessary for algorithms that operate on entire paths rather than
        individual nodes/relationships.

        Args:
            args: Optional variable name followed by pattern_element.

        Returns:
            Dict with type "PathPattern" containing optional variable and element.

        """
        variable = None
        element = None
        for arg in args:
            if isinstance(arg, str) and variable is None:
                variable = arg
            else:
                element = arg
        return {
            "type": "PathPattern",
            "variable": variable,
            "element": element,
        }

    def pattern_element(self, args: list[Any]) -> dict[str, Any]:
        """Transform a pattern element (sequence of nodes and relationships).

        Pattern elements describe connected graph structures, alternating between
        nodes and relationships. The parts list maintains order, which is necessary
        for directional relationship matching.

        Args:
            args: Alternating node_pattern and relationship_pattern nodes.

        Returns:
            Dict with type "PatternElement" containing ordered list of parts.

        """
        return {"type": "PatternElement", "parts": list(args)}

    def shortest_path(self, args: list[Any]) -> dict[str, Any]:
        """Transform SHORTESTPATH or ALLSHORTESTPATHS function.

        Shortest path functions find minimal-length paths between nodes.
        ALLSHORTESTPATHS finds all paths with minimal length. This distinction
        is necessary for different algorithmic execution strategies.

        Args:
            args: Mix of function name keyword and pattern parts.

        Returns:
            Dict with type "ShortestPath" containing all flag and pattern parts.

        """
        all_shortest = any(
            "ALL" in str(a).upper() for a in args if isinstance(a, str)
        )
        nodes_and_rel = [a for a in args if isinstance(a, dict)]
        return {
            "type": "ShortestPath",
            "all": all_shortest,
            "parts": nodes_and_rel,
        }

    # ========================================================================
    # Node pattern
    # ========================================================================

    def node_pattern(self, args: list[Any]) -> dict[str, Any]:
        """Transform a node pattern (node in parentheses).

        Node patterns describe nodes to match or create, with optional variable,
        labels, properties, and WHERE clause. Parentheses syntax () is required.

        The filler dict is merged into the node pattern to avoid extra nesting.
        This flattening is necessary for simpler AST traversal.

        Args:
            args: Optional node_pattern_filler dict with node components.

        Returns:
            Dict with type "NodePattern" containing variable, labels, properties, where.

        """
        filler = args[0] if args else {}
        return (
            {"type": "NodePattern", **filler}
            if isinstance(filler, dict)
            else {"type": "NodePattern", "filler": filler}
        )

    def node_pattern_filler(self, args: list[Any]) -> dict[str, Any]:
        """Extract components from inside node parentheses.

        Node patterns can contain: variable, labels, properties, and WHERE.
        These components are parsed separately and need to be combined into
        a single dict for the node pattern. Type detection is necessary to
        distinguish properties from other structured components.

        Args:
            args: Mix of variable name string and component dicts (labels/properties/where).

        Returns:
            Dict containing all node components with appropriate keys.

        """
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                # Check if this is a labels or where dict (has known keys)
                if "labels" in arg or "where" in arg:
                    filler.update(arg)
                # Check if this is a properties object (no special keys)
                elif "type" not in arg and arg:
                    # This is a properties dict - store it as 'properties'
                    filler["properties"] = arg
                else:
                    # Other structured objects
                    filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def node_labels(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform node label expressions.

        Nodes can have multiple labels (e.g., :Person:Employee). Labels can also
        use boolean logic (e.g., :Person|Employee for OR). Wrapping in a dict is
        necessary to distinguish labels from other node components.

        Args:
            args: List of label_expression nodes.

        Returns:
            Dict with "labels" key containing list of label expressions.

        """
        return {"labels": list(args)}

    def label_expression(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform a single label expression.

        Labels can be prefixed with : or IS. For simple cases, just return the
        label. Complex cases with multiple parts need wrapping.

        Args:
            args: Label term(s) from the expression.

        Returns:
            Single label term or dict with type "LabelExpression" for complex cases.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelExpression", "parts": list(args)}

    def label_term(self, args: list[Any]) -> Any | dict[str, Any]:
        """Transform label OR expressions (e.g., ``Person|Employee``).

        Label terms can use ``|`` for alternation (match either label). Single labels
        pass through; multiple labels are wrapped in a LabelOr node.

        Args:
            args: Label factor nodes separated by ``|``.

        Returns:
            Single factor or dict with type "LabelOr" for multiple factors.

        """
        if len(args) == 1:
            return args[0]
        return {"type": "LabelOr", "terms": list(args)}

    def label_factor(self, args: list[Any]) -> Any | None:
        """Pass through label factors (possibly negated with !).

        Label factors can have ! negation prefix. Pass-through avoids extra wrapping.

        Args:
            args: Single label_primary node.

        Returns:
            The label primary unchanged.

        """
        return args[0] if args else None

    def label_primary(self, args: list[Any]) -> Any | None:
        """Pass through primary label expressions.

        Primary labels are either names, parenthesized expressions, or % (any label).
        Pass-through avoids unnecessary nesting.

        Args:
            args: Label name or grouped expression.

        Returns:
            The label value unchanged.

        """
        return args[0] if args else None

    def label_name(self, args: list[Any]) -> str:
        """Extract label name identifier.

        Label names can have leading : from grammar rules. Stripping both : and
        backticks is necessary for normalized label comparison.

        Args:
            args: IDENTIFIER token possibly with leading :.

        Returns:
            Label name string with : and backticks removed.

        """
        return str(args[0]).lstrip(":").strip("`")

    def node_properties(self, args: list[Any]) -> Any | None:
        """Pass through node properties or WHERE clause.

        Node properties can be specified as a map literal or extracted via WHERE.
        Pass-through avoids extra wrapping.

        Args:
            args: Properties dict or WHERE clause.

        Returns:
            The properties/where node unchanged.

        """
        return args[0] if args else None

    def node_where(self, args: list[Any]) -> dict[str, Any]:
        """Transform inline WHERE clause within node pattern.

        WHERE can filter node properties inline (e.g., (n WHERE n.age > 30)).
        Wrapping in a dict is necessary to distinguish from property maps.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.

        """
        return {"where": args[0] if args else None}

    # ========================================================================
    # Relationship pattern
    # ========================================================================

    def relationship_pattern(self, args: list[Any]) -> Any | None:
        """Pass through relationship patterns.

        Relationships are parsed by direction-specific rules (left/right/both/any).
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single relationship node from directional rules.

        Returns:
            The relationship pattern unchanged.

        """
        return args[0] if args else None

    def full_rel_left(self, args: list[Any]) -> dict[str, Any]:
        """Transform left-pointing relationship (<--).

        Left direction means the relationship points from right to left in the
        pattern. This distinction is necessary for directed graph traversal.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "left", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "left", **detail}

    def full_rel_right(self, args: list[Any]) -> dict[str, Any]:
        """Transform right-pointing relationship (-->).

        Right direction means the relationship points from left to right in the
        pattern. This is the most common relationship direction.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "right", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "right", **detail}

    def full_rel_both(self, args: list[Any]) -> dict[str, Any]:
        """Transform bidirectional relationship (<-->).

        Both direction means the relationship can be traversed in either direction.
        This is uncommon but supported for specific use cases.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "both", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "both", **detail}

    def full_rel_any(self, args: list[Any]) -> dict[str, Any]:
        """Transform undirected relationship (---).

        Any direction means the relationship can point either way. This is useful
        for matching symmetric relationships where direction doesn't matter.

        Args:
            args: Optional rel_detail dict with relationship components.

        Returns:
            Dict with type "RelationshipPattern", direction "any", and details.

        """
        detail = args[0] if args else {}
        return {"type": "RelationshipPattern", "direction": "any", **detail}

    def rel_detail(self, args: list[Any]) -> dict[str, Any]:
        """Extract details from inside relationship brackets [...].

        Pass-through is necessary to avoid adding wrapper nodes.

        Args:
            args: rel_filler dict with relationship components.

        Returns:
            The filler dict unchanged, or empty dict if no details.

        """
        return args[0] if args else {}

    def rel_filler(self, args: list[Any]) -> dict[str, Any]:
        """Extract components from inside relationship brackets.

        Relationships can have: variable, types, properties, path length, and WHERE.
        These are combined into a single dict. This merging is necessary to
        flatten the AST structure.

        Args:
            args: Mix of variable name string and component dicts.

        Returns:
            Dict containing all relationship components.

        """
        filler = {}
        for arg in args:
            if isinstance(arg, dict):
                filler.update(arg)
            elif isinstance(arg, str) and "variable" not in filler:
                filler["variable"] = arg
        return filler

    def rel_types(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform relationship type constraints.

        Relationships can be constrained to one or more types (e.g., [:KNOWS|:LIKES]).
        Wrapping in a dict is necessary to distinguish from other components.

        Args:
            args: List of relationship type names.

        Returns:
            Dict with "types" key containing list of type names.

        """
        return {"types": list(args)}

    def rel_type(self, args: list[Any]) -> str:
        """Extract relationship type name.

        Type names are identifiers. Stripping backticks is necessary for
        normalized type comparison.

        Args:
            args: IDENTIFIER token.

        Returns:
            Type name string with backticks removed.

        """
        return str(args[0]).strip("`")

    def rel_properties(self, args: list[Any]) -> dict[str, Any]:
        """Transform relationship property constraints.

        Relationships can have property filters just like nodes. Wrapping in a
        dict distinguishes properties from other components.

        Args:
            args: Properties map.

        Returns:
            Dict with "properties" key containing the property map.

        """
        return {"properties": args[0] if args else None}

    def rel_where(self, args: list[Any]) -> dict[str, Any]:
        """Transform inline WHERE clause within relationship pattern.

        WHERE can filter relationship properties (e.g., [:KNOWS WHERE r.since > 2020]).
        Wrapping in a dict distinguishes from property maps.

        Args:
            args: Expression for the WHERE condition.

        Returns:
            Dict with "where" key containing the condition expression.

        """
        return {"where": args[0] if args else None}

    def path_length(self, args: list[Any]) -> dict[str, Any]:
        r"""Transform variable-length path specification (\*).

        Variable-length paths match multiple hops (e.g., ``*1..3`` or ``*`` for unlimited).
        This is necessary for traversing graph paths of unknown length.

        Args:
            args: Optional path_length_range specification.

        Returns:
            Dict with normalized PathLength node for downstream conversion.

        """
        range_spec = args[0] if args else None

        length_node: dict[str, Any] = {
            "type": "PathLength",
            "min": None,
            "max": None,
            "unbounded": False,
        }

        if isinstance(range_spec, dict):
            if "fixed" in range_spec:
                value = range_spec.get("fixed")
                length_node["min"] = value
                length_node["max"] = value
            else:
                if "min" in range_spec:
                    length_node["min"] = range_spec.get("min")
                if "max" in range_spec:
                    length_node["max"] = range_spec.get("max")
                if range_spec.get("unbounded"):
                    length_node["unbounded"] = True
        elif isinstance(range_spec, int):
            length_node["min"] = range_spec
            length_node["max"] = range_spec
        elif range_spec is None:
            length_node["unbounded"] = True
        else:
            # Fallback: attempt to coerce to int for fixed length strings/tokens
            try:
                value = int(str(range_spec))
            except (TypeError, ValueError):
                value = None
            length_node["min"] = value
            length_node["max"] = value

        # When unbounded and no explicit lower bound, default Cypher lower bound is 1
        if length_node["unbounded"] and length_node["min"] is None:
            length_node["min"] = 1

        return {"length": length_node}

    def path_length_range(
        self,
        args: list[Any],
    ) -> int | dict[str, int | None]:
        """Transform path length range specification.

        Ranges can be: exact (5), minimum (5..), maximum (..5), or bounded (5..10).
        These distinctions are necessary for path matching algorithms.

        Args:
            args: One or two integer arguments for range bounds.

        Returns:
            Dict with "fixed", "min"/"max", or "unbounded" keys.

        """
        if len(args) == 1:
            return {"fixed": int(str(args[0]))}
        if len(args) == 2:
            return {
                "min": int(str(args[0])) if args[0] else None,
                "max": int(str(args[1])) if args[1] else None,
            }
        return {"unbounded": True}

    # ========================================================================
    # Properties
    # ========================================================================

    def properties(self, args: list[Any]) -> dict[str, Any]:
        """Extract property map from curly braces {...}.

        Property maps are key-value pairs for node/relationship properties.
        Extracting the inner dict is necessary to unwrap the intermediate structure.

        Args:
            args: property_list wrapper containing props dict.

        Returns:
            Dict mapping property names to values.

        """
        props = next(
            (a for a in args if isinstance(a, dict) and "props" in str(a)),
            {"props": {}},
        )
        return props.get("props", {})

    def property_list(self, args: list[Any]) -> dict[str, dict[str, Any]]:
        """Transform comma-separated property key-value pairs.

        Combines individual property assignments into a single map. Wrapping
        in a dict with "props" key is necessary to pass through transformer.

        Args:
            args: List of property_key_value dicts.

        Returns:
            Dict with "props" key containing merged property map.

        """
        result = {}
        for arg in args:
            if isinstance(arg, dict) and "key" in arg:
                result[arg["key"]] = arg["value"]
        return {"props": result}

    def property_key_value(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single property assignment (key: value).

        Separating key and value is necessary for validation and execution.

        Args:
            args: [property_name, expression]

        Returns:
            Dict with "key" and "value" for the property assignment.

        """
        return {
            "key": str(args[0]),
            "value": args[1] if len(args) > 1 else None,
        }

    def property_name(self, args: list[Any]) -> str:
        """Extract property name identifier.

        Property names are identifiers. Stripping backticks is necessary for
        normalized property name comparison.

        Args:
            args: IDENTIFIER token.

        Returns:
            Property name string with backticks removed.

        """
        return str(args[0]).strip("`")


class ClauseRulesMixin:
    """Grammar rule methods for Cypher clauses and statement structure.

    Handles:
    - Top-level query structure (cypher_query, statement_list, query_statement)
    - UNION operations
    - Read clauses (MATCH, OPTIONAL MATCH, UNWIND)
    - Write clauses (CREATE, MERGE, DELETE, SET, REMOVE, FOREACH)
    - CALL/YIELD for procedure invocation
    - RETURN and WITH clauses (items, aliases, DISTINCT)
    - WHERE clause
    - ORDER BY, SKIP, LIMIT modifiers
    """

    def union_all_marker(self, args: list[Any]) -> bool:
        """Return True to signal that this UNION has the ALL qualifier."""
        return True

    def union_op(self, args: list[Any]) -> dict[str, Any]:
        """Transform a UNION [ALL] connector between two query statements.

        Args:
            args: ``[True]`` when the operator is ``UNION ALL``; ``[]`` when
                it is plain ``UNION``.

        Returns:
            Dict with ``type='UnionOp'`` and ``all`` flag.

        """
        return {"type": "UnionOp", "all": bool(args)}

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
        """Transform a list of statements, preserving UNION connectors.

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

    def query_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a read-only query statement (MATCH...RETURN).

        Query statements consist of read clauses (MATCH, UNWIND, WITH) followed
        by an optional RETURN clause. This separation is necessary for the AST
        to distinguish between read-only queries and update operations.

        Args:
            args: Mix of read clause nodes and an optional ReturnStatement.

        Returns:
            Dict with type "QueryStatement" containing separated clauses and return.

        """
        read_clauses = [
            a
            for a in args
            if not isinstance(a, dict) or a.get("type") != "ReturnStatement"
        ]
        return_clause = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "ReturnStatement"
            ),
            None,
        )
        return {
            "type": "QueryStatement",
            "clauses": read_clauses,
            "return": return_clause,
        }

    def update_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform an update statement (CREATE/MERGE/DELETE/SET/REMOVE).

        Update statements can have prefix clauses (MATCH/WITH for context),
        one or more update operations, and an optional RETURN clause.
        Supports multi-step pipelines where WITH/MATCH clauses are interleaved
        with update clauses (e.g. MATCH → SET → WITH → SET → RETURN).

        The ``clauses`` list preserves the exact order clauses appear in the
        query, which is necessary for correct multi-step pipeline execution.
        The ``prefix`` and ``updates`` lists are retained for backward
        compatibility with code that accesses those keys directly.

        Args:
            args: Mix of prefix clauses, update clauses, and optional return.

        Returns:
            Dict with type "UpdateStatement" containing ordered clauses list
            plus legacy ``prefix``, ``updates``, and ``return`` keys.

        """
        _UPDATE_TYPES = frozenset(
            {
                "CreateClause",
                "MergeClause",
                "DeleteClause",
                "SetClause",
                "RemoveClause",
            },
        )

        prefix_clauses = []
        update_clauses = []
        return_clause = None
        ordered_clauses: list[Any] = []

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnStatement":
                    return_clause = arg
                elif arg.get("type") in _UPDATE_TYPES:
                    update_clauses.append(arg)
                    ordered_clauses.append(arg)
                else:
                    prefix_clauses.append(arg)
                    ordered_clauses.append(arg)

        if return_clause is not None:
            ordered_clauses.append(return_clause)

        return {
            "type": "UpdateStatement",
            "prefix": prefix_clauses,
            "updates": update_clauses,
            "return": return_clause,
            "clauses": ordered_clauses,
        }

    def update_clause(self, args: list[Any]) -> Any | None:
        """Pass through update clauses without modification.

        This method exists because the grammar has an update_clause rule that
        acts as a union of different update types. Pass-through is necessary
        to avoid adding unnecessary wrapper nodes in the AST.

        Args:
            args: Single update clause node (CREATE/MERGE/DELETE/SET/REMOVE).

        Returns:
            The update clause node unchanged.

        """
        return args[0] if args else None

    def read_clause(self, args: list[Any]) -> Any | None:
        """Pass through read clauses without modification.

        This method exists because the grammar has a read_clause rule that
        acts as a union of different read types. Pass-through is necessary
        to avoid adding unnecessary wrapper nodes in the AST.

        Args:
            args: Single read clause node (MATCH/UNWIND/WITH/CALL).

        Returns:
            The read clause node unchanged.

        """
        return args[0] if args else None

    def _ambig(self, args: list[Any]) -> Any:
        """Handle ambiguous parses by selecting the most specific interpretation.

        The Earley parser can produce multiple valid parse trees for ambiguous
        grammar rules. This method implements a disambiguation strategy that
        prefers structured semantic nodes over primitive values, ensuring the
        AST contains the most useful representation for type checking.

        Priority order:
        1. Not expression nodes (highest priority for negation)
        2. Other structured dictionary nodes (semantic information)
        3. Primitive values (fallback)

        This is necessary to resolve conflicts like "NOT" being both a keyword
        and a potential identifier, or property lookups vs. simple variables.

        Args:
            args: List of alternative parse results for the same input.

        Returns:
            The most semantically rich parse result.

        """
        if not args:
            return None
        # Prefer dicts (structured data) over primitives
        structured = [a for a in args if isinstance(a, dict)]
        if structured:
            # Prefer nodes with 'Not' type (for NOT expressions)
            not_nodes = [a for a in structured if a.get("type") == "Not"]
            if not_nodes:
                return not_nodes[0]
            # Otherwise return first structured node
            return structured[0]
        # Return first argument if no structured data
        return args[0]

    def statement(self, args: list[Any]) -> Any | None:
        """Pass through statement nodes.

        Statements can be query statements, update statements, or call statements.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single statement node.

        Returns:
            The statement node unchanged.

        """
        return args[0] if args else None

    # ========================================================================
    # CALL statement
    # ========================================================================

    def call_statement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a standalone CALL statement for procedure invocation.

        CALL statements invoke stored procedures, which can have arguments and
        yield results. This is necessary for extending Cypher with custom logic.

        Args:
            args: [procedure_reference, optional explicit_args, optional yield_clause]

        Returns:
            Dict with type "CallStatement" containing procedure info, args, and yield.

        """
        procedure = args[0] if args else None
        remaining = list(args[1:]) if len(args) > 1 else []

        arguments: list[Any] = []
        yield_clause = None

        if remaining:
            potential_args = remaining[0]
            if isinstance(potential_args, list):
                arguments = potential_args
                remaining = remaining[1:]
            elif (
                isinstance(potential_args, dict)
                and potential_args.get("type") == "YieldClause"
            ):
                yield_clause = potential_args
                remaining = remaining[1:]
            elif potential_args is None:
                arguments = []
                remaining = remaining[1:]
            else:
                arguments = [potential_args]
                remaining = remaining[1:]

        if remaining and yield_clause is None:
            yield_clause = remaining[0]

        yield_items: list[Any] = []
        where = None
        if isinstance(yield_clause, dict):
            items = yield_clause.get("items")
            if isinstance(items, list):
                yield_items = items
            where = yield_clause.get("where")

        return {
            "type": "Call",
            "procedure_name": procedure,
            "arguments": arguments,
            "yield_items": yield_items,
            "where": where,
        }

    def call_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a CALL clause within a larger query.

        Call clauses can appear as read clauses, allowing procedure results to
        be used in subsequent query parts. Different type from call_statement
        is necessary to distinguish standalone vs. embedded calls.

        Args:
            args: [procedure_reference, explicit_args, yield_clause]

        Returns:
            Dict with type "CallClause" containing procedure info, args, and yield.

        """
        procedure = args[0] if args else None
        remaining = list(args[1:]) if len(args) > 1 else []

        arguments: list[Any] = []
        yield_clause = None

        if remaining:
            potential_args = remaining[0]
            if isinstance(potential_args, list):
                arguments = potential_args
                remaining = remaining[1:]
            elif (
                isinstance(potential_args, dict)
                and potential_args.get("type") == "YieldClause"
            ):
                yield_clause = potential_args
                remaining = remaining[1:]
            elif potential_args is None:
                arguments = []
                remaining = remaining[1:]
            else:
                arguments = [potential_args]
                remaining = remaining[1:]

        if remaining and yield_clause is None:
            yield_clause = remaining[0]

        yield_items: list[Any] = []
        where = None
        if isinstance(yield_clause, dict):
            items = yield_clause.get("items")
            if isinstance(items, list):
                yield_items = items
            where = yield_clause.get("where")

        return {
            "type": "Call",
            "procedure_name": procedure,
            "arguments": arguments,
            "yield_items": yield_items,
            "where": where,
        }

    def procedure_reference(self, args: list[Any]) -> Any | None:
        """Extract procedure name reference.

        Procedures are referenced by their function name, which may include
        namespace qualification. Pass-through is necessary to avoid wrapping.

        Args:
            args: Function name (possibly namespaced).

        Returns:
            Procedure name string or dict.

        """
        return args[0] if args else None

    def explicit_args(self, args: list[Any]) -> list[Any]:
        """Transform explicit procedure arguments list.

        Procedures can accept arguments just like functions. Converting to
        a list is necessary for consistent handling of variadic arguments.

        Args:
            args: Expression nodes for each argument.

        Returns:
            List of argument expressions.

        """
        return list(args) if args else []

    def yield_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a YIELD clause that selects procedure output fields.

        YIELD specifies which fields from procedure results to expose, with
        optional WHERE filtering. This structure is necessary for controlling
        procedure output visibility.

        Args:
            args: [yield_items or "*", optional where_clause]

        Returns:
            Dict with type "YieldClause" containing items and optional where filter.

        """
        items = args[0] if args else None
        where = args[1] if len(args) > 1 else None
        return {"type": "YieldClause", "items": items, "where": where}

    def yield_items(self, args: list[Any]) -> list[Any]:
        """Transform list of yielded fields.

        Multiple fields can be yielded from a procedure. List normalization
        is necessary for consistent iteration during execution.

        Args:
            args: Individual yield_item nodes.

        Returns:
            List of yield item dictionaries.

        """
        return list(args) if args else []

    def yield_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single yielded field with optional alias.

        Fields can be renamed using AS. Separating field and alias is
        necessary for proper symbol table construction.

        Args:
            args: [field_name] or [field_name, alias]

        Returns:
            Dict with field name and optional alias.

        """
        variable = args[0] if args else None
        alias = args[1] if len(args) > 1 else None
        return {"type": "YieldItem", "variable": variable, "alias": alias}

    def field_name(self, args: list[Any]) -> str:
        """Extract field name identifier.

        Field names in YIELD clauses reference procedure output columns.
        Stripping backticks is necessary to normalize identifier representation.

        Args:
            args: IDENTIFIER token.

        Returns:
            Field name string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # MATCH clause
    # ========================================================================

    def match_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a MATCH clause for pattern matching.

        MATCH finds existing graph patterns. The OPTIONAL modifier makes the match
        non-failing (like SQL LEFT JOIN). WHERE filters matched patterns.
        Separating these components is necessary for query optimization and execution.

        Args:
            args: Mix of OPTIONAL keyword, pattern, and optional where_clause.

        Returns:
            Dict with type "MatchClause" containing optional flag, pattern, and where.

        """

        def _is_optional(value: Any) -> bool:
            """Return ``True`` if *value* represents the ``OPTIONAL`` keyword.

            Recursively checks strings, Lark :class:`Token`/:class:`Tree`
            nodes, dicts, and sequences to detect the OPTIONAL modifier in
            any parse-tree representation.
            """
            if isinstance(value, str):
                return value.upper() == "OPTIONAL"
            if isinstance(value, Token):
                token_value = str(getattr(value, "value", "") or "").upper()
                token_type = str(getattr(value, "type", "") or "").upper()
                return (
                    token_value == "OPTIONAL"
                    or token_type == "OPTIONAL_KEYWORD"
                )
            if isinstance(value, Tree):
                if getattr(value, "data", "") == "optional_keyword":
                    return True
                return any(_is_optional(child) for child in value.children)
            if isinstance(value, dict):
                return any(_is_optional(child) for child in value.values())
            if isinstance(value, (list, tuple)):
                return any(_is_optional(child) for child in value)
            return False

        optional = any(_is_optional(arg) for arg in args)
        pattern = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "Pattern"
            ),
            None,
        )
        where = next(
            (
                a
                for a in args
                if isinstance(a, dict) and a.get("type") == "WhereClause"
            ),
            None,
        )
        return {
            "type": "MatchClause",
            "optional": optional,
            "pattern": pattern,
            "where": where,
        }

    # ========================================================================
    # CREATE clause
    # ========================================================================

    def create_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a CREATE clause for creating new graph elements.

        CREATE adds new nodes and relationships to the graph based on a pattern.
        Wrapping the pattern in a typed node is necessary to distinguish CREATE
        from MATCH and other pattern-using clauses during execution.

        Args:
            args: Pattern to create.

        Returns:
            Dict with type "CreateClause" containing the creation pattern.

        """
        return {"type": "CreateClause", "pattern": args[0] if args else None}

    # ========================================================================
    # MERGE clause
    # ========================================================================

    def merge_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a MERGE clause for create-or-match operations.

        MERGE ensures a pattern exists, creating it if necessary. This is atomic
        and prevents duplicates. ON MATCH/CREATE actions allow different behavior
        based on whether the pattern existed. This structure is necessary for
        conditional update logic.

        Args:
            args: [pattern, optional merge_action nodes]

        Returns:
            Dict with type "MergeClause" containing pattern and conditional actions.

        """
        pattern = args[0] if args else None
        actions = args[1:] if len(args) > 1 else []
        return {"type": "MergeClause", "pattern": pattern, "actions": actions}

    def merge_action(self, args: list[Any]) -> dict[str, Any]:
        """Transform ON MATCH or ON CREATE action within MERGE.

        These actions execute conditionally based on whether MERGE found or
        created the pattern. Distinguishing the trigger type is necessary for
        correct execution semantics.

        Args:
            args: ["MATCH" or "CREATE" keyword, set_clause]

        Returns:
            Dict with type "MergeAction" specifying trigger type and SET operation.

        """
        on_type: str | None = None
        set_clause = None

        for arg in args:
            if isinstance(arg, str):
                upper = arg.upper()
                if upper == "ON":
                    continue
                if upper in {"MATCH", "CREATE"}:
                    on_type = upper.lower()
                    continue
            if isinstance(arg, Token):
                token_value = str(getattr(arg, "value", "") or "").upper()
                token_type = str(getattr(arg, "type", "") or "").upper()
                candidate = token_value or token_type
                if candidate == "ON":
                    continue
                if candidate in {
                    "MATCH",
                    "CREATE",
                    "CREATE_KEYWORD",
                    "MATCH_KEYWORD",
                }:
                    on_type = (
                        "MATCH" if "MATCH" in candidate else "CREATE"
                    ).lower()
                    continue
            if isinstance(arg, dict):
                set_clause = arg

        if on_type is None:
            on_type = "create"

        return {"type": "MergeAction", "on": on_type, "set": set_clause}

    def merge_action_type(self, args: list[Any]) -> str:
        """Normalize merge action type tokens to keyword strings."""
        if not args:
            return ""
        token = args[0]
        if isinstance(token, str):
            return token.upper()
        if isinstance(token, Token):
            value = str(getattr(token, "value", "") or "").upper()
            if value:
                return value
            return str(getattr(token, "type", "") or "").upper()
        return str(token).upper()

    def merge_action_match(self, _args: list[Any]) -> str:
        """Return normalized MATCH keyword for merge action."""
        return "MATCH"

    def merge_action_create(self, _args: list[Any]) -> str:
        """Return normalized CREATE keyword for merge action."""
        return "CREATE"

    # ========================================================================
    # DELETE clause
    # ========================================================================

    def detach_keyword(self, args: list[Any]) -> str:
        """Transform detach_keyword rule."""
        return "DETACH"

    def delete_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a DELETE clause for removing graph elements.

        DELETE removes nodes and relationships. DETACH DELETE also removes
        relationships connected to deleted nodes, preventing orphaned edges.
        This distinction is necessary for safe cascading deletion.

        Args:
            args: [optional "DETACH" keyword, delete_items]

        Returns:
            Dict with type "DeleteClause" containing detach flag and items to delete.

        """
        detach = any(
            str(a).upper() == "DETACH" for a in args if isinstance(a, str)
        )
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {
            "type": "DeleteClause",
            "detach": detach,
            "items": items.get("items", []),
        }

    def delete_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of expressions to delete.

        Multiple items can be deleted in one clause. Wrapping in a dict is
        necessary to distinguish the list from other argument types.

        Args:
            args: Expression nodes identifying items to delete.

        Returns:
            Dict containing list of items.

        """
        return {"items": list(args)}

    # ========================================================================
    # SET clause
    # ========================================================================

    def set_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a SET clause for updating graph properties.

        SET modifies node/relationship properties and labels. Multiple set
        operations can be combined in one clause. Extracting the items list
        is necessary for execution planning.

        Args:
            args: set_items wrapper containing list of set operations.

        Returns:
            Dict with type "SetClause" containing list of set operations.

        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "SetClause", "items": items.get("items", [])}

    def set_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of SET operations.

        Wrapping in a dict is necessary to pass the list through the
        transformer chain without flattening.

        Args:
            args: Individual set_item nodes.

        Returns:
            Dict containing list of set operations.

        """
        return {"items": list(args)}

    def set_item(self, args: list[Any]) -> Any | None:
        """Pass through individual SET operation.

        The grammar has set_item as a union of different set types.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single set operation (property/labels/all properties/add properties).

        Returns:
            The set operation node unchanged.

        """
        return args[0] if args else None

    def set_property_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single property assignment (e.g., SET n.age = 30).

        Property updates are the most common SET operation. Separating variable,
        property name, and value is necessary for validation and execution.

        Args:
            args: [variable_name, property_lookup, expression]

        Returns:
            Dict with type "SetProperty" containing variable, property, and value.

        """
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        value = args[2] if len(args) > 2 else None
        return {
            "type": "SetProperty",
            "variable": variable,
            "property": prop,
            "value": value,
        }

    def set_labels_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform label assignment (e.g., SET n:Person:Employee).

        Labels are added to nodes for categorization. Separate handling is
        necessary because labels are not stored as properties.

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "SetLabels" containing variable and label list.

        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "SetLabels", "variable": variable, "labels": labels}

    def set_all_properties_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property map replacement (e.g., SET n = {name: 'Alice'}).

        This replaces ALL properties on a node/relationship with a new map.
        Distinct type is necessary to warn users about potential data loss.

        Args:
            args: [variable_name, expression]

        Returns:
            Dict with type "SetAllProperties" for complete property replacement.

        """
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {
            "type": "SetAllProperties",
            "variable": variable,
            "value": value,
        }

    def add_all_properties_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property map merge (e.g., SET n += {age: 30}).

        This merges new properties with existing ones without removing others.
        Distinct type from SetAllProperties is necessary for different semantics.

        Args:
            args: [variable_name, expression]

        Returns:
            Dict with type "AddAllProperties" for additive property merge.

        """
        variable = args[0] if args else None
        value = args[1] if len(args) > 1 else None
        return {
            "type": "AddAllProperties",
            "variable": variable,
            "value": value,
        }

    # ========================================================================
    # REMOVE clause
    # ========================================================================

    def remove_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a REMOVE clause for deleting properties or labels.

        REMOVE deletes properties or labels without deleting the node/relationship
        itself. This is distinct from DELETE which removes entire elements.

        Args:
            args: remove_items wrapper containing list of remove operations.

        Returns:
            Dict with type "RemoveClause" containing list of remove operations.

        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "RemoveClause", "items": items.get("items", [])}

    def remove_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of REMOVE operations.

        Wrapping in a dict is necessary to pass the list through the
        transformer chain.

        Args:
            args: Individual remove_item nodes.

        Returns:
            Dict containing list of remove operations.

        """
        return {"items": list(args)}

    def remove_item(self, args: list[Any]) -> Any | None:
        """Pass through individual REMOVE operation.

        The grammar has remove_item as a union of property and label removal.
        Pass-through avoids adding wrapper nodes.

        Args:
            args: Single remove operation (property or labels).

        Returns:
            The remove operation node unchanged.

        """
        return args[0] if args else None

    def remove_property_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform property removal (e.g., REMOVE n.age).

        Removes a single property from a node/relationship. Separating variable
        and property is necessary for validation.

        Args:
            args: [variable_name, property_lookup]

        Returns:
            Dict with type "RemoveProperty" containing variable and property name.

        """
        variable = args[0] if args else None
        prop = args[1] if len(args) > 1 else None
        return {
            "type": "RemoveProperty",
            "variable": variable,
            "property": prop,
        }

    def remove_labels_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform label removal (e.g., REMOVE n:Person).

        Removes labels from a node. Separate handling from properties is
        necessary because labels are metadata, not property values.

        Args:
            args: [variable_name, node_labels]

        Returns:
            Dict with type "RemoveLabels" containing variable and labels to remove.

        """
        variable = args[0] if args else None
        labels = args[1] if len(args) > 1 else None
        return {"type": "RemoveLabels", "variable": variable, "labels": labels}

    # ========================================================================
    # FOREACH clause
    # ========================================================================

    def foreach_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a FOREACH clause for iterative list mutation.

        FOREACH (variable IN list | update_clause+) iterates over every element
        of *list*, binds it to *variable*, and executes the inner update clauses
        for each element.  It is a pure mutation construct — no new variables
        leak into the outer query scope.

        Args:
            args: [variable_name (str), list_expression, update_clause, ...]
                  — at least three elements: variable, expression, one clause.

        Returns:
            Dict with type "ForeachClause" containing ``variable``,
            ``list_expression``, and ``clauses`` (list of update clause dicts).

        """
        variable = args[0] if args else None
        list_expr = args[1] if len(args) > 1 else None
        clauses = list(args[2:]) if len(args) > 2 else []
        return {
            "type": "ForeachClause",
            "variable": variable,
            "list_expression": list_expr,
            "clauses": clauses,
        }

    # ========================================================================
    # UNWIND clause
    # ========================================================================

    def unwind_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform an UNWIND clause for list expansion.

        UNWIND expands a list into individual rows, creating a new variable for
        each element. This is necessary for processing collections in Cypher,
        similar to SQL's UNNEST or CROSS JOIN LATERAL.

        Args:
            args: [expression (the list), variable_name (for each element)]

        Returns:
            Dict with type "UnwindClause" containing source expression and variable.

        """
        expr = args[0] if args else None
        var = args[1] if len(args) > 1 else None
        return {"type": "UnwindClause", "expression": expr, "variable": var}

    # ========================================================================
    # WITH clause
    # ========================================================================

    def with_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WITH clause for query chaining and variable passing.

        WITH acts like a pipe operator, passing selected variables to the next
        query part while filtering, sorting, and limiting. This is necessary for
        multi-stage queries where intermediate results need transformation.

        Unlike RETURN (which ends a query), WITH continues processing. The DISTINCT,
        WHERE, ORDER BY, SKIP, and LIMIT modifiers control what gets passed forward.

        Args:
            args: Mix of DISTINCT keyword, return body, and optional clauses.

        Returns:
            Dict with type "WithClause" containing all components for variable passing.

        """
        distinct = any(
            str(a).upper() == "DISTINCT" for a in args if isinstance(a, str)
        )
        items = []
        where = None
        order = None
        skip = None
        limit = None

        for arg in args:
            if isinstance(arg, dict):
                if arg.get("type") == "ReturnBody":
                    # Extract items from the return body
                    items = arg.get("items", [])
                elif arg.get("type") == "WhereClause":
                    where = arg
                elif arg.get("type") == "OrderClause":
                    order = arg
                elif arg.get("type") == "SkipClause":
                    skip = arg
                elif arg.get("type") == "LimitClause":
                    limit = arg
            elif isinstance(arg, list) and not items:
                # return_body returns a list of items directly
                items = arg
            elif arg == "*":
                items = "*"

        return {
            "type": "WithClause",
            "distinct": distinct,
            "items": items,
            "where": where,
            "order": order,
            "skip": skip,
            "limit": limit,
        }

    # ========================================================================
    def distinct_keyword(self, args: list[Any]) -> str:
        """Transform distinct_keyword rule."""
        return "DISTINCT"

    # RETURN clause
    # ========================================================================

    def return_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a RETURN clause for query output specification.

        RETURN determines what a query outputs, similar to SQL SELECT. It can
        return specific expressions (with aliases), or ``*`` for all variables.
        DISTINCT, ORDER BY, SKIP, and LIMIT control the result set.

        This structure is necessary to separate output specification from ordering
        and pagination, enabling query optimization and execution planning.

        Args:
            args: Mix of DISTINCT keyword, return body/items/``*``, and optional clauses.

        Returns:
            Dict with type "ReturnStatement" containing all output specifications.

        """
        # LOGGER.debug(f"DEBUG return_clause args: {args} types: {[type(a) for a in args]}")
        distinct = False
        for a in args:
            if (hasattr(a, "type") and a.type == "DISTINCT") or (
                isinstance(a, str) and a.upper() == "DISTINCT"
            ):
                distinct = True
            elif hasattr(a, "value") and str(a.value).upper() == "DISTINCT":
                # Handle Token objects that might not satisfy isinstance(str) directly in some envs
                # or if it is a Tree/Token mixup
                distinct = True

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

    def return_body(self, args: list[Any]) -> str | list[Any]:
        r"""Extract the body of a RETURN clause (items or \*).

        This handles the special case of RETURN ``*`` vs. RETURN item1, item2.
        Pass-through is necessary to avoid double-wrapping the items list.

        Args:
            args: Either ``"*"`` string or return_items list.

        Returns:
            Either ``"*"`` string or list of return items.

        """
        if args and args[0] == "*":
            return "*"
        # args[0] is already a list from return_items
        return args[0] if args else []

    def return_items(self, args: list[Any]) -> list[Any]:
        """Transform comma-separated list of return items.

        Normalizing to a list is necessary for consistent iteration during
        output construction.

        Args:
            args: Individual return_item nodes.

        Returns:
            List of return item dictionaries.

        """
        return list(args) if args else []

    def return_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single return item with optional alias.

        Return items can be aliased using AS (e.g., RETURN n.name AS fullName).
        Wrapping with type "ReturnItem" is necessary for type checking to
        distinguish expressions from their return metadata.

        Args:
            args: [expression] or [expression, alias]

        Returns:
            Dict with type "ReturnItem" containing expression and optional alias.

        """
        if len(args) == 1:
            return {"type": "ReturnItem", "expression": args[0], "alias": None}
        return {"type": "ReturnItem", "expression": args[0], "alias": args[1]}

    def return_alias(self, args: list[Any]) -> str:
        """Extract return item alias identifier.

        Aliases define the output column names. Stripping backticks is necessary
        to normalize identifier representation.

        Args:
            args: IDENTIFIER token.

        Returns:
            Alias string with backticks removed.

        """
        return str(args[0]).strip("`")

    # ========================================================================
    # WHERE clause
    # ========================================================================

    def where_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a WHERE clause for filtering.

        WHERE filters graph patterns (in MATCH) or intermediate results (in WITH).
        Wrapping the condition is necessary to attach it to the appropriate clause.

        Args:
            args: Single boolean expression for the filter condition.

        Returns:
            Dict with type "WhereClause" containing the filter expression.

        """
        return {"type": "WhereClause", "condition": args[0] if args else None}

    # ========================================================================
    # ORDER BY clause
    # ========================================================================

    def order_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform an ORDER BY clause for result sorting.

        ORDER BY sorts results by one or more expressions, each with a direction.
        This structure is necessary for execution planning and index utilization.

        Args:
            args: order_items wrapper containing list of sort specifications.

        Returns:
            Dict with type "OrderClause" containing list of order items.

        """
        items = next(
            (a for a in args if isinstance(a, dict) and "items" in str(a)),
            {"items": []},
        )
        return {"type": "OrderClause", "items": items.get("items", [])}

    def order_items(self, args: list[Any]) -> dict[str, list[Any]]:
        """Transform comma-separated list of ORDER BY items.

        Wrapping in a dict is necessary to pass the list through the transformer.

        Args:
            args: Individual order_item nodes.

        Returns:
            Dict containing list of order specifications.

        """
        return {"items": list(args)}

    def nulls_placement(self, args: list[Any]) -> dict[str, Any]:
        """Transform a NULLS FIRST or NULLS LAST clause.

        Args:
            args: [NULLS_FIRST_KEYWORD | NULLS_LAST_KEYWORD token]

        Returns:
            Dict with placement field ("first" or "last").

        """
        token = args[0] if args else None
        placement = "first" if str(token).upper() == "FIRST" else "last"
        return {"type": "nulls_placement", "placement": placement}

    def order_item(self, args: list[Any]) -> dict[str, Any]:
        """Transform a single ORDER BY item with optional direction and nulls placement.

        Each item specifies an expression, sort direction (ASC/DESC), and optional
        NULLS FIRST/LAST placement.  Default is ascending, nulls last.

        Args:
            args: [expression, optional direction string, optional nulls_placement dict]

        Returns:
            Dict with expression, direction, and nulls_placement fields.

        """
        expr = args[0] if args else None
        direction = None
        nulls_kw = None

        for arg in args[1:]:
            if isinstance(arg, dict) and arg.get("type") == "nulls_placement":
                nulls_kw = arg["placement"]
            elif isinstance(arg, str):
                direction = arg

        direction_value = direction or "asc"
        ascending = direction_value not in {"desc", "descending"}

        return {
            "type": "OrderByItem",
            "expression": expr,
            "ascending": ascending,
            "nulls_placement": nulls_kw,
        }

    def order_direction(self, args: list[Any]) -> str:
        """Normalize ORDER BY direction keywords.

        Supports ASC/ASCENDING and DESC/DESCENDING. Normalization is necessary
        for consistent execution regardless of which keyword form is used.

        Args:
            args: Direction keyword token (optional).

        Returns:
            Normalized direction string: "asc" or "desc".

        """
        if not args:
            return "asc"
        d = str(args[0]).upper()
        return "desc" if d in ["DESC", "DESCENDING"] else "asc"

    # ========================================================================
    # SKIP and LIMIT
    # ========================================================================

    def skip_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a SKIP clause for result pagination.

        SKIP skips the first N results, enabling pagination. The expression
        is evaluated at runtime, allowing parameterized pagination.

        Args:
            args: Expression evaluating to number of rows to skip.

        Returns:
            Dict with type "SkipClause" containing skip count expression.

        """
        return {"type": "SkipClause", "value": args[0] if args else None}

    def limit_clause(self, args: list[Any]) -> dict[str, Any]:
        """Transform a LIMIT clause for result set size restriction.

        LIMIT restricts output to N results. Combined with SKIP, this enables
        efficient pagination. Expression evaluation is necessary for parameterization.

        Args:
            args: Expression evaluating to maximum number of rows to return.

        Returns:
            Dict with type "LimitClause" containing limit count expression.

        """
        return {"type": "LimitClause", "value": args[0] if args else None}
