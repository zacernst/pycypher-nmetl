"""Visitor-pattern expression renderer for human-readable display text.

Extracted from :class:`~pycypher.star.Star._expr_display_text` to reduce
cyclomatic complexity in the Star orchestrator.

The ``ExpressionRenderer`` translates Cypher AST expression nodes into
short human-readable strings, used for auto-naming RETURN columns when
no explicit ``AS`` alias is present.

Architecture
------------

Uses a dispatch dictionary mapping AST node types to focused handler
methods, replacing the previous 30+ ``isinstance`` chain (CC ~54) with
individual methods (CC ~3 each).

::

    ExpressionRenderer
    ├── render()  — public dispatch entry point
    ├── _render_variable()
    ├── _render_property_lookup()
    ├── _render_function_invocation()
    ├── _render_arithmetic()
    ├── _render_comparison()
    ├── ...
    └── (one handler per AST node type)
"""

from __future__ import annotations

from typing import Any


class ExpressionRenderer:
    """Render Cypher AST expression nodes as human-readable text.

    Thread-safe and stateless — a single instance can be shared across
    queries.

    Usage::

        renderer = ExpressionRenderer()
        text = renderer.render(some_ast_expr)  # e.g. "n.name" or "count(n)"

    """

    def __init__(self) -> None:
        """Initialize renderer with lazy dispatch table.

        The type-to-handler dispatch table is built on the first
        :meth:`render` call to avoid import-time dependency on
        :mod:`pycypher.ast_models`.
        """
        self._dispatch: dict[type, Any] | None = None

    def _build_dispatch(self) -> dict[type, Any]:
        """Build the type → handler dispatch table.

        Returns:
            Mapping from AST node type to rendering method.

        """
        from pycypher.ast_models import (
            And,
            Arithmetic,
            BooleanLiteral,
            CaseExpression,
            Comparison,
            CountStar,
            FloatLiteral,
            FunctionInvocation,
            IndexLookup,
            IntegerLiteral,
            LabelPredicate,
            ListComprehension,
            Not,
            NullCheck,
            NullLiteral,
            Or,
            Parameter,
            PropertyLookup,
            Reduce,
            Slicing,
            StringLiteral,
            StringPredicate,
            Unary,
            Variable,
            Xor,
        )

        return {
            Variable: self._render_variable,
            PropertyLookup: self._render_property_lookup,
            IntegerLiteral: self._render_integer,
            FloatLiteral: self._render_float,
            StringLiteral: self._render_string,
            BooleanLiteral: self._render_boolean,
            NullLiteral: self._render_null,
            FunctionInvocation: self._render_function,
            Arithmetic: self._render_binary_op,
            Comparison: self._render_binary_op,
            StringPredicate: self._render_binary_op,
            NullCheck: self._render_null_check,
            Not: self._render_not,
            And: self._render_connective,
            Or: self._render_connective,
            Xor: self._render_connective,
            LabelPredicate: self._render_label_predicate,
            Unary: self._render_unary,
            Parameter: self._render_parameter,
            CountStar: self._render_count_star,
            IndexLookup: self._render_index_lookup,
            Slicing: self._render_slicing,
            ListComprehension: self._render_list_comprehension,
            CaseExpression: self._render_case,
            Reduce: self._render_reduce,
        }

    def render(self, expression: Any) -> str | None:
        """Return a short human-readable text for *expression*, or ``None``.

        Follows the unqualified-property convention:
        ``PropertyLookup(p, 'name')`` → ``"name"``.

        Args:
            expression: Any Cypher AST expression node.

        Returns:
            A string display name, or ``None`` if no representation is
            available for the expression type.

        """
        if self._dispatch is None:
            self._dispatch = self._build_dispatch()

        handler = self._dispatch.get(type(expression))
        if handler is None:
            return None
        return handler(expression)

    # ------------------------------------------------------------------
    # Individual renderers (CC ~2–3 each)
    # ------------------------------------------------------------------

    def _render_variable(self, expr: Any) -> str:
        """Render a :class:`Variable` node as its bare name (e.g. ``"n"``)."""
        return expr.name

    def _render_property_lookup(self, expr: Any) -> str:
        """Render a :class:`PropertyLookup` as the unqualified property name (e.g. ``"name"``)."""
        return expr.property

    def _render_integer(self, expr: Any) -> str:
        """Render an :class:`IntegerLiteral` as its decimal string (e.g. ``"42"``)."""
        return str(expr.value)

    def _render_float(self, expr: Any) -> str:
        """Render a :class:`FloatLiteral` as its decimal string (e.g. ``"3.14"``)."""
        return str(expr.value)

    def _render_string(self, expr: Any) -> str:
        """Render a :class:`StringLiteral` as its raw value (e.g. ``"hello"``)."""
        return str(expr.value)

    def _render_boolean(self, expr: Any) -> str:
        """Render a :class:`BooleanLiteral` as Cypher ``"true"`` or ``"false"``."""
        return "true" if expr.value else "false"

    def _render_null(self, expr: Any) -> str:
        """Render a :class:`NullLiteral` as the string ``"null"``."""
        return "null"

    def _render_function(self, expr: Any) -> str:
        """Render a :class:`FunctionInvocation` as ``name(arg1, arg2, ...)``.

        Handles both list and dict argument representations. Unrenderable
        arguments are replaced with ``"?"``.
        """
        fname = expr.function_name
        raw_args = expr.arguments
        if isinstance(raw_args, list):
            arg_list = raw_args
        elif isinstance(raw_args, dict):
            arg_list = raw_args.get("arguments") or raw_args.get("args") or []
        else:
            arg_list = []
        arg_texts = [self.render(a) or "?" for a in arg_list]
        return f"{fname}({', '.join(arg_texts)})"

    def _render_binary_op(self, expr: Any) -> str:
        """Render a binary operation (``Arithmetic``, ``Comparison``, or ``StringPredicate``).

        Produces ``"left op right"`` (e.g. ``"n.age > 18"``).  Shared handler
        for all AST nodes that expose ``left``, ``operator``, and ``right``.
        """
        left_text = self.render(expr.left) or "?"
        right_text = self.render(expr.right) or "?"
        return f"{left_text} {expr.operator} {right_text}"

    def _render_null_check(self, expr: Any) -> str:
        """Render a :class:`NullCheck` as ``"expr IS NULL"`` or ``"expr IS NOT NULL"``."""
        operand_text = self.render(expr.operand) or "?"
        return f"{operand_text} {expr.operator}"

    def _render_not(self, expr: Any) -> str:
        """Render a :class:`Not` node as ``"NOT expr"``."""
        operand_text = self.render(expr.operand) or "?"
        return f"NOT {operand_text}"

    def _render_connective(self, expr: Any) -> str:
        """Render an :class:`And`, :class:`Or`, or :class:`Xor` as ``"a OP b OP c"``."""
        sep = f" {expr.operator} "
        parts = [self.render(op) or "?" for op in expr.operands]
        return sep.join(parts)

    def _render_label_predicate(self, expr: Any) -> str:
        """Render a :class:`LabelPredicate` as ``"expr:Label1:Label2"``."""
        operand_text = self.render(expr.operand) or "?"
        return f"{operand_text}:{':'.join(expr.labels)}"

    def _render_unary(self, expr: Any) -> str | None:
        """Render a :class:`Unary` node as ``"op expr"`` (e.g. ``"-x"``).

        Returns ``None`` if the operand is absent.
        """
        if expr.operand is None:
            return None
        operand_text = self.render(expr.operand) or "?"
        return f"{expr.operator}{operand_text}"

    def _render_parameter(self, expr: Any) -> str:
        """Render a :class:`Parameter` as ``"$name"``."""
        return f"${expr.name}"

    def _render_count_star(self, expr: Any) -> str:
        """Render a :class:`CountStar` as the string ``"count(*)"``."""
        return "count(*)"

    def _render_index_lookup(self, expr: Any) -> str:
        """Render an :class:`IndexLookup` as ``"expr[index]"``."""
        expr_text = self.render(expr.expression) or "?"
        idx_text = self.render(expr.index) or "?"
        return f"{expr_text}[{idx_text}]"

    def _render_slicing(self, expr: Any) -> str:
        """Render a :class:`Slicing` as ``"expr[start..end]"`` with optional bounds."""
        expr_text = self.render(expr.expression) or "?"
        start_text = self.render(expr.start) if expr.start is not None else ""
        end_text = self.render(expr.end) if expr.end is not None else ""
        return f"{expr_text}[{start_text}..{end_text}]"

    def _render_list_comprehension(self, expr: Any) -> str:
        """Render a :class:`ListComprehension` as ``"[var IN list | map_expr]"``.

        Omits the ``| map_expr`` portion when no mapping expression is present.
        """
        var_text = expr.variable.name if expr.variable else "?"
        list_text = self.render(expr.list_expr) or "?"
        map_text = (
            self.render(expr.map_expr) if expr.map_expr is not None else None
        )
        if map_text is not None:
            return f"[{var_text} IN {list_text} | {map_text}]"
        return f"[{var_text} IN {list_text}]"

    def _render_case(self, expr: Any) -> str:
        """Render a :class:`CaseExpression` as the short label ``"case"``."""
        return "case"

    def _render_reduce(self, expr: Any) -> str:
        """Render a :class:`Reduce` expression as ``"reduce(acc, list)"``."""
        acc_text = expr.accumulator.name if expr.accumulator else "?"
        list_text = self.render(expr.list_expr) or "?"
        return f"reduce({acc_text}, {list_text})"
