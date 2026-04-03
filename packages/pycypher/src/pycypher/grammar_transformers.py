"""Specialized AST transformers for openCypher grammar parsing.

This module contains focused transformer classes that replace the monolithic
CypherASTTransformer with specialized, single-responsibility transformers.

Each transformer inherits from a grammar-rule mixin that provides the
production implementations of all transformer methods for its domain:

- LiteralTransformer: Handles literal values (strings, numbers, booleans, lists, maps)
- ExpressionTransformer: Handles expressions (arithmetic, logical, comparisons, predicates)
- FunctionTransformer: Handles function calls, CASE, comprehensions, reduce, quantifiers
- PatternTransformer: Handles graph patterns (nodes, relationships, paths)
- StatementTransformer: Handles Cypher statements and clauses (MATCH, RETURN, etc.)
"""

from __future__ import annotations

from typing import Any

from lark import Transformer

from pycypher.grammar_rule_mixins import (
    ClauseRulesMixin,
    ExpressionRulesMixin,
    FunctionRulesMixin,
    LiteralRulesMixin,
    PatternRulesMixin,
)


class LiteralTransformer(LiteralRulesMixin, Transformer):
    """Transforms Lark parse-tree nodes for literal values into Python objects.

    Handles number literals (signed/unsigned, hex, octal, float, inf, NaN),
    string literals (with escape-sequence processing), boolean literals,
    null, lists, maps, parameters, and variable names.

    All methods inherited from LiteralRulesMixin.
    """


class ExpressionTransformer(ExpressionRulesMixin, Transformer):
    r"""Transforms Lark parse-tree nodes for expressions into AST dicts.

    Handles arithmetic operators (+, -, \*, /, %, ^), unary operators,
    comparison expressions, boolean logic (AND, OR, NOT, XOR),
    string predicates, null predicates, property lookups, index lookups,
    slicing, EXISTS, and inline pattern predicates.

    All methods inherited from ExpressionRulesMixin.
    """


class FunctionTransformer(FunctionRulesMixin, Transformer):
    """Transforms Lark parse-tree nodes for function calls and comprehensions.

    Handles function invocation, CASE expressions, list comprehensions,
    pattern comprehensions, REDUCE, quantifier expressions (ALL, ANY, NONE,
    SINGLE), and map projections.

    All methods inherited from FunctionRulesMixin.
    """


class PatternTransformer(PatternRulesMixin, Transformer):
    """Transforms Lark parse-tree nodes for graph patterns into AST dicts.

    Handles node patterns, relationship patterns, path patterns, label
    expressions, inline property maps, path lengths, and shortest path.

    All methods inherited from PatternRulesMixin.
    """


class StatementTransformer(ClauseRulesMixin, Transformer):
    """Transforms Lark parse-tree nodes for Cypher statements and clauses.

    Handles the top-level query structure (MATCH, RETURN, WITH, WHERE,
    ORDER BY, SKIP, LIMIT, UNION, CREATE, MERGE, DELETE, SET, REMOVE,
    FOREACH, UNWIND, CALL) and produces the typed dict AST consumed by
    ASTConverter.

    All methods inherited from ClauseRulesMixin.
    """


class CompositeTransformer(Transformer):
    """Composite transformer that delegates to specialized transformers.

    This class maintains the same interface as the original monolithic
    CypherASTTransformer but delegates method calls to focused, specialized
    transformer instances. Each delegate inherits all production methods
    from its corresponding grammar-rule mixin, so no fallback is needed.

    Method resolution is cached on the instance after first lookup to avoid
    repeated iteration through delegates. This also eliminates per-call
    logging that caused Rich ``OSError: bad file descriptor`` during
    multi-threaded AST cache warmup, since Rich's ``Console`` is not
    thread-safe.
    """

    def __init__(self) -> None:
        """Initialize composite transformer with specialized delegates."""
        super().__init__()
        self._literal_transformer = LiteralTransformer()
        self._expression_transformer = ExpressionTransformer()
        self._function_transformer = FunctionTransformer()
        self._pattern_transformer = PatternTransformer()
        self._statement_transformer = StatementTransformer()

        # Ordered list of delegates for method resolution.
        self._delegates: tuple[Transformer, ...] = (
            self._literal_transformer,
            self._expression_transformer,
            self._function_transformer,
            self._pattern_transformer,
            self._statement_transformer,
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate method calls to appropriate specialized transformer.

        Resolved methods are cached on the instance (via ``__dict__``) so
        that ``__getattr__`` is only invoked once per method name.  This
        eliminates repeated delegate iteration and all per-call logging,
        preventing Rich ``OSError`` during concurrent access.
        """
        # Avoid infinite recursion during init — _delegates may not exist yet.
        if name.startswith("_"):
            raise AttributeError(name)

        # Search specialized delegates.
        for transformer in self._delegates:
            method = getattr(transformer, name, None)
            if method is not None:
                # Cache on instance so future access bypasses __getattr__.
                self.__dict__[name] = method
                return method

        msg = f"No transformer handles method '{name}'"
        raise AttributeError(msg)

    # Methods that need to be on the composite directly
    def transform(self, tree: Any) -> Any:
        """Transform a parse tree to AST using specialized transformers."""
        return super().transform(tree)

    def _ambig(self, args: list[Any]) -> Any:
        """Handle ambiguous parse cases."""
        if len(args) == 1:
            return args[0]
        return args[0]
