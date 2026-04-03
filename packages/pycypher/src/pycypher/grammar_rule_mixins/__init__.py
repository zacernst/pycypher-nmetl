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

from pycypher.grammar_rule_mixins.clauses import ClauseRulesMixin
from pycypher.grammar_rule_mixins.expressions import ExpressionRulesMixin
from pycypher.grammar_rule_mixins.functions import FunctionRulesMixin
from pycypher.grammar_rule_mixins.literals import LiteralRulesMixin
from pycypher.grammar_rule_mixins.patterns import PatternRulesMixin

__all__ = [
    "ClauseRulesMixin",
    "ExpressionRulesMixin",
    "FunctionRulesMixin",
    "LiteralRulesMixin",
    "PatternRulesMixin",
]
