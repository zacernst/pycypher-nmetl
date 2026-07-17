ADR-011: Grammar Rule Mixins Package Split
==========================================

:Status: Accepted
:Date: 2026-03
:Relates to: ``packages/pycypher/src/pycypher/grammar_rule_mixins/``
:Supersedes: Original monolithic ``grammar_rule_mixins.py``

Context
-------

The original ``grammar_rule_mixins.py`` was a single file containing all Lark
transformer methods for converting raw parse trees into Pydantic AST nodes.
As the Cypher grammar grew, this file exceeded 3,000 lines covering five
unrelated domains (literals, expressions, patterns, functions, clauses).
This made the file difficult to navigate, review, and test in isolation.

Additionally, during concurrent AST cache warmup, per-call Rich logging
in the monolithic transformer caused ``OSError: bad file descriptor`` errors
due to file descriptor contention.

Decision
--------

Split the monolithic file into a **Python package** with five focused mixin
modules:

.. code-block:: text

   grammar_rule_mixins/
   ├── __init__.py       # Re-exports all 5 mixins
   ├── literals.py       # LiteralRulesMixin (278 lines)
   ├── expressions.py    # ExpressionRulesMixin (683 lines)
   ├── patterns.py       # PatternRulesMixin (520 lines)
   ├── functions.py      # FunctionRulesMixin (594 lines)
   └── clauses.py        # ClauseRulesMixin (1,165 lines)

``CompositeTransformer`` in ``grammar_transformers.py`` delegates to
specialized transformer classes (``LiteralTransformer``,
``ExpressionTransformer``, ``PatternTransformer``, ``StatementTransformer``)
that inherit from these mixins.  Method resolution is cached on the instance
after first lookup to avoid repeated MRO iteration during concurrent warmup.

Alternatives Considered
-----------------------

1. **Keep monolithic file** — Too large for effective review and navigation;
   concurrent Rich logging bug made this untenable.

2. **Separate transformer classes per rule group** — Would break Lark's
   method-name-based dispatch, which expects all visitor methods on a single
   transformer instance.

3. **Decorator-based organization** — Methods stay in one file but are tagged
   by domain.  Doesn't reduce file size or enable independent testing.

Consequences
------------

- Each mixin can be understood, reviewed, and tested independently.
- Lark's visitor mechanism is preserved via Python MRO (multiple inheritance).
- ``__init__.py`` re-exports all mixins, so ``from pycypher.grammar_rule_mixins
  import ClauseRulesMixin`` continues to work.
- Method-resolution caching eliminates the Rich ``OSError`` during concurrent
  AST cache warmup.
- Import path changed from module to package — existing ``from
  pycypher.grammar_rule_mixins import ...`` imports are unaffected.
