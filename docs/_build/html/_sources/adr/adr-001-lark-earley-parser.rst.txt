ADR-001: Use Lark with Earley Algorithm for Cypher Parsing
==========================================================

:Status: Accepted
:Date: 2024-01
:Affects: ``packages/pycypher/src/pycypher/grammar_parser.py``,
          ``packages/pycypher/src/pycypher/grammar_transformers.py``

Context
-------

PyCypher needs a parser that can handle the full openCypher grammar specification.
The grammar is complex, with ambiguous constructs (e.g. label predicates in WHERE
clauses, list comprehensions vs pattern comprehensions) that require a parser
capable of handling ambiguity.

Key requirements:

- Full openCypher BNF coverage
- Good error messages for malformed queries
- Reasonable cold-start performance for interactive use
- Maintainable grammar definition (not hand-written parser tables)

Decision
--------

Use `Lark <https://github.com/lark-parser/lark>`_ with the **Earley** parsing
algorithm.  The grammar is defined declaratively in Lark's EBNF notation,
derived from the official openCypher BNF specification.

To eliminate cold-start latency (~96ms for grammar compilation), the parser
is compiled eagerly in a daemon thread at module import time and cached as a
process-level singleton via ``get_default_parser()``.

The Lark parse tree is transformed into Pydantic AST models through a
``CompositeTransformer`` architecture with specialised transformers for each
concern (literals, expressions, patterns, statements), following the Single
Responsibility Principle.

Consequences
------------

**Benefits:**

- Earley handles all context-free grammars including ambiguous ones, so the
  grammar can match the openCypher spec without workarounds
- Lark's declarative grammar is readable and maintainable
- Eager background compilation means the parser is ready before the first
  query in interactive sessions
- The transformer architecture keeps grammar-to-AST conversion modular

**Trade-offs:**

- Earley is slower than LALR for unambiguous grammars (microseconds vs
  nanoseconds per parse); acceptable because query execution dominates
- Grammar compilation cost (~96ms) is paid once at import time
- Lark is a runtime dependency

**Constraints:**

- Application code must use ``get_default_parser()`` (the singleton) rather
  than constructing ``GrammarParser()`` directly
- Grammar changes require updating both the Lark grammar and the corresponding
  transformer logic
