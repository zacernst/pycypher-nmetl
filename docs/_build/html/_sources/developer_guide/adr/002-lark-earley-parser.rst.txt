ADR-002: Lark Parser with Earley Algorithm
==========================================

:Status: Accepted
:Date: 2024
:Relates to: ``packages/pycypher/src/pycypher/grammar_parser.py``

Context
-------

PyCypher needs a complete Cypher grammar parser.  Cypher has complex operator
precedence, optional clauses, and syntactic ambiguities (e.g., ``MATCH (n)``
could be a function call or a node pattern depending on context).  The parser
must handle the full grammar without requiring manual disambiguation.

Decision
--------

Use **Lark** with the **Earley** parsing algorithm.  Earley handles ambiguous
and context-sensitive grammars without requiring the grammar author to
restructure rules for lookahead constraints.

The compiled parser is cached as a process-level singleton via
``get_default_parser()``; application code must not construct ``GrammarParser()``
directly.  Parse results (ASTs) are also cached with LRU capacity of 512
entries.

Alternatives Considered
-----------------------

1. **LALR parser** (Lark also supports LALR) — Faster, but requires the
   grammar to be unambiguous.  Cypher's syntax makes this difficult without
   significant grammar restructuring.

2. **ANTLR** — Powerful but requires a Java runtime for grammar compilation
   and generates code that is harder to integrate into a pure-Python project.

3. **Hand-written recursive descent** — Maximum flexibility but extremely
   labor-intensive for a grammar as large as Cypher, and difficult to maintain
   as the grammar evolves.

Consequences
------------

- Cold parse: ~56 ms per unique query; warm (cached): < 0.1 ms.
- Grammar changes are expressed declaratively in ``.lark`` files rather than
  imperative parser code.
- Earley's O(n³) worst case is acceptable for query strings (typically < 1 KB).
- ``CompositeTransformer`` converts raw parse trees to typed Pydantic AST nodes
  via Lark's visitor/transformer mechanism.
