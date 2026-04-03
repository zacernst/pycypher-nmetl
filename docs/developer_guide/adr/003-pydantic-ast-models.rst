ADR-003: Pydantic-Based Strongly-Typed AST Models
==================================================

:Status: Accepted
:Date: 2024
:Relates to: ``packages/pycypher/src/pycypher/ast_models.py``

Context
-------

After Lark parses a Cypher query, the result is a raw parse tree of
dictionaries and lists.  Downstream consumers (semantic validation, query
planning, evaluators) need to safely navigate and pattern-match on AST nodes.
Dict-based trees are error-prone — a typo in a key name produces ``None``
rather than an error, and there is no IDE support for available fields.

Decision
--------

Define all AST node types as **Pydantic BaseModel** classes with explicit field
declarations and type annotations.  ``ASTConverter`` translates the raw Lark
parse tree into typed Pydantic nodes.

Examples: ``Query``, ``Match``, ``Return``, ``NodePattern``, ``Variable``,
``PropertyAccess``, ``FunctionCall``, ``BinaryOp``.

Alternatives Considered
-----------------------

1. **Dict-based AST** — Standard in many parser projects but loses type safety,
   IDE support, and validation.

2. **Dataclasses** — Lighter weight but lacks built-in validation, JSON
   serialization, and schema generation that Pydantic provides.

3. **TypedDict** — Provides type hints but no runtime validation; still
   dict-based so no method dispatch.

Consequences
------------

- AST node shapes are validated at construction time — malformed trees fail
  fast with clear error messages.
- IDE autocompletion and type checking work across the entire AST traversal
  pipeline.
- Variables are uniformly represented as ``Variable`` instances (not strings),
  eliminating a class of type confusion bugs.
- Serialization support enables AST caching and inter-process communication.
- Slight construction overhead vs raw dicts, negligible relative to parse time.
