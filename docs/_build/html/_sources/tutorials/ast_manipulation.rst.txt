AST Manipulation
================

Learn how to create, modify, and traverse AST nodes programmatically using
PyCypher's Pydantic-based model layer.

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Familiarity with Cypher query syntax
* Basic knowledge of Pydantic models

Overview
--------

PyCypher represents parsed Cypher queries as a tree of strongly-typed
`Pydantic <https://docs.pydantic.dev/>`_ models defined in
:mod:`pycypher.ast_models`.  Every element of the query — nodes, relationships,
expressions, clauses — has a corresponding Python class with validated fields.

The typical flow is:

1. **Parse** a Cypher string with :class:`~pycypher.grammar_parser.GrammarParser`
2. **Convert** the raw Lark tree to typed models with :class:`~pycypher.ast_models.ASTConverter`
3. **Inspect or modify** the resulting AST using standard Python attribute access
4. **Traverse** the tree with :meth:`~pycypher.ast_models.ASTNode.traverse`

Parsing a Query to a Typed AST
------------------------------

The simplest way to get a typed AST from a Cypher string is the one-liner
:meth:`~pycypher.ast_converter.ASTConverter.from_cypher`:

.. code-block:: python

   from pycypher.ast_models import ASTConverter

   typed_ast = ASTConverter.from_cypher(
       "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
   )

   print(type(typed_ast))
   # <class 'pycypher.ast_models.Query'>

If you need access to the intermediate Lark parse tree (e.g. for custom
transformers or debugging), use the two-step flow instead:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter

   parser = GrammarParser()
   parse_tree = parser.parse("MATCH (n:Person) WHERE n.age > 30 RETURN n.name")

   converter = ASTConverter()
   typed_ast = converter.convert(
       parser.transformer.transform(parse_tree)
   )

The returned ``Query`` object is the root of a fully validated tree.  Every
child node carries type information, making IDE auto-complete and static
analysis tools work out of the box.

Creating AST Nodes from Scratch
-------------------------------

You can construct AST nodes directly without parsing a string.  This is useful
for code-generation tools, query rewriters, and testing.

Nodes and Labels
~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import NodePattern, Variable

   # A node pattern: (p:Person)
   person = NodePattern(
       variable=Variable(name="p"),
       labels=["Person"],
   )
   print(person.variable.name)   # "p"
   print(person.labels)          # ["Person"]

   # An anonymous node: ()
   anon = NodePattern()
   print(anon.variable)          # None

Relationship Patterns
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import (
       RelationshipPattern,
       RelationshipDirection,
       Variable,
   )

   # -[r:KNOWS]->
   rel = RelationshipPattern(
       variable=Variable(name="r"),
       rel_types=["KNOWS"],
       direction=RelationshipDirection.RIGHT,
   )
   print(rel.direction)   # RelationshipDirection.RIGHT

Expressions
~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import (
       Arithmetic,
       Comparison,
       IntegerLiteral,
       PropertyLookup,
       Variable,
   )

   # n.age > 30
   predicate = Comparison(
       operator=">",
       left=PropertyLookup(
           expression=Variable(name="n"),
           property_name="age",
       ),
       right=IntegerLiteral(value=30),
   )

   # n.score + 10
   expr = Arithmetic(
       operator="+",
       left=PropertyLookup(
           expression=Variable(name="n"),
           property_name="score",
       ),
       right=IntegerLiteral(value=10),
   )

Traversing the AST
------------------

Every ``ASTNode`` subclass exposes a :meth:`traverse` iterator that yields
all nodes in the tree in depth-first order:

.. code-block:: python

   for node in typed_ast.traverse():
       print(f"{node.__class__.__name__}")

You can filter by type to find specific constructs:

.. code-block:: python

   from pycypher.ast_models import NodePattern, PropertyLookup

   # Find all node patterns in the query
   node_patterns = [
       n for n in typed_ast.traverse()
       if isinstance(n, NodePattern)
   ]
   print(f"Found {len(node_patterns)} node pattern(s)")

   # Find all property lookups
   lookups = [
       n for n in typed_ast.traverse()
       if isinstance(n, PropertyLookup)
   ]
   for lk in lookups:
       print(f"  property: {lk.property_name}")

Pretty-Printing
---------------

The :meth:`pretty` method produces a human-readable tree representation:

.. code-block:: python

   print(typed_ast.pretty())

This is invaluable for debugging grammar changes and understanding how PyCypher
interprets a given Cypher string.

Validation
----------

Because every AST node is a Pydantic model, construction validates field types
automatically:

.. code-block:: python

   from pydantic import ValidationError
   from pycypher.ast_models import IntegerLiteral

   try:
       bad = IntegerLiteral(value="not_a_number")
   except ValidationError as e:
       print(e)  # Pydantic reports the type mismatch

For semantic validation (undefined variables, aggregation rules, etc.) use
:class:`~pycypher.semantic_validator.SemanticValidator` — see the
:doc:`query_validation` tutorial for details.

Common AST Node Types
---------------------

+----------------------------+--------------------------------------------------+
| Class                      | Represents                                       |
+============================+==================================================+
| ``Query``                  | Top-level query (list of clauses)                |
+----------------------------+--------------------------------------------------+
| ``Match``                  | MATCH clause with patterns and WHERE             |
+----------------------------+--------------------------------------------------+
| ``Return``                 | RETURN clause with items and modifiers           |
+----------------------------+--------------------------------------------------+
| ``With``                   | WITH clause (projection between pipeline stages) |
+----------------------------+--------------------------------------------------+
| ``NodePattern``            | ``(var:Label {props})``                          |
+----------------------------+--------------------------------------------------+
| ``RelationshipPattern``    | ``-[var:TYPE]->``                                |
+----------------------------+--------------------------------------------------+
| ``PropertyLookup``         | ``var.property``                                 |
+----------------------------+--------------------------------------------------+
| ``Comparison``             | ``left op right``                                |
+----------------------------+--------------------------------------------------+
| ``Arithmetic``             | ``left op right`` (math)                         |
+----------------------------+--------------------------------------------------+
| ``FunctionInvocation``     | ``funcName(args)``                               |
+----------------------------+--------------------------------------------------+
| ``Variable``               | Named variable reference                         |
+----------------------------+--------------------------------------------------+
| ``IntegerLiteral``         | Integer constant                                 |
+----------------------------+--------------------------------------------------+
| ``StringLiteral``          | String constant                                  |
+----------------------------+--------------------------------------------------+

See :mod:`pycypher.ast_models` for the complete list.

Next Steps
----------

* :doc:`query_validation` — validate queries before execution
* :doc:`basic_query_parsing` — execute queries end-to-end
* :doc:`../api/pycypher` — full API reference
