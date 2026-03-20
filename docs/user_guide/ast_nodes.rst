AST Nodes
=========

Complete reference for all AST node types in PyCypher.

Overview
--------

PyCypher uses strongly-typed AST nodes based on Pydantic models. All AST nodes inherit from the ``ASTNode`` base class and provide validation, serialization, and tree traversal capabilities.

Node Hierarchy
--------------

The AST follows the openCypher grammar structure:

* **Query** - Top-level query container
* **Clause** - MATCH, CREATE, RETURN, WITH, WHERE, etc.
* **Expression** - All expressions (arithmetic, comparison, literals, etc.)
* **Pattern** - Graph patterns for matching and creation
* **Literal** - Literal values (integers, strings, booleans, lists, maps)

Core Node Types
---------------

Query
~~~~~

The root node for all Cypher queries:

.. code-block:: python

   from pycypher.ast_models import Query, Match, Return
   
   # A Query contains clauses
   query = Query(clauses=[
       Match(...),
       Return(...)
   ])

Clauses
~~~~~~~

**Match Clause**

.. code-block:: python

   from pycypher.ast_models import Match, Pattern, NodePattern, Variable
   
   # MATCH (n:Person)
   match = Match(
       pattern=Pattern(
           pattern_paths=[
               PatternPath(
                   node_pattern=NodePattern(
                       variable=Variable(name="n"),
                       labels=["Person"]
                   )
               )
           ]
       )
   )

**Return Clause**

.. code-block:: python

   from pycypher.ast_models import Return, ReturnBody, ReturnItem, Variable
   
   # RETURN n.name
   return_clause = Return(
       return_body=ReturnBody(
           return_items=[
               ReturnItem(
                   expression=PropertyLookup(
                       expression=Variable(name="n"),
                       property_name="name"
                   )
               )
           ]
       )
   )

Expressions
~~~~~~~~~~~

**Comparison**

.. code-block:: python

   from pycypher.ast_models import Comparison, Variable, IntegerLiteral
   
   # n.age > 30
   comparison = Comparison(
       operator=">",
       left=PropertyLookup(
           expression=Variable(name="n"),
           property_name="age"
       ),
       right=IntegerLiteral(value=30)
   )

**Arithmetic**

.. code-block:: python

   from pycypher.ast_models import Arithmetic
   
   # a + b
   addition = Arithmetic(
       operator="+",
       left=Variable(name="a"),
       right=Variable(name="b")
   )

**List Comprehension**

.. code-block:: python

   from pycypher.ast_models import ListComprehension
   
   # [x IN items WHERE x > 5 | x * 2]
   list_comp = ListComprehension(
       variable=Variable(name="x"),
       list_expr=Variable(name="items"),
       where=Comparison(
           operator=">",
           left=Variable(name="x"),
           right=IntegerLiteral(value=5)
       ),
       map_expr=Arithmetic(
           operator="*",
           left=Variable(name="x"),
           right=IntegerLiteral(value=2)
       )
   )

Patterns
~~~~~~~~

**Node Pattern**

.. code-block:: python

   from pycypher.ast_models import NodePattern, Variable, MapLiteral
   
   # (person:Person {name: 'Alice'})
   node = NodePattern(
       variable=Variable(name="person"),
       labels=["Person"],
       properties=MapLiteral(
           entries={"name": StringLiteral(value="Alice")}
       )
   )

**Relationship Pattern**

.. code-block:: python

   from pycypher.ast_models import RelationshipPattern
   
   # -[:KNOWS {since: 2020}]->
   rel = RelationshipPattern(
       variable=Variable(name="r"),
       types=["KNOWS"],
       properties=MapLiteral(
           entries={"since": IntegerLiteral(value=2020)}
       ),
       direction="OUTGOING"
   )

Literals
~~~~~~~~

All literal types:

.. code-block:: python

   from pycypher.ast_models import (
       IntegerLiteral,
       FloatLiteral,
       StringLiteral,
       BooleanLiteral,
       NullLiteral,
       ListLiteral,
       MapLiteral
   )
   
   # Integer
   int_lit = IntegerLiteral(value=42)
   
   # String
   str_lit = StringLiteral(value="Hello")
   
   # Boolean
   bool_lit = BooleanLiteral(value=True)
   
   # Null
   null_lit = NullLiteral()
   
   # List
   list_lit = ListLiteral(
       elements=[
           IntegerLiteral(value=1),
           IntegerLiteral(value=2)
       ]
   )
   
   # Map
   map_lit = MapLiteral(
       entries={
           "key1": StringLiteral(value="value1"),
           "key2": IntegerLiteral(value=123)
       }
   )

Variables
~~~~~~~~~

All variable references use the ``Variable`` class:

.. code-block:: python

   from pycypher.ast_models import Variable
   
   # Create a variable
   var = Variable(name="person")
   
   # Variables are used throughout the AST
   # - In patterns
   # - In expressions
   # - In comprehensions
   # - In return items

Validation
----------

All AST nodes support validation:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter
   
   parser = GrammarParser()
   converter = ASTConverter()
   
   query = "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"
   raw_ast = parser.parse_to_ast(query)
   typed_ast = converter.convert(raw_ast)
   
   # Validate the AST
   result = typed_ast.validate()
   
   if result.is_valid:
       print("Query is valid!")
   else:
       for issue in result.issues:
           print(f"Validation issue: {issue}")

Tree Traversal
--------------

AST nodes inherit from ``TreeMixin`` providing tree traversal methods:

.. code-block:: python

   # Find all variables in the AST
   variables = typed_ast.find_all(Variable)
   
   # Find all node patterns
   node_patterns = typed_ast.find_all(NodePattern)
   
   # Get all children of a node
   children = node.children()

Pydantic Integration
--------------------

All AST nodes are Pydantic models:

.. code-block:: python

   # Serialize to dict
   node_dict = node.model_dump()
   
   # Serialize to JSON
   node_json = node.model_dump_json()
   
   # Load from dict
   node = NodePattern(**node_dict)
   
   # Validation is automatic
   try:
       invalid_node = NodePattern(variable="not a variable")
   except ValidationError as e:
       print(f"Validation error: {e}")

For More Information
--------------------

* See :doc:`../api/pycypher` for complete API reference
* See :doc:`../tutorials/ast_manipulation` for examples
* See :doc:`variables` for details on variable handling
