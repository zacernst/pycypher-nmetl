Basic Query Parsing
===================

This tutorial demonstrates how to parse openCypher queries using PyCypher.

Learning Objectives
-------------------

* Parse simple and complex Cypher queries
* Convert raw AST to typed AST
* Inspect AST structure
* Handle parsing errors

Setup
-----

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter

   parser = GrammarParser()
   converter = ASTConverter()

Parsing Simple Queries
-----------------------

Match Query
~~~~~~~~~~~

.. code-block:: python

   query = "MATCH (n:Person) RETURN n"
   
   # Parse to raw AST
   raw_ast = parser.parse_to_ast(query)
   print(f"Raw AST type: {type(raw_ast)}")
   
   # Convert to typed AST
   typed_ast = converter.convert(raw_ast)
   print(f"Typed AST type: {type(typed_ast)}")
   
   # Inspect structure
   regular_query = typed_ast.regular_query
   single_query = regular_query.single_queries[0]
   match_clause = single_query.reading_clauses[0]
   
   print(f"Match clause patterns: {match_clause.pattern}")

Create Query
~~~~~~~~~~~~

.. code-block:: python

   query = "CREATE (p:Person {name: 'Alice', age: 30})"
   
   raw_ast = parser.parse_to_ast(query)
   typed_ast = converter.convert(raw_ast)
   
   # Access the create clause
   single_query = typed_ast.regular_query.single_queries[0]
   create_clause = single_query.updating_clauses[0]
   
   pattern = create_clause.pattern
   node_pattern = pattern.pattern_paths[0].node_pattern
   
   print(f"Node labels: {node_pattern.labels}")
   print(f"Node properties: {node_pattern.properties}")

Complex Queries
---------------

Match with WHERE
~~~~~~~~~~~~~~~~

.. code-block:: python

   query = """
   MATCH (person:Person)-[:KNOWS]->(friend:Person)
   WHERE person.age > 30 AND friend.city = 'New York'
   RETURN person.name, friend.name
   """
   
   raw_ast = parser.parse_to_ast(query)
   typed_ast = converter.convert(raw_ast)
   
   single_query = typed_ast.regular_query.single_queries[0]
   match_clause = single_query.reading_clauses[0]
   where = match_clause.where
   
   print(f"WHERE condition: {where.expression}")
   
   # Access RETURN clause
   return_clause = single_query.return_clause
   return_items = return_clause.return_body.return_items
   
   for item in return_items:
       print(f"Return item: {item.expression}")

Handling Variables
------------------

All variables in the AST are represented as ``Variable`` instances:

.. code-block:: python

   from pycypher.ast_models import Variable
   
   query = "MATCH (n:Person) RETURN n.name AS personName"
   
   raw_ast = parser.parse_to_ast(query)
   typed_ast = converter.convert(raw_ast)
   
   # Get the variable from the pattern
   single_query = typed_ast.regular_query.single_queries[0]
   match_clause = single_query.reading_clauses[0]
   pattern_path = match_clause.pattern.pattern_paths[0]
   node_pattern = pattern_path.node_pattern
   
   # Variable is a Variable instance, not a string
   var = node_pattern.variable
   assert isinstance(var, Variable)
   print(f"Variable name: {var.name}")
   
   # Property lookup also uses Variable
   return_item = single_query.return_clause.return_body.return_items[0]
   prop_lookup = return_item.expression
   assert isinstance(prop_lookup.expression, Variable)
   print(f"Property on variable: {prop_lookup.property_name}")

Error Handling
--------------

.. code-block:: python

   from pycypher.exceptions import ParseError
   
   invalid_query = "MATCH (n:Person RETURN n"  # Missing closing parenthesis
   
   try:
       raw_ast = parser.parse_to_ast(invalid_query)
   except ParseError as e:
       print(f"Parse error: {e}")
       print(f"Error location: line {e.line}, column {e.column}")

Next Steps
----------

* Learn about :doc:`ast_manipulation` to modify queries
* Explore :doc:`query_validation` to validate parsed queries
* Try :doc:`pattern_matching` for advanced patterns
