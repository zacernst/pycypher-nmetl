Query Processing
================

Understanding query parsing, validation, optimization, and execution in PyCypher.

Overview
--------

PyCypher processes Cypher queries through a multi-stage pipeline:

1. **Parsing**: Convert text to raw AST using Lark grammar
2. **Conversion**: Transform raw AST to typed Pydantic models
3. **Validation**: Check query semantics and structure
4. **Optimization**: Improve query execution plan
5. **Translation**: Convert to relational algebra

Parsing Pipeline
----------------

Raw Parsing
~~~~~~~~~~~

The ``GrammarParser`` uses Lark to parse Cypher queries:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   
   parser = GrammarParser()
   
   query = """
   MATCH (person:Person)-[:KNOWS]->(friend)
   WHERE person.age > 30
   RETURN person.name, friend.name
   """
   
   # Parse to raw AST (nested dicts and lists)
   raw_ast = parser.parse_to_ast(query)
   print(type(raw_ast))  # dict

The raw AST is a nested structure of dictionaries matching the Lark grammar.

AST Conversion
~~~~~~~~~~~~~~

The ``ASTConverter`` transforms raw AST to typed nodes:

.. code-block:: python

   from pycypher.ast_models import ASTConverter
   
   converter = ASTConverter()
   typed_ast = converter.convert(raw_ast)
   
   # Now we have strongly-typed Pydantic models
   print(type(typed_ast))  # Query
   print(type(typed_ast.clauses[0]))  # Match

**Benefits of typed AST:**
- Type safety with mypy/ty
- Automatic validation
- IDE autocomplete
- Serialization support
- Tree traversal methods

Validation
----------

Semantic Validation
~~~~~~~~~~~~~~~~~~~

The ``SemanticValidator`` checks query correctness:

.. code-block:: python

   from pycypher.semantic_validator import SemanticValidator
   
   validator = SemanticValidator()
   result = validator.validate(typed_ast)
   
   if result.is_valid:
       print("Query is valid!")
   else:
       for issue in result.issues:
           print(f"Issue: {issue.message}")
           print(f"  Location: {issue.location}")
           print(f"  Severity: {issue.severity}")

**What gets validated:**
- Variable scoping and references
- Type compatibility
- Required vs optional clauses
- Pattern structure
- Function signatures
- Aggregation rules

Common Validation Issues
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Issue: Undefined variable
   query = "MATCH (n:Person) RETURN m.name"
   # Error: Variable 'm' not defined
   
   # Issue: Invalid aggregation
   query = "MATCH (n) RETURN n.name, count(n)"
   # Error: Cannot mix aggregation with non-aggregated items
   
   # Issue: Type mismatch
   query = "MATCH (n) WHERE n.age > 'thirty'"
   # Error: Cannot compare integer with string

Query Optimization
------------------

The ``QueryOptimizer`` improves query execution:

.. code-block:: python

   from pycypher.query_optimizer import QueryOptimizer
   
   optimizer = QueryOptimizer()
   optimized_ast = optimizer.optimize(typed_ast)

Optimization Strategies
~~~~~~~~~~~~~~~~~~~~~~~

**Filter Pushdown**

Move WHERE conditions as close to data access as possible:

.. code-block:: text

   Before optimization:
   MATCH (p:Person)-[:KNOWS]->(f)
   WHERE p.age > 30
   
   After optimization (conceptually):
   Filter on Person nodes first, then expand

**Index Usage**

Identify opportunities to use indexes:

.. code-block:: text

   Can use index on Person.age:
   MATCH (p:Person)
   WHERE p.age = 30

**Join Reordering**

Optimize the order of pattern matching:

.. code-block:: text

   Start with most selective pattern:
   MATCH (rare:RareLabel)-[:R]->(common)

Relational Algebra Translation
-------------------------------

The STAR translator converts AST to relational algebra:

.. code-block:: python

   from pycypher.star import STAR
   
   translator = STAR()
   
   # Convert pattern to relational algebra
   from pycypher.ast_models import Pattern
   relation = translator.to_relation(pattern)

Relational Operators
~~~~~~~~~~~~~~~~~~~~

PyCypher uses type-based column namespacing:

.. code-block:: python

   from pycypher.relational_models import (
       EntityTable,
       RelationshipTable,
       FilterRows,
       Join,
       Projection
   )
   
   # EntityTable for node types
   # Columns: Person____ID__, Person__name, Person__age
   person_table = EntityTable(entity_type="Person")
   
   # RelationshipTable for relationship types  
   # Columns: KNOWS____SOURCE__, KNOWS____TARGET__, KNOWS____ID__
   knows_table = RelationshipTable(relationship_type="KNOWS")
   
   # Join preserves only ID columns
   joined = Join(left=person_table, right=knows_table)
   
   # Projection fetches attributes on-demand
   projected = Projection(source=joined, columns=["Person__name"])

**Key Design Principles:**

1. **ID-only preservation**: Joins and filters only keep ID columns
2. **Type-based prefixing**: All columns prefixed with entity/relationship type
3. **Lazy attribute loading**: Attributes fetched only when needed
4. **Deterministic naming**: No column name collisions

Expression Evaluation
---------------------

The ``ExpressionEvaluator`` computes expression values:

.. code-block:: python

   from pycypher.expression_evaluator import ExpressionEvaluator
   from pycypher.ast_models import Arithmetic, IntegerLiteral
   
   evaluator = ExpressionEvaluator()
   
   # Evaluate: 2 + 3
   expr = Arithmetic(
       operator="+",
       left=IntegerLiteral(value=2),
       right=IntegerLiteral(value=3)
   )
   
   result = evaluator.evaluate(expr)
   print(result)  # 5

**Supported operations:**

- Arithmetic: ``+``, ``-``, ``*``, ``/``, ``%``
- Comparison: ``=``, ``<>``, ``<``, ``<=``, ``>``, ``>=``
- Logical: AND, OR, NOT, XOR
- String: ``+``, CONTAINS, STARTS WITH, ENDS WITH
- List: IN, subscript, slice
- Functions: count, sum, avg, min, max, etc.

Query Execution
---------------

End-to-End Processing
~~~~~~~~~~~~~~~~~~~~~

Complete query processing example:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   from pycypher.ast_models import ASTConverter
   from pycypher.semantic_validator import SemanticValidator
   from pycypher.query_optimizer import QueryOptimizer
   from pycypher.star import STAR
   
   # 1. Parse
   parser = GrammarParser()
   raw_ast = parser.parse_to_ast(query)
   
   # 2. Convert
   converter = ASTConverter()
   typed_ast = converter.convert(raw_ast)
   
   # 3. Validate
   validator = SemanticValidator()
   validation = validator.validate(typed_ast)
   if not validation.is_valid:
       raise ValueError("Invalid query")
   
   # 4. Optimize
   optimizer = QueryOptimizer()
   optimized_ast = optimizer.optimize(typed_ast)
   
   # 5. Translate
   translator = STAR()
   relation = translator.to_relation(optimized_ast)
   
   # 6. Execute against backend
   result = relation.to_pandas()

Error Handling
--------------

Handling Parse Errors
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.exceptions import ParseError
   
   try:
       raw_ast = parser.parse_to_ast(invalid_query)
   except ParseError as e:
       print(f"Parse error at line {e.line}, column {e.column}")
       print(f"Message: {e.message}")

Handling Validation Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   result = validator.validate(typed_ast)
   if not result.is_valid:
       for issue in result.issues:
           if issue.severity == "ERROR":
               # Fatal error
               print(f"Error: {issue.message}")
           elif issue.severity == "WARNING":
               # Non-fatal warning
               print(f"Warning: {issue.message}")

Performance Considerations
--------------------------

**Parsing**
- Grammar parsing is relatively fast
- Consider caching parsed queries for repeated use

**Validation**
- Validation overhead is minimal
- Most checks are O(n) in AST size

**Optimization**
- Optimization passes are incremental
- Can be skipped for simple queries

**Translation**
- Translation to relational algebra is efficient
- Column naming is deterministic and cached

**Execution**
- Relational algebra operators execute against pandas DataFrames
- Performance depends on data size and query complexity
- Optimizations include ID-only joins and lazy attribute loading

For More Information
--------------------

* See :doc:`../api/pycypher` for complete API reference
* See :doc:`../tutorials/query_validation` for validation examples
