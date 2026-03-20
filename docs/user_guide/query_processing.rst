Query Processing
================

Understanding query parsing, validation, and execution in PyCypher.

Overview
--------

PyCypher processes Cypher queries through a multi-stage pipeline:

1. **Parsing**: Convert Cypher text to a raw AST using the Lark grammar
2. **Conversion**: Transform the raw AST to typed Pydantic models via ``ASTConverter``
3. **Validation**: Check query semantics and structure (via ``SemanticValidator``)
4. **Execution**: Translate and execute against a ``BindingFrame`` IR

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

For the simplest use case, the top-level :func:`~pycypher.validate_query`
convenience function parses and validates in a single call:

.. code-block:: python

   from pycypher import validate_query

   errors = validate_query("MATCH (n:Person) RETURN m")
   for error in errors:
       print(error)

See the :doc:`../tutorials/query_validation` tutorial for a detailed walkthrough.

Query Execution
---------------

The ``Star`` class parses, translates, and executes a Cypher query against a
populated ``Context`` in a single call:

.. code-block:: python

   from pycypher.relational_models import Context
   from pycypher.star import Star

   star = Star(context=context)

   # Execute a complete query and get a pandas DataFrame
   result = star.execute_query("MATCH (p:Person) RETURN p.name AS name")

BindingFrame IR
~~~~~~~~~~~~~~~

The execution engine uses a ``BindingFrame`` as its internal representation.
A BindingFrame is a ``pd.DataFrame`` whose **columns are Cypher variable names**
(e.g. ``"p"``, ``"r"``) and whose values are entity IDs.  Attributes are never
stored in the frame — they are fetched on demand from the ``Context``.

.. code-block:: python

   from pycypher.binding_frame import BindingFrame, EntityScan

   # EntityScan produces a frame with one column per variable
   scan = EntityScan(entity_type="Person", var_name="p")
   frame = scan.scan(context)

   # Fetch an attribute — ID-keyed lookup against the entity table
   names = frame.get_property("p", "name")

This design eliminates three sources of fragility in earlier approaches:

1. Opaque 32-hex HASH_ID column names
2. ``EntityType__propertyname`` column prefixes bleeding into operator logic
3. ``variable_map`` metadata that had to be threaded through every operator

Expression Evaluation
---------------------

The ``BindingExpressionEvaluator`` computes expression values vectorially
against a ``BindingFrame``.  It returns a ``pd.Series`` aligned with the frame:

.. code-block:: python

   from pycypher.binding_evaluator import BindingExpressionEvaluator
   from pycypher.ast_models import Arithmetic, IntegerLiteral

   evaluator = BindingExpressionEvaluator(frame)

   expr = Arithmetic(
       operator="+",
       left=IntegerLiteral(value=2),
       right=IntegerLiteral(value=3),
   )
   result_series = evaluator.evaluate(expr)
   print(result_series.iloc[0])  # 5

**Supported expression types:**

- Arithmetic: ``+``, ``-``, ``*``, ``/``, ``%``, ``^``
- Comparison: ``=``, ``<>``, ``<``, ``<=``, ``>``, ``>=``
- Logical: AND, OR, NOT, XOR (Kleene three-valued logic, fully vectorised)
- Null checks: IS NULL, IS NOT NULL
- **Label predicates**: ``n:Label``, ``n:Label1:Label2`` — test whether a node
  variable belongs to a specific entity type.  The compound form (colon-separated
  labels) applies AND semantics: ``n:Person:Employee`` is true only if the node
  has both labels simultaneously.  Fully composable with AND/OR/NOT:
  ``WHERE n:Person AND NOT n:Manager``.
- **String predicates**: STARTS WITH, ENDS WITH, CONTAINS, ``=~`` (regex), IN.
  When used with a non-string left-hand operand (integer, float, boolean),
  PyCypher raises a ``TypeError`` identifying the operator, the actual type,
  and a suggested remedy::

    # p.age is integer — raises TypeError:
    # "Operator 'STARTS WITH' requires a string left-hand operand,
    #  but got 'int64'. Use toString() to convert if needed."
    WHERE p.age STARTS WITH '3'

    # Fix:
    WHERE toString(p.age) STARTS WITH '3'

  All-null columns pass through without error (null IS NOT a non-string value).
- CASE expressions (both searched and simple, vectorised)
- List index access: ``list[i]``
- List slicing: ``list[start..end]``
- **List comprehensions**: ``[x IN list WHERE cond | expr]`` — filter and/or
  transform a list per row.  The WHERE and map expression are each evaluated
  once over all (row, element) pairs (vectorised).
- **Pattern comprehensions**: ``[(p)-[:REL]->(q) WHERE cond | expr]`` —
  collect graph neighbours into a per-row list.  Evaluated via a single
  ``pd.merge`` across all anchor rows (vectorised).
- **Quantifier predicates**: ``any(x IN list WHERE cond)``,
  ``all(x IN list WHERE cond)``, ``none(x IN list WHERE cond)``,
  ``single(x IN list WHERE cond)`` — return a Boolean per row, vectorised
  via the same explode-evaluate-regroup pipeline as list comprehensions.
- **EXISTS subqueries**: ``EXISTS { MATCH (p)-[:REL]->(q) WHERE cond }`` —
  returns ``true`` if the inner pattern matches at least one row.  The
  subquery is executed once for the entire outer frame using a sentinel-column
  batch technique (not once per row).
- **REDUCE expressions**: ``reduce(acc = init, x IN list | step)`` — fold a
  list into a scalar using an accumulator.  The step expression is evaluated
  once per step position, batched across all active rows (rows whose list
  has not yet been exhausted).  The accumulator dependency within a single
  row remains sequential, but rows at the same step position are evaluated
  together in one ``BindingFrame``, reducing allocations from
  O(rows × max\_items) to O(max\_items).
- Scalar functions: via ``ScalarFunctionRegistry`` — 131 functions across
  string (toUpper, toLower, trim, substring, startsWith, endsWith,
  contains, byteSize, …), math (abs, ceil, floor, sqrt, log, log10, pow,
  hypot, fmod, gcd, lcm, …), bitwise (bitAnd, bitOr, bitXor, bitNot,
  bitShiftLeft, bitShiftRight), trigonometric (sin, cos, tan, asin, acos,
  atan, atan2, sinh, cosh, tanh, …), angle conversion (degrees, radians),
  constants (pi, e), list (head, last, tail, range, toList, …), type
  predicates (isString, isInteger, isFloat, …), temporal (date, datetime,
  localdatetime, duration, date.truncate, datetime.truncate,
  localdatetime.truncate, …), hash & encoding (md5, sha256,
  encodeBase64, …), and utility (coalesce, toString, toInteger,
  randomUUID, …).  Math, trig, bitwise, and string predicate functions
  are fully vectorised using numpy or pandas ``str`` operations — no
  per-row Python overhead for ``abs(n.score)``, ``sin(n.angle)``,
  ``bitAnd(n.flags, 0xFF)``, or ``startsWith(n.name, 'A')``.
  ``round()`` supports an optional third argument to select any of the
  seven Neo4j 5.x rounding modes: ``HALF_UP`` (default, ties away from
  zero), ``HALF_DOWN`` (ties toward zero), ``HALF_EVEN`` (banker's
  rounding), ``CEILING``, ``FLOOR``, ``UP`` (always away from zero),
  ``DOWN`` (truncation toward zero).

Cross-Product MATCH
-------------------

When two ``MATCH`` patterns share no common variables, pycypher produces the
Cartesian product (SQL CROSS JOIN) of both scans:

.. code-block:: python

   # All 3×2=6 (person, product) pairs
   result = star.execute_query(
       "MATCH (p:Person), (pr:Product) "
       "RETURN p.name AS person, pr.title AS product"
   )

   # Filter: who can afford what?
   result = star.execute_query(
       "MATCH (p:Person), (pr:Product) "
       "WHERE p.budget >= pr.price "
       "RETURN p.name AS person, pr.title AS product"
   )

   # Self cross-join: all ordered (A, B) person pairs where A ≠ B
   result = star.execute_query(
       "MATCH (p:Person), (q:Person) "
       "WHERE p.name <> q.name "
       "RETURN p.name AS pname, q.name AS qname"
   )

Aggregation
~~~~~~~~~~~

Aggregation functions (``count``, ``sum``, ``avg``, ``min``, ``max``,
``collect``) are evaluated by ``evaluate_aggregation()``, which returns a
scalar suitable for use in a WITH or RETURN clause:

.. code-block:: python

   # Full-table aggregation (no GROUP BY)
   result = star.execute_query(
       "MATCH (p:Person) RETURN count(*) AS n"
   )

   # Grouped aggregation
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.dept AS dept, count(p) AS n"
   )

The ``DISTINCT`` modifier is supported for all aggregation functions:

.. code-block:: python

   # Count unique departments (not total people)
   result = star.execute_query(
       "MATCH (p:Person) RETURN count(DISTINCT p.dept) AS dept_count"
   )

   # Collect unique scores only
   result = star.execute_query(
       "MATCH (p:Person) RETURN collect(DISTINCT p.score) AS unique_scores"
   )

   # Sum of unique scores
   result = star.execute_query(
       "MATCH (p:Person) RETURN sum(DISTINCT p.score) AS total"
   )

Projection Modifiers: ORDER BY, LIMIT, SKIP, DISTINCT
------------------------------------------------------

RETURN and WITH both support the standard Cypher projection modifiers.
They are applied after aggregation in the following order:
DISTINCT → ORDER BY → SKIP → LIMIT.

.. code-block:: python

   # Sort by age descending, return the top 3
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age DESC LIMIT 3"
   )

   # Remove duplicate departments
   result = star.execute_query(
       "MATCH (p:Person) RETURN DISTINCT p.dept"
   )

   # Paginate: skip the first 10 rows, return the next 5
   result = star.execute_query(
       "MATCH (p:Person) RETURN p.name SKIP 10 LIMIT 5"
   )

   # WITH pipeline: sort and cap rows before the next stage
   result = star.execute_query(
       "MATCH (p:Person) "
       "WITH p.name AS name, p.age AS age ORDER BY age ASC LIMIT 1 "
       "RETURN name, age"
   )

ORDER BY expressions may reference any property of the matched nodes,
including properties that are *not* in the RETURN item list.  If an
ORDER BY expression cannot be evaluated against the projected columns it
is automatically evaluated against the pre-projection binding frame.

**Null placement** — By default PyCypher matches Neo4j 5.x semantics: nulls
sort *last* for both ASC and DESC.  Use the optional ``NULLS FIRST`` /
``NULLS LAST`` suffix to override this per sort key:

.. code-block:: python

   # Put rows with null scores first, then ascending non-nulls
   result = star.execute_query(
       "MATCH (n:Person) RETURN n.name, n.score "
       "ORDER BY n.score ASC NULLS FIRST"
   )

   # Descending by score; explicit NULLS LAST (same as default)
   result = star.execute_query(
       "MATCH (n:Person) RETURN n.name, n.score "
       "ORDER BY n.score DESC NULLS LAST"
   )

   # Multi-column sort with mixed null placement
   result = star.execute_query(
       "MATCH (n:Item) RETURN n.val, n.tag "
       "ORDER BY n.val ASC NULLS FIRST, n.tag ASC NULLS LAST"
   )

Each sort key carries its own NULLS directive independently.  Without a
``NULLS`` keyword the default is ``NULLS LAST`` (Neo4j 5.x default).

Auto-generated Column Names
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a ``RETURN`` item has no explicit ``AS`` alias, PyCypher infers a
display name from the expression (matching Neo4j's behaviour):

- ``RETURN p.name`` → column ``name`` *(unqualified property name)*
- ``RETURN p`` → column ``p`` *(variable name)*
- ``RETURN toUpper(p.name)`` → column ``toUpper(name)``
- ``RETURN p.age + 1`` → column ``age + 1``
- ``RETURN 42`` → column ``42``
- ``RETURN true`` → column ``true``
- ``RETURN p.age > 25`` → column ``age > 25`` *(comparison)*
- ``RETURN p.name STARTS WITH 'A'`` → column ``name STARTS WITH A``
- ``RETURN p.name IS NULL`` → column ``name IS NULL``
- ``RETURN NOT p.active`` → column ``NOT active``
- ``RETURN p.age > 18 AND p.active`` → column ``age > 18 AND active``
- ``RETURN -p.age`` → column ``-age`` *(unary negation)*
- ``RETURN $limit`` → column ``$limit`` *(query parameter)*
- ``RETURN count(*)`` → column ``count(*)``
- ``RETURN xs[0]`` → column ``xs[0]`` *(index lookup)*
- ``RETURN xs[1..3]`` → column ``xs[1..3]`` *(slicing)*
- ``RETURN [x IN xs | x * 2]`` → column ``[x IN xs | x * 2]`` *(list comprehension)*
- ``RETURN CASE WHEN … END`` → column ``case``
- ``RETURN reduce(s = 0, x IN ns | s + x)`` → column ``reduce(s, ns)``

Two or more aliasless items always receive distinct column names:

.. code-block:: python

   result = star.execute_query(
       "MATCH (p:Person) RETURN toUpper(p.name), p.age + 1"
   )
   # Columns: ['toUpper(name)', 'age + 1']

   result = star.execute_query(
       "MATCH (p:Person) RETURN p.age > 30, p.name IS NOT NULL"
   )
   # Columns: ['age > 30', 'name IS NOT NULL']

An explicit ``AS`` alias always overrides the auto-generated name.

OPTIONAL MATCH — Left-Join Semantics
-------------------------------------

``OPTIONAL MATCH`` is the Cypher analogue of a SQL LEFT JOIN.  Rows from the
preceding ``MATCH`` are always preserved; when the optional pattern finds no
match, the unbound variables take ``null`` values.

.. code-block:: python

   # Everyone returned; fname is null for those with no KNOWS edge
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
       WITH p.name AS pname, f.name AS fname
       RETURN pname, fname
       """
   )

   # Filter to those who have no outgoing KNOWS relationship
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
       WITH p.name AS pname, f.name AS fname
       WHERE fname IS NULL
       RETURN pname
       """
   )

   # Combine with coalesce for null-safe display
   result = star.execute_query(
       """
       MATCH (p:Person)
       OPTIONAL MATCH (p)-[:KNOWS]->(f:Person)
       WITH p.name AS pname, coalesce(f.name, 'nobody') AS friend
       RETURN pname, friend
       """
   )

When ``OPTIONAL MATCH`` appears as the first clause of a query (no preceding
``MATCH``), it behaves like a regular ``MATCH`` except that an unknown entity
type produces 0 rows instead of raising an error.

UNWIND — List Explosion
-----------------------

``UNWIND`` takes a list expression and produces one row per list element,
binding each element to an alias variable.  It is commonly used after
``collect()`` to re-expand aggregated lists:

.. code-block:: python

   # Collect names then unwind into individual rows
   result = star.execute_query(
       """
       MATCH (p:Person)
       WITH collect(p.name) AS names
       UNWIND names AS name
       RETURN name
       """
   )

   # Explode a list property (one row per tag per person)
   result = star.execute_query(
       """
       MATCH (p:Person)
       WITH p.name AS name, p.tags AS tags
       UNWIND tags AS tag
       RETURN name, tag
       """
   )

   # Filter after UNWIND using WITH ... WHERE
   result = star.execute_query(
       """
       MATCH (p:Person)
       WITH p.name AS name, p.tags AS tags
       UNWIND tags AS tag
       WITH name, tag WHERE tag = 'python'
       RETURN name, tag
       """
   )

WITH * — Pass-Through Projection
---------------------------------

``WITH *`` passes all variables from the preceding ``MATCH`` (or earlier pipeline
stage) into subsequent clauses unchanged.  No explicit alias list is needed:

.. code-block:: python

   # Access any matched property after WITH *
   result = star.execute_query(
       "MATCH (p:Person) WITH * RETURN p.name AS name, p.age AS age"
   )

   # Filter using WITH * WHERE
   result = star.execute_query(
       "MATCH (p:Person) WITH * WHERE p.age > 30 RETURN p.name AS name"
   )

   # Sort and cap rows without enumerating aliases
   result = star.execute_query(
       "MATCH (p:Person) WITH * ORDER BY p.age ASC LIMIT 3 RETURN p.name AS name"
   )

   # Chain: use WITH * to pass variables, then project explicitly in the next WITH
   result = star.execute_query(
       "MATCH (p:Person) WITH * WITH p.name AS name, p.dept AS dept RETURN name, dept"
   )

Variable-Length Paths
---------------------

Relationship patterns support ``*min..max`` hop-depth syntax for reachability
queries.  The BFS traversal honours relationship direction:

.. code-block:: python

   # All paths of exactly 1–3 KNOWS hops
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) RETURN a.name, b.name"
   )

   # Unbounded — any positive number of hops
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*]->(b:Person) RETURN a.name, b.name"
   )

   # Exactly N hops
   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS*2..2]->(b:Person) RETURN a.name, b.name"
   )

The ``length()`` function returns the number of relationships (hops) in a
named path variable:

.. code-block:: python

   result = star.execute_query(
       "MATCH p = (a:Person)-[:KNOWS*1..4]->(b:Person) "
       "RETURN a.name, b.name, length(p) AS hops"
   )

shortestPath and allShortestPaths
----------------------------------

``shortestPath(pattern)`` returns one row per (start, end) pair at the
minimum hop count.  ``allShortestPaths(pattern)`` returns every path that
ties for the minimum — there may be more than one:

.. code-block:: python

   # One row per (start, end) pair: minimum hops
   result = star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'})
       MATCH p = shortestPath((a)-[:KNOWS*]->(b))
       RETURN a.name AS start, b.name AS end, length(p) AS hops
       """
   )

   # All minimum-hop paths between the same pair
   result = star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Dave'})
       MATCH p = allShortestPaths((a)-[:KNOWS*]->(b))
       RETURN a.name AS start, b.name AS end, length(p) AS hops
       """
   )

Both functions return an empty result (not an error) when no path exists.

SET Clause and Atomicity
------------------------

The SET clause writes property updates back to entity and relationship tables
via ``BindingFrame.mutate()``.  All mutations within a query are buffered in a
shadow layer and only promoted to the canonical tables on successful
completion.  If the query raises an exception, all pending mutations are
discarded:

.. code-block:: python

   # Successful SET — changes persist for subsequent queries
   star.execute_query("MATCH (p:Person) SET p.score = 99 RETURN p.name")

   # Failed SET — context remains unchanged
   try:
       star.execute_query(
           "MATCH (p:Person) SET p.name = 'Changed' RETURN nonexistent.x"
       )
   except Exception:
       pass  # Person.name is still the original value

   # Set a relationship property
   star.execute_query(
       "MATCH (a:Person)-[r:KNOWS]->(b:Person) SET r.since = 2020"
   )

REMOVE Clause
-------------

``REMOVE`` deletes a property from matching entities.  Property deletion is
also buffered and only committed on success:

.. code-block:: python

   # Remove a property from all matching nodes
   star.execute_query(
       "MATCH (p:Person) WHERE p.temp IS NOT NULL REMOVE p.temp"
   )

   # Remove from a relationship
   star.execute_query(
       "MATCH (a:Person)-[r:KNOWS]->(b:Person) REMOVE r.since"
   )

CREATE and MERGE Clauses
------------------------

CREATE inserts new nodes and relationships.  MERGE is an upsert: it first
tries to match the pattern; if no match is found, it creates the pattern.

.. code-block:: python

   # Create a node
   star.execute_query(
       "CREATE (p:Person {name: 'Eve', age: 27})"
   )

   # Create a relationship between matched nodes
   star.execute_query(
       """
       MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Eve'})
       CREATE (a)-[:KNOWS]->(b)
       """
   )

   # MERGE with ON CREATE / ON MATCH actions
   star.execute_query(
       """
       MERGE (p:Person {name: 'Alice'})
       ON CREATE SET p.created_at = timestamp()
       ON MATCH  SET p.updated_at = timestamp()
       """
   )

**MERGE error semantics** — when the entity type in a MERGE pattern is not
registered in the context, MERGE interprets the absence as "no match" and
creates the pattern.  This is the expected Cypher upsert behaviour for
bootstrapping new entity types.  Internally, the engine catches only
:class:`~pycypher.exceptions.GraphTypeNotFoundError` during the match phase;
any other exception propagates normally so genuine bugs are never silently
swallowed.  ``GraphTypeNotFoundError`` is exported from the top-level
``pycypher`` package and can be caught directly:

.. code-block:: python

   from pycypher import GraphTypeNotFoundError

   try:
       scan = EntityScan(entity_type="Ghost", var_name="g")
       scan.scan(context)
   except GraphTypeNotFoundError as e:
       print(f"Entity type {e.type_name!r} is not in the context")

DELETE Clause
-------------

DELETE removes matched nodes from the entity table:

.. code-block:: python

   # Delete a specific node
   star.execute_query(
       "MATCH (p:Person {name: 'Eve'}) DELETE p"
   )

FOREACH Clause
--------------

``FOREACH`` applies a write clause to every element of a list.  The loop
variable is scoped to the FOREACH body; variables in the outer query are
visible but cannot be assigned:

.. code-block:: python

   # Activate every person in a collected list
   star.execute_query(
       """
       MATCH (p:Person)
       WITH collect(p) AS people
       FOREACH (person IN people | SET person.active = true)
       """
   )

   # Create nodes from a literal list
   star.execute_query(
       """
       FOREACH (name IN ['Alice', 'Bob', 'Carol'] |
           MERGE (p:Person {name: name})
       )
       """
   )

UNION — Combining Results
--------------------------

``UNION`` merges two or more sub-query result sets and deduplicates the
combined rows.  ``UNION ALL`` skips deduplication:

.. code-block:: python

   # Deduplicated union
   result = star.execute_query(
       """
       MATCH (p:Person) WHERE p.dept = 'Eng' RETURN p.name AS name
       UNION
       MATCH (p:Person) WHERE p.age < 30  RETURN p.name AS name
       """
   )

   # Keep all rows
   result = star.execute_query(
       """
       MATCH (p:Person) RETURN p.name AS name
       UNION ALL
       MATCH (p:Person) WHERE p.active = true RETURN p.name AS name
       """
   )

All sub-queries in a UNION must project the same column names.

End-to-End Processing
~~~~~~~~~~~~~~~~~~~~~

Complete query processing example:

.. code-block:: python

   from pycypher.relational_models import Context
   from pycypher.star import Star

   context = Context()  # populate with EntityTable / RelationshipTable objects

   # Parse, translate, and execute in one call
   star = Star(context=context)
   result = star.execute_query(
       "MATCH (p:Person) WHERE p.age > 30 RETURN p.name AS name"
   )
   # result is a pandas DataFrame with a 'name' column

Error Handling
--------------

Handling Parse Errors
~~~~~~~~~~~~~~~~~~~~~

Lark parse errors are raised as :class:`lark.exceptions.UnexpectedInput`
subclasses (``UnexpectedToken``, ``UnexpectedCharacters``, ``UnexpectedEOF``).

.. code-block:: python

   from lark.exceptions import UnexpectedInput

   try:
       raw_ast = parser.parse_to_ast(invalid_query)
   except UnexpectedInput as e:
       print(f"Parse error: {e}")

To check syntax without raising, use :meth:`~pycypher.grammar_parser.GrammarParser.validate`:

.. code-block:: python

   parser = GrammarParser()

   # Returns True for valid Cypher, False for genuine syntax errors.
   # NOTE: internal transformer bugs (VisitError) are NOT caught and will
   # propagate — this is intentional so bugs surface rather than being masked.
   if parser.validate("MATCH (n:Person) RETURN n.name"):
       print("Valid query")
   else:
       print("Syntax error in query")

Performance Considerations
--------------------------

**Parse caching (automatic)**

The Earley-based grammar parser is compiled once per process and cached as a
singleton.  AST parse results are additionally cached by query string using an
LRU cache (capacity 512 entries).  Identical query strings hit an O(1) cache
lookup on all calls after the first — no Cypher parsing overhead for repeated
executions of the same query.

For an ETL pipeline executing 5 queries across 1 000 data batches this means
roughly 5 cold parses (~56 ms each) rather than 5 000 (~280 s total).  No
manual caching is required.

**Vectorised execution**

- BindingFrame operations execute against pandas DataFrames using vectorised
  pandas operations.  The following paths are fully vectorised (no per-row
  Python loops):

  - Boolean logic (AND, OR, NOT, XOR) — Kleene three-valued numpy operations.
  - Property lookups — two-level ID-keyed index cache:

    1. **Arrow→pandas conversion cache** — for ``ContextBuilder``-loaded data
       (Arrow-backed tables), the ``pyarrow.Table``→``pd.DataFrame`` conversion
       is performed once per entity type and cached in
       ``Context._property_lookup_cache``.  Subsequent property lookups on the
       same entity type skip the Arrow conversion entirely.
    2. **set_index cache** — the ``raw_df.set_index(ID_COLUMN)`` indexed
       DataFrame is reused across all property accesses in the same query.
       ``EntityScan.scan()`` pre-warms the cache so all subsequent lookups in
       the same query are guaranteed hits.
    3. **Cross-query persistence** — the cache is retained across read-only
       queries; it is only cleared when a mutation (SET/CREATE/DELETE) is
       committed.

    Combined effect: 8.4× speedup on repeated read-only queries vs. the
    uncached baseline (0.805s → 0.096s for 20 warm queries on a 2 000-row
    Arrow-backed context).
  - List comprehensions — all (row, element) pairs exploded into one flat
    DataFrame; WHERE and map expressions evaluated once each.
  - Pattern comprehensions — single ``pd.merge`` across all anchor rows;
    WHERE and map evaluated once over all pairs.
  - Quantifier predicates (any/all/none/single) — same explode pipeline as
    list comprehensions.
  - EXISTS subqueries — sentinel-column batch: the inner query executes once
    with all outer rows rather than once per row.
  - Grouped aggregations (``count``, ``sum``, ``avg``, ``min``, ``max``,
    ``stdev``, ``stdevp``) — pandas native Cython aggregation.
  - Scalar math functions (``abs``, ``ceil``, ``floor``, ``sign``, ``sqrt``,
    ``cbrt``, ``log``, ``log2``, ``log10``, ``exp``, ``pow``) — numpy C-level
    array operations via ``np.abs``, ``np.log``, ``np.power``, etc.
  - Trigonometric functions (``sin``, ``cos``, ``tan``, ``asin``, ``acos``,
    ``atan``, ``atan2``, ``sinh``, ``cosh``, ``tanh``, ``degrees``,
    ``radians``, ``cot``, ``haversin``) — numpy C-level.
  - String predicate functions (``startsWith``, ``endsWith``, ``contains``) —
    pandas ``.str`` accessor (Cython-level); pattern is always treated as a
    literal string, not a regex.

- ``reduce(acc = init, x IN list | step)`` uses a **batch-per-step** approach:
  all rows that are at the same step position are evaluated together in a
  single ``BindingFrame``.  Accumulator state within a row is sequential
  (step *i* uses the value from step *i−1*), but the step expression itself
  is only called O(max\_items) times regardless of row count — 50 calls for
  200 rows × 50-element lists instead of 10 000.

**Scaling**

- Performance scales linearly with the number of matching rows in a MATCH
  clause.  For very large tables (>1 M rows), push selective filters as early
  as possible using WHERE clauses immediately after MATCH.
- Variable-length paths (``[:REL*]``) perform a BFS over the relationship
  table; execution time is proportional to the number of reachable edges.
  Bound hop limits (e.g. ``*1..3``) significantly reduce the search space
  compared to unbounded ``*``.

For More Information
--------------------

* See :doc:`../api/pycypher` for complete API reference
* See :doc:`../tutorials/query_validation` for validation examples
