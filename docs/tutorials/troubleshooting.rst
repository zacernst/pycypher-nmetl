Troubleshooting Queries
=======================

Diagnose and fix common query issues — parse errors, missing data,
performance problems, and unexpected results.

.. contents:: In this guide
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* A working ``Star`` instance (see :doc:`basic_query_parsing`)

Diagnostic Tools
-----------------

Enable Debug Logging
~~~~~~~~~~~~~~~~~~~~

The single most useful diagnostic step:

.. code-block:: python

   import logging
   logging.getLogger("pycypher").setLevel(logging.DEBUG)

This prints parse timing, clause-by-clause execution, row counts, and
memory usage for every query.

Validate Before Executing
~~~~~~~~~~~~~~~~~~~~~~~~~

Catch errors early without running the full query:

.. code-block:: python

   from pycypher import validate_query

   errors = validate_query("MATCH (n:Person) RETURN m")
   for e in errors:
       print(f"{e.severity.value}: {e.message}")
   # ERROR: Variable 'm' is used but not defined

Check Available Functions
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   print(star.available_functions())
   # ['abs', 'acos', 'asin', ..., 'valuetype', 'values']

Inspect the Parsed AST
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from pycypher.ast_models import ASTConverter

   ast = ASTConverter.from_cypher("MATCH (p:Person) RETURN p.name")
   for clause in ast.clauses:
       print(type(clause).__name__, clause)

Query Metrics
~~~~~~~~~~~~~

.. code-block:: python

   from shared.metrics import QUERY_METRICS

   stats = QUERY_METRICS.snapshot()
   print(f"Queries: {stats.total_queries}, Errors: {stats.error_rate:.1%}")
   print(f"p50: {stats.timing_p50_ms:.0f}ms, p99: {stats.timing_p99_ms:.0f}ms")

Parse Errors
-------------

"Syntax error" or UnexpectedInput
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Symptom**: ``lark.exceptions.UnexpectedInput`` when parsing.

**Common causes**:

1. **Missing closing parenthesis**:

   .. code-block:: text

      MATCH (p:Person WHERE p.age > 30 RETURN p.name
      #                 ^ missing closing )

2. **Missing relationship direction arrow**:

   .. code-block:: text

      MATCH (a)-[:KNOWS]-(b)    # undirected — OK
      MATCH (a)-[:KNOWS](b)     # missing arrow — ERROR

3. **Using SQL keywords instead of Cypher**:

   .. code-block:: text

      # SQL style (wrong)
      SELECT p.name FROM Person p WHERE p.age > 30

      # Cypher style (correct)
      MATCH (p:Person) WHERE p.age > 30 RETURN p.name

4. **Unquoted strings in WHERE**:

   .. code-block:: text

      WHERE p.name = Alice     # ERROR — unquoted
      WHERE p.name = 'Alice'   # correct

**Fix**: Use ``GrammarParser.validate()`` for a quick syntax check:

.. code-block:: python

   from pycypher.grammar_parser import GrammarParser
   print(GrammarParser().validate("MATCH (p:Person) RETURN p"))  # True/False

Missing or Empty Results
-------------------------

Query returns empty DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Checklist**:

1. **Entity type registered?** Check your ContextBuilder call:

   .. code-block:: python

      # If your data uses "User" but your query says "Person":
      MATCH (p:Person) ...  # returns nothing

      # Fix: use the same label
      MATCH (u:User) ...

2. **IDs match between entities and relationships?**

   .. code-block:: python

      # Entity IDs are integers
      people = pd.DataFrame({"__ID__": [1, 2, 3], ...})

      # But relationship sources are strings — no matches!
      knows = pd.DataFrame({"__SOURCE__": ["1", "2"], "__TARGET__": ["2", "3"]})

      # Fix: ensure consistent types
      knows["__SOURCE__"] = knows["__SOURCE__"].astype(int)
      knows["__TARGET__"] = knows["__TARGET__"].astype(int)

3. **Relationship direction wrong?**

   .. code-block:: python

      # Data: Alice -> Bob (source=Alice, target=Bob)
      # Query: who does Bob know?
      MATCH (b:Person {name: 'Bob'})-[:KNOWS]->(other)
      # Returns nothing because Bob is the TARGET, not SOURCE

      # Fix: reverse the direction
      MATCH (other)-[:KNOWS]->(b:Person {name: 'Bob'})

4. **WHERE filter too restrictive?** Remove the WHERE clause temporarily
   to see if the MATCH alone produces results.

Property returns NULL unexpectedly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Property name mismatch (case-sensitive).

.. code-block:: python

   # DataFrame has "Name" but query uses "name"
   MATCH (p:Person) RETURN p.name   # NULL — wrong case
   MATCH (p:Person) RETURN p.Name   # correct

**Fix**: Check your DataFrame column names:

.. code-block:: python

   print(people_df.columns.tolist())
   # ['__ID__', 'Name', 'age']  ← note capital N

Unexpected extra rows
~~~~~~~~~~~~~~~~~~~~~

**Cause**: Usually a cross-join from unconnected MATCH patterns:

.. code-block:: python

   # This produces |Person| × |Product| rows (Cartesian product)
   MATCH (p:Person), (pr:Product) RETURN p.name, pr.title

   # Fix: connect them with a relationship
   MATCH (p:Person)-[:BOUGHT]->(pr:Product) RETURN p.name, pr.title

Performance Issues
-------------------

Query is slow
~~~~~~~~~~~~~

**Step 1**: Enable debug logging to identify the slow clause.

**Step 2**: Check for these common causes:

.. list-table::
   :widths: 40 60
   :header-rows: 1

   * - Pattern
     - Fix
   * - Unbounded path ``-[*]->``
     - Add bounds: ``-[*1..3]->``
   * - Cross-join ``MATCH (a), (b)``
     - Use relationship: ``(a)-[:REL]->(b)``
   * - Wide RETURN with many properties
     - Select only needed properties
   * - Missing WHERE on large scan
     - Add early filter: ``WHERE p.active``

**Step 3**: Use the query profiler for detailed timing:

.. code-block:: python

   from pycypher.query_profiler import QueryProfiler

   profiler = QueryProfiler(star)
   report = profiler.profile("MATCH (p:Person)-[:KNOWS*]->(q) RETURN p.name, q.name")
   print(report)
   print(f"Hotspot: {report.hotspot}")
   print(f"Suggestions: {report.recommendations}")

**Step 4**: Set a timeout to prevent runaway queries:

.. code-block:: python

   result = star.execute_query(query, timeout_seconds=5.0)

See :doc:`../user_guide/performance_tuning` for comprehensive tuning guidance.

Memory usage is high
~~~~~~~~~~~~~~~~~~~~~

**Cause**: Large intermediate results from cross-joins or unbounded paths.

**Fix**:

.. code-block:: python

   # Set a memory budget
   result = star.execute_query(query, memory_budget_bytes=500 * 1024 * 1024)

   # Or limit cross-join size globally
   import os
   os.environ["PYCYPHER_MAX_CROSS_JOIN_ROWS"] = "1000000"

Exception Reference
--------------------

.. list-table::
   :widths: 35 65
   :header-rows: 1

   * - Exception
     - Meaning and fix
   * - ``VariableNotFoundError``
     - Variable used but never bound in MATCH/WITH.
       Check spelling and scope.
   * - ``UnsupportedFunctionError``
     - Function name not recognized.  Use
       ``star.available_functions()`` to see valid names.
   * - ``GraphTypeNotFoundError``
     - Entity label not registered in the context.
       Check your ContextBuilder calls.
   * - ``QueryTimeoutError``
     - Query exceeded its time budget.  Simplify the query
       or increase the timeout.
   * - ``QueryMemoryBudgetError``
     - Estimated memory exceeds the budget.  Add filters
       or increase the budget.
   * - ``UnexpectedInput`` (Lark)
     - Cypher syntax error.  Check parentheses, quotes,
       and keyword spelling.

Try It Yourself
----------------

**Exercise**: The following query returns no results.  Find and fix the bug:

.. code-block:: python

   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
   })
   knows = pd.DataFrame({
       "__SOURCE__": ["1", "2"],
       "__TARGET__": ["2", "3"],
   })
   context = ContextBuilder.from_dict({"Person": people, "KNOWS": knows})
   star = Star(context=context)

   result = star.execute_query(
       "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
   )
   print(result)  # Empty!

.. toggle::

   The bug is a type mismatch: ``__ID__`` contains integers (1, 2, 3) but
   ``__SOURCE__`` and ``__TARGET__`` contain strings ("1", "2", "3").
   Fix by making the types consistent:

   .. code-block:: python

      knows = pd.DataFrame({
          "__SOURCE__": [1, 2],   # integers, matching __ID__
          "__TARGET__": [2, 3],
      })

Next Steps
----------

* :doc:`query_validation` — pre-execution validation
* :doc:`../user_guide/performance_tuning` — production tuning
* :doc:`../deployment/troubleshooting` — Docker and infrastructure issues
