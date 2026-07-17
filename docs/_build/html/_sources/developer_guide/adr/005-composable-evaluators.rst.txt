ADR-005: Composable Evaluator Pattern
======================================

:Status: Accepted
:Date: 2025
:Relates to: ``packages/pycypher/src/pycypher/evaluator_protocol.py``

Context
-------

Expression evaluation in Cypher spans arithmetic, boolean logic, string
predicates, comparisons, aggregation, collection operations, and function
calls.  A single monolithic evaluator class would be thousands of lines long
and create circular import dependencies between the expression evaluator
and the aggregation system.

Decision
--------

Decompose expression evaluation into focused, single-responsibility evaluator
classes that compose via delegation through an ``ExpressionEvaluatorProtocol``:

- ``BindingExpressionEvaluator`` — main coordinator
- ``ArithmeticEvaluator`` — ``+``, ``-``, ``*``, ``/``, ``%``, ``^``
- ``BooleanEvaluator`` — ``AND``, ``OR``, ``NOT``, ``XOR``
- ``ComparisonEvaluator`` — ``=``, ``<>``, ``<``, ``>``, ``<=``, ``>=``
- ``ScalarFunctionEvaluator`` — built-in and user-defined functions
- ``StringPredicateEvaluator`` — ``STARTS WITH``, ``ENDS WITH``, ``CONTAINS``
- ``AggregationEvaluator`` — ``COUNT``, ``SUM``, ``AVG``, ``COLLECT``, etc.
- ``CollectionEvaluator`` — list comprehensions, ``UNWIND``, ``range()``
- ``ExistsEvaluator`` — ``EXISTS { ... }`` subqueries

The ``ExpressionEvaluatorProtocol`` breaks circular imports by defining the
interface that evaluators depend on rather than concrete implementations.

Alternatives Considered
-----------------------

1. **Monolithic evaluator** — Simpler dispatch but creates a multi-thousand-line
   file with circular import issues.

2. **Visitor pattern** with separate visitors per expression type — More
   traditional but harder to compose and share evaluation context.

3. **Expression compilation to bytecode** — Maximum performance but
   significantly more complex; premature optimization for current workloads.

Consequences
------------

- Each evaluator can be developed, tested, and optimized independently.
- Vectorization is applied per-evaluator (numpy for math, pandas ``.str``
  for strings).
- Circular import chains eliminated via protocol-based dependency inversion.
- Slightly higher dispatch overhead, negligible vs actual computation cost.
