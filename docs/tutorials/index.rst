Tutorials
=========

Learn PyCypher through structured, hands-on tutorials that progress from
first query to production deployment.

.. toctree::
   :maxdepth: 2
   :caption: Tutorial Series:

   basic_query_parsing
   graph_modeling
   pattern_matching
   query_validation
   ast_manipulation
   data_etl_pipeline
   integration_guide
   troubleshooting

Learning Pathway
-----------------

The tutorials are organized into three tiers.  Start at your level and
work forward:

**Beginner** — Your first graph queries

1. :doc:`basic_query_parsing` — Load data, execute MATCH/WHERE/RETURN, sort
   and aggregate results.  *Start here.*
2. :doc:`graph_modeling` — Design your tabular data as entities and
   relationships.  Modeling patterns, ID strategies, common pitfalls.

**Intermediate** — Real-world graph analysis

3. :doc:`pattern_matching` — Variable-length paths, shortestPath, OPTIONAL
   MATCH, pattern comprehensions, quantifier predicates, EXISTS subqueries.
4. :doc:`query_validation` — Pre-execution syntax and semantic validation.
   Error severity levels, custom validation pipelines.
5. :doc:`ast_manipulation` — Programmatic AST inspection, traversal, and
   construction using Pydantic models.

**Advanced** — Production systems

6. :doc:`data_etl_pipeline` — ContextBuilder, YAML pipelines, the ``nmetl``
   CLI, data sources, and output sinks.
7. :doc:`integration_guide` — Embed PyCypher in web APIs, batch jobs, and
   notebooks.  Query parameters, error handling, context refresh patterns.
8. :doc:`troubleshooting` — Diagnose parse errors, missing results,
   performance problems, and unexpected output.

.. tip::

   Each tutorial builds on earlier ones.  The beginner tutorials establish
   the vocabulary and patterns that intermediate and advanced tutorials
   assume.

Prerequisites
-------------

* Python 3.14 or higher (free-threaded build recommended)
* PyCypher packages installed (see :doc:`../getting_started`)
* Basic familiarity with pandas DataFrames

Each tutorial includes:

* Clear learning objectives
* Complete, runnable code examples
* "Try it yourself" exercises
* Common pitfalls and solutions

Related Resources
-----------------

* :doc:`../getting_started` — Installation and quick-start examples
* :doc:`../user_guide/index` — In-depth reference guides
* :doc:`../user_guide/performance_tuning` — Production optimization
* :doc:`../api/pycypher` — Full API reference

Example Scripts
~~~~~~~~~~~~~~~

The ``examples/`` directory contains runnable scripts that complement these
tutorials.  Run any script with ``uv run python examples/<script>.py``:

* ``quickstart.py`` — Minimal end-to-end query (pairs with :doc:`basic_query_parsing`)
* ``advanced_grammar_examples.py`` — Complex Cypher patterns including
  variable-length paths, comprehensions, and REDUCE (pairs with :doc:`pattern_matching`)
* ``functions_in_where.py`` — Using scalar functions inside WHERE predicates
* ``scalar_functions_in_with.py`` — Transforming data with functions in WITH clauses
* ``ast_conversion_example.py`` — Programmatic AST inspection and traversal
  (pairs with :doc:`ast_manipulation`)
* ``multi_query_composition.py`` — Multi-query ETL pipelines with cross-query
  dependencies (pairs with :doc:`data_etl_pipeline`)
