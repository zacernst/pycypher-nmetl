Data ETL Pipeline
=================

Build end-to-end ETL pipelines that load data from files or databases,
transform it with Cypher queries, and write results to output sinks — using
PyCypher's ingestion layer and the ``nmetl`` CLI.

.. contents:: In this tutorial
   :local:
   :depth: 2

Prerequisites
-------------

* PyCypher installed (see :doc:`../getting_started`)
* Sample CSV or Parquet files to work with
* Basic understanding of Cypher queries

Overview
--------

PyCypher ships with a lightweight ETL framework built around three concepts:

1. **Data sources** — load entities and relationships from files, DataFrames,
   or SQL databases
2. **Cypher queries** — transform, filter, and aggregate the loaded graph data
3. **Output sinks** — write query results to files or external systems

You can drive pipelines **programmatically** via :class:`~pycypher.ingestion.context_builder.ContextBuilder`
or **declaratively** via a YAML config file and the ``nmetl`` CLI.

Programmatic Pipeline
---------------------

Loading Data with ContextBuilder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`~pycypher.ingestion.context_builder.ContextBuilder` provides a
fluent API for assembling a query context:

.. code-block:: python

   from pycypher.ingestion.context_builder import ContextBuilder
   from pycypher.star import Star

   context = (
       ContextBuilder()
       .add_entity("Person", "people.csv", id_col="person_id")
       .add_entity("Company", "companies.csv", id_col="company_id")
       .add_relationship(
           "WORKS_AT",
           "employment.csv",
           source_col="person_id",
           target_col="company_id",
       )
       .build()
   )

   star = Star(context=context)
   result = star.execute_query(
       "MATCH (p:Person)-[:WORKS_AT]->(c:Company) "
       "RETURN p.name AS employee, c.name AS company"
   )

The ``source`` argument accepts:

- **File paths**: CSV, Parquet, or JSON (local or remote via ``s3://``, ``https://``)
- **pandas DataFrames**: for data already in memory
- **PyArrow Tables**: for zero-copy Arrow integration

Quick Setup from a Dict
~~~~~~~~~~~~~~~~~~~~~~~~

For the simplest case — DataFrames keyed by entity type:

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion.context_builder import ContextBuilder

   context = ContextBuilder.from_dict({
       "Person": pd.DataFrame({
           "__ID__": [1, 2, 3],
           "name": ["Alice", "Bob", "Carol"],
           "age": [30, 25, 35],
       }),
       "Company": pd.DataFrame({
           "__ID__": [10, 20],
           "name": ["Acme", "Globex"],
       }),
   })

Supported Data Sources
~~~~~~~~~~~~~~~~~~~~~~

The :func:`~pycypher.ingestion.data_sources.data_source_from_uri` factory
resolves the correct reader based on URI scheme and file extension:

+----------------------------------------+---------------------------+
| URI pattern                            | Reader                    |
+========================================+===========================+
| ``/path/file.csv``, ``file:///…``      | CSV via DuckDB            |
+----------------------------------------+---------------------------+
| ``/path/file.parquet``                 | Parquet via DuckDB        |
+----------------------------------------+---------------------------+
| ``/path/file.json``                    | JSON via DuckDB           |
+----------------------------------------+---------------------------+
| ``s3://``, ``gs://``, ``https://``     | Remote file via DuckDB    |
+----------------------------------------+---------------------------+
| ``postgresql://``, ``mysql://``, etc.  | SQL query                 |
+----------------------------------------+---------------------------+

SQL sources require a ``query`` parameter:

.. code-block:: python

   context = (
       ContextBuilder()
       .add_entity(
           "Person",
           "postgresql://localhost/mydb",
           query="SELECT id, name, age FROM people",
           id_col="id",
       )
       .build()
   )

YAML Configuration
------------------

For repeatable pipelines, define the entire ETL in a YAML file and run it
with the ``nmetl`` CLI.

Config File Structure
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml

   # pipeline.yaml
   entities:
     - type: Person
       uri: data/people.csv
       id_col: person_id

     - type: Company
       uri: data/companies.parquet

   relationships:
     - type: WORKS_AT
       uri: data/employment.csv
       source_col: person_id
       target_col: company_id

   queries:
     - name: employee_report
       cypher: |
         MATCH (p:Person)-[:WORKS_AT]->(c:Company)
         WHERE p.age >= 30
         RETURN p.name AS employee, c.name AS company, p.age AS age
         ORDER BY p.age DESC
       output:
         uri: output/senior_employees.csv

     - name: company_headcount
       cypher: |
         MATCH (p:Person)-[:WORKS_AT]->(c:Company)
         RETURN c.name AS company, count(p) AS headcount
         ORDER BY headcount DESC
       output:
         uri: output/headcount.parquet

Environment Variables
~~~~~~~~~~~~~~~~~~~~~

YAML values support ``${VAR_NAME}`` placeholders that are substituted from
the process environment:

.. code-block:: yaml

   entities:
     - type: Person
       uri: ${DATA_DIR}/people.csv

The ``nmetl`` CLI
-----------------

The ``nmetl`` command-line tool runs pipelines from YAML configs:

.. code-block:: bash

   # Run a pipeline
   uv run nmetl run pipeline.yaml

   # Validate config without running
   uv run nmetl validate pipeline.yaml

   # List all queries in the config
   uv run nmetl list-queries pipeline.yaml

   # Dry-run: show what would be executed
   uv run nmetl run pipeline.yaml --dry-run

Error Handling
~~~~~~~~~~~~~~

The CLI translates common errors into user-friendly messages:

- **File not found** — reports the missing path
- **Permission denied** — reports the access error
- **Parse errors** — shows the line and column of the syntax issue
- **Validation errors** — lists all config problems before attempting to run

Writing Output
--------------

Query results can be written to CSV, Parquet, or JSON files via
:func:`~pycypher.ingestion.output_writer.write_dataframe_to_uri`:

.. code-block:: python

   from pycypher.ingestion import write_dataframe_to_uri

   write_dataframe_to_uri(result_dataframe, "output/results.csv")
   write_dataframe_to_uri(result_dataframe, "output/results.parquet")

The output format is inferred from the file extension (``.csv``, ``.parquet``,
``.json``).  Parent directories are created automatically.

Security
--------

The ingestion layer includes built-in security checks
(:mod:`pycypher.ingestion.security`):

- **Path traversal prevention** — file paths are sanitized to block ``../``
  escape attempts
- **SQL injection protection** — SQL queries passed to data sources are
  validated against injection patterns
- **URI scheme validation** — only recognised schemes are accepted

Complete Example
----------------

.. code-block:: python

   import pandas as pd
   from pycypher.ingestion.context_builder import ContextBuilder
   from pycypher.star import Star

   # 1. Load data
   people = pd.DataFrame({
       "__ID__": [1, 2, 3],
       "name": ["Alice", "Bob", "Carol"],
       "dept": ["Eng", "Sales", "Eng"],
   })
   knows = pd.DataFrame({
       "__SOURCE__": [1, 2],
       "__TARGET__": [2, 3],
   })

   context = (
       ContextBuilder()
       .add_entity("Person", people)
       .add_relationship("KNOWS", knows)
       .build()
   )

   # 2. Query
   star = Star(context=context)
   result = star.execute_query(
       """
       MATCH (p:Person)-[:KNOWS]->(f:Person)
       RETURN p.name AS person, f.name AS friend, p.dept AS dept
       """
   )
   print(result)

   # 3. Aggregate
   headcount = star.execute_query(
       """
       MATCH (p:Person)
       RETURN p.dept AS department, count(p) AS n
       ORDER BY n DESC
       """
   )
   print(headcount)

Next Steps
----------

* :doc:`basic_query_parsing` — deeper dive into query execution
* :doc:`pattern_matching` — advanced graph pattern techniques
* :doc:`../api/pycypher` — full API reference for all ingestion modules
