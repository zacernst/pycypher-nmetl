Usage
=====

Package Management with uv
-----------------------

pycypher-nmetl uses `uv <https://github.com/astral-sh/uv>`_ for package management. uv is a fast, reliable Python package installer and resolver. All package installation commands in this documentation use uv.

To install packages:

.. code-block:: bash

    uv pip install <package-name>

To run Python tools without installing them globally:

.. code-block:: bash

    uv run <tool-name> [arguments]

Basic Usage
----------

Import the necessary modules:

.. code-block:: python

    from pycypher import CypherQuery
    from nmetl import ETLPipeline

Creating a Cypher Query
----------------------

.. code-block:: python

    query = CypherQuery("""
        MATCH (n:Person)
        WHERE n.age > 30
        RETURN n.name, n.age
    """)

Building an ETL Pipeline
-----------------------

.. code-block:: python

    pipeline = ETLPipeline()
    pipeline.add_source(query)
    pipeline.add_transformation(lambda data: data.assign(age_group=data.age // 10 * 10))
    pipeline.add_sink("output.csv")

    # Execute the pipeline
    pipeline.execute()

Advanced Usage
-------------

For more advanced usage examples, please refer to the API documentation.
