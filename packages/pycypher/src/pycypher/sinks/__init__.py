"""Sink adapters for writing pycypher query results to external stores.

Each sub-module is an optional, write-only adapter.  The dependencies are
declared as optional extras so that users who don't need a particular sink
are not forced to install its dependencies.

Available sinks
---------------
``pycypher.sinks.neo4j``
    Write DataFrames to a Neo4j graph database via the official Python
    driver.  Requires the ``neo4j`` package (``uv pip install neo4j``).
"""
