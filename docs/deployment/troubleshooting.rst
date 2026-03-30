Troubleshooting
===============

Common issues and their solutions when deploying and operating PyCypher.

Docker Issues
-------------

``docker compose up`` fails with "variable not set"
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Required environment variables are missing.

**Fix**: Copy and populate the ``.env`` file:

.. code-block:: bash

   cp .env.example .env
   # Set NEO4J_USER, NEO4J_PASSWORD, SPARK_RPC_SECRET, NOMINATIM_PASSWORD

Container starts but ``uv sync`` fails
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: uv cache is stale or corrupted.

**Fix**:

.. code-block:: bash

   # Clear the uv cache volume
   docker compose down
   docker volume rm pycypher-nmetl_uv-cache
   make dev-up

Port conflicts
~~~~~~~~~~~~~~

**Cause**: Another service is using the same port.

**Fix**: Check which ports are in use:

.. code-block:: bash

   # macOS / Linux
   lsof -i :7474   # Neo4j
   lsof -i :7077   # Spark
   lsof -i :8090   # Spark UI
   lsof -i :8092   # Nominatim
   lsof -i :8093   # FastOpenData API

Stop the conflicting service or change the port mapping in
``docker-compose.yml``.

Python / Dependency Issues
--------------------------

``ModuleNotFoundError: No module named 'pycypher'``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Workspace not synced after changes.

**Fix**:

.. code-block:: bash

   uv sync
   uv run python -c "from pycypher import Star; print('OK')"

``Python >= 3.14 required``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: System Python is too old.

**Fix**: Install Python 3.14+ via ``uv``:

.. code-block:: bash

   uv python install 3.14

Backend failures
~~~~~~~~~~~~~~~~

**Cause**: Optional backend dependency not installed.

**Fix**:

.. code-block:: bash

   # Install DuckDB backend
   uv pip install duckdb

   # Install Polars backend
   uv pip install polars

   # Verify
   uv run python -c "from pycypher.backend_engine import select_backend; print(select_backend(hint='pandas').name)"

Spark Issues
------------

Spark worker cannot connect to master
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: RPC secret mismatch or network issue.

**Fix**: Verify the secret is consistent:

.. code-block:: bash

   # Both master and worker must use the same SPARK_RPC_SECRET
   docker compose logs spark-master | grep -i "auth"
   docker compose logs spark-worker | grep -i "auth"

Spark job fails with OOM
~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Worker memory is insufficient.

**Fix**: Increase worker memory in ``docker-compose.yml``:

.. code-block:: yaml

   spark-worker:
     environment:
       - SPARK_WORKER_MEMORY=4G

Or scale out with more workers:

.. code-block:: bash

   make spark-scale WORKERS=4

Neo4j Issues
------------

Neo4j authentication failure
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Credentials in ``.env`` do not match the Neo4j volume state.

**Fix**: If you changed the password after first setup, reset the volume:

.. code-block:: bash

   make neo4j-down
   docker volume rm pycypher-nmetl_neo4j-data
   make neo4j-up

Neo4j APOC procedures not available
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: APOC plugin failed to install.

**Fix**: Check logs and ensure the plugin config is correct:

.. code-block:: bash

   make neo4j-logs
   # Look for APOC-related messages

Nominatim Issues
----------------

Nominatim import takes hours
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Expected**: The first-start import of the US OSM extract takes several hours
and requires approximately 32 GB of RAM.  Subsequent starts skip the import.

.. code-block:: bash

   # Watch progress
   make nominatim-logs

   # Check status
   make nominatim-status

Nominatim returns empty results
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Import is incomplete or PBF file is missing.

**Fix**: Ensure the PBF exists before starting Nominatim:

.. code-block:: bash

   ls packages/fastopendata/raw_data/us-latest.osm.pbf

If missing, download it via the fastopendata container:

.. code-block:: bash

   make fod-shell
   # Inside container: follow DATASETS.md instructions to download the PBF

Query Execution Issues
----------------------

Query hangs or runs too long
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Fix**: Set a timeout:

.. code-block:: python

   result = star.execute_query("...", timeout_seconds=30)

Cross-product produces too many rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Fix**: The cross-join limit prevents runaway cartesian products:

.. code-block:: python

   # Default limit applies; increase if needed
   result = star.execute_query("...", max_cross_join_rows=500_000)

``GraphTypeNotFoundError``
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Cause**: Query references an entity or relationship type not in the context.

**Fix**: Check available types:

.. code-block:: python

   print(repr(context))
   # Context(backend='pandas', entities={'Person': 4}, relationships={'KNOWS': 3})

Getting Help
------------

* **Issue tracker**: Report bugs at the GitHub repository
* **Test suite**: Run ``uv run pytest -x`` to identify failures
* **Logs**: Check ``make dev-logs`` for container output
* **Metrics**: Use ``QUERY_METRICS.snapshot().diagnostic_report()`` for execution stats
