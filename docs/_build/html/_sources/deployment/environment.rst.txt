Environment Configuration
=========================

PyCypher uses environment variables for service credentials and connection
strings.  All required variables must be set before running ``docker compose``.

Setup
-----

.. code-block:: bash

   # Copy the template
   cp .env.example .env

   # Edit with real values
   $EDITOR .env

Required Variables
------------------

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Variable
     - Example
     - Description
   * - ``NEO4J_USER``
     - ``neo4j``
     - Neo4j database username
   * - ``NEO4J_PASSWORD``
     - ``<strong-password>``
     - Neo4j database password
   * - ``SPARK_RPC_SECRET``
     - ``<random-secret>``
     - Spark inter-node RPC authentication secret
   * - ``NOMINATIM_PASSWORD``
     - ``<strong-password>``
     - Nominatim PostgreSQL password

Optional Variables
------------------

.. list-table::
   :widths: 25 25 50
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``GITHUB_TOKEN``
     - (empty)
     - GitHub token for private dependencies
   * - ``CODE_SERVER_PASSWORD``
     - (none)
     - Password for browser-based VS Code
   * - ``NOMINATIM_URL``
     - ``http://nominatim:8080``
     - Nominatim endpoint (inside Docker network)
   * - ``NEO4J_URI``
     - ``bolt://neo4j:7687``
     - Neo4j Bolt endpoint

Connection URLs
~~~~~~~~~~~~~~~

Services are accessible at different URLs depending on whether you are inside
the Docker network or on the host:

.. list-table::
   :widths: 20 40 40
   :header-rows: 1

   * - Service
     - Inside Docker
     - From Host
   * - Spark Master
     - ``spark://spark-master:7077``
     - ``spark://localhost:7077``
   * - Neo4j Bolt
     - ``bolt://neo4j:7687``
     - ``bolt://localhost:7687``
   * - Neo4j Browser
     - --
     - ``http://localhost:7474``
   * - Spark UI
     - --
     - ``http://localhost:8090``
   * - Nominatim
     - ``http://nominatim:8080``
     - ``http://localhost:8092``
   * - FastOpenData API
     - ``http://fastopendata-api:8000``
     - ``http://localhost:8093``

Monitoring & Observability
--------------------------

.. list-table::
   :widths: 30 20 50
   :header-rows: 1

   * - Variable
     - Default
     - Description
   * - ``PYCYPHER_METRICS_ENABLED``
     - ``1``
     - Enable in-process metrics collection (``0`` to disable)
   * - ``PYCYPHER_SLOW_QUERY_MS``
     - ``1000``
     - Threshold (ms) for flagging slow queries
   * - ``PYCYPHER_METRICS_EXPORT``
     - (empty)
     - Comma-separated exporters: ``prometheus``, ``statsd``, ``json``
   * - ``PYCYPHER_METRICS_PREFIX``
     - ``pycypher``
     - Metric name prefix for exporters
   * - ``PYCYPHER_METRICS_EXPORT_INTERVAL_S``
     - ``60``
     - Push interval for metrics export (seconds)
   * - ``PYCYPHER_STATSD_HOST``
     - ``127.0.0.1``
     - StatsD/Datadog agent host
   * - ``PYCYPHER_STATSD_PORT``
     - ``8125``
     - StatsD/Datadog agent port
   * - ``PYCYPHER_METRICS_JSON_PATH``
     - ``metrics.jsonl``
     - Output path for JSON file exporter
   * - ``PYCYPHER_LOG_LEVEL``
     - ``WARNING``
     - Logging level (DEBUG, INFO, WARNING, ERROR)
   * - ``PYCYPHER_LOG_FORMAT``
     - ``rich``
     - Log format: ``rich`` (console) or ``json`` (structured)
   * - ``PYCYPHER_OTEL_ENABLED``
     - ``0``
     - Enable OpenTelemetry distributed tracing
   * - ``OTEL_SERVICE_NAME``
     - ``pycypher``
     - Service name for OTel spans
   * - ``PYROSCOPE_ENABLED``
     - ``0``
     - Enable Pyroscope continuous profiling
   * - ``PYROSCOPE_SERVER``
     - ``http://localhost:4040``
     - Pyroscope server endpoint
   * - ``PYROSCOPE_APP_NAME``
     - ``pycypher``
     - Application name for Pyroscope

Python Environment
------------------

PyCypher requires Python 3.14+ and uses ``uv`` for dependency management.

.. code-block:: bash

   # Install uv
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # Sync all workspace dependencies
   uv sync

   # Verify installation
   uv run python -c "from pycypher import Star; print('OK')"

The workspace is structured as a monorepo with cross-package dependencies:

* ``packages/shared`` -- common utilities (no dependencies on other packages)
* ``packages/pycypher`` -- core engine (depends on ``shared``)
* ``packages/fastopendata`` -- data pipeline (depends on ``pycypher``)

After editing any ``pyproject.toml``, always re-sync:

.. code-block:: bash

   uv sync

Security Notes
--------------

* Never commit ``.env`` files to version control (already in ``.gitignore``)
* Generate strong random secrets for ``SPARK_RPC_SECRET`` and passwords
* The production Dockerfile runs as a non-root user (UID 1000)
* Spark RPC authentication and encryption are enabled by default
* Neo4j heap and page cache are capped to prevent OOM (1 GB heap, 512 MB page cache)
