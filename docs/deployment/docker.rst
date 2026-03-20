Docker Containerisation
=======================

PyCypher ships two Dockerfiles and a Docker Compose configuration that
orchestrates the full development and integration stack.

Images
------

Production Image (``Dockerfile``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The production image uses a **multi-stage build** to minimize image size.
Stage 1 installs dependencies with build tools (compilers, git).  Stage 2
copies only the virtual environment into a ``python:3.14-slim`` runtime
image, excluding all build-only tooling.

.. code-block:: bash

   docker build -t pycypher:latest .

Key characteristics:

* **Multi-stage build** -- ~400 MB final image (vs ~1.2 GB single-stage)
* **Non-root user** (``appuser``, UID 1000) for security hardening
* **Built-in health check** -- ``HEALTHCHECK`` directive runs ``nmetl health``
* **Health server default** -- ``CMD`` starts the HTTP health endpoint on port 8079
* Layer-cache-optimized -- dependency manifests copied before source code

Running the production image:

.. code-block:: bash

   # Start with health server (default CMD)
   docker run -p 8079:8079 pycypher:latest

   # Run a one-off query instead
   docker run pycypher:latest nmetl query --inline "MATCH (n) RETURN n"

   # Start health server on all interfaces
   docker run -p 8079:8079 pycypher:latest nmetl health-server --bind 0.0.0.0

Development Image (``Dockerfile.dev``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The development image adds interactive tooling on top of the same Python base.

.. code-block:: bash

   docker build -f Dockerfile.dev -t pycypher-dev:latest .

Additional tools included:

* ``ipython``, ``ipdb`` for interactive debugging
* ``pytest``, ``pytest-cov`` for testing
* ``sudo`` access for the developer user
* VS Code Dev Container compatible

Docker Compose
--------------

The ``docker-compose.yml`` defines the full multi-service stack.  All services
share the ``pycypher-dev-network`` bridge network.

Quick Start
~~~~~~~~~~~

.. code-block:: bash

   # 1. Copy and populate the environment file
   cp .env.example .env
   # Edit .env with real credentials (see Environment Configuration)

   # 2. Start the development stack
   make dev-up

   # 3. Access the dev container
   make dev-shell

   # 4. Run the test suite inside the container
   cd /workspace && uv run pytest

Services
~~~~~~~~

.. list-table::
   :widths: 20 15 15 50
   :header-rows: 1

   * - Service
     - Container
     - Ports
     - Notes
   * - pycypher-dev
     - pycypher-dev
     - --
     - Main dev container; ``uv sync`` on start
   * - spark-master
     - pycypher-spark-master
     - 7077, 8090
     - Bitnami Spark 3.5; RPC auth + encryption
   * - spark-worker
     - pycypher-spark-worker
     - 8091
     - 2 GB RAM, 2 cores per worker
   * - neo4j
     - pycypher-neo4j
     - 7474, 7687
     - Neo4j 5 Community + APOC plugin
   * - nominatim
     - pycypher-nominatim
     - 8092
     - US OSM geocoder; 300s start period
   * - fastopendata
     - pycypher-fastopendata
     - --
     - ETL pipeline container
   * - fastopendata-api
     - pycypher-fastopendata-api
     - 8093
     - REST API (Swagger at /docs)
   * - code-server
     - pycypher-code-server
     - 8080
     - VS Code in browser (profile: code-server)

Optional Profiles
~~~~~~~~~~~~~~~~~

Some services are behind Docker Compose profiles to avoid starting them by
default:

.. code-block:: bash

   # Start with Jupyter Lab
   make dev-jupyter
   # → http://localhost:8888

   # Start with browser-based VS Code
   make dev-vscode
   # → http://localhost:8080

Volumes
~~~~~~~

Persistent volumes ensure data survives container restarts:

.. list-table::
   :widths: 30 70
   :header-rows: 1

   * - Volume
     - Purpose
   * - ``uv-cache``
     - Python package cache (speeds up rebuilds)
   * - ``pytest-cache``
     - Test result cache
   * - ``neo4j-data``
     - Neo4j graph database files
   * - ``neo4j-logs``
     - Neo4j server logs
   * - ``neo4j-import``
     - Neo4j CSV import directory
   * - ``spark-events``
     - Spark event history for the History Server UI
   * - ``fastopendata-raw``
     - Downloaded raw datasets
   * - ``nominatim-data``
     - PostgreSQL database for Nominatim

Building Custom Images
~~~~~~~~~~~~~~~~~~~~~~

When adding new Python dependencies:

.. code-block:: bash

   # Rebuild just the dev container
   make dev-rebuild

   # Rebuild all images
   docker compose build

   # Rebuild with no cache (after Dockerfile changes)
   docker compose build --no-cache pycypher-dev

Health Checks
~~~~~~~~~~~~~

All key containers include health checks:

* **pycypher-dev**: ``nmetl health`` every 30s (60s start grace period)
* **Neo4j**: ``wget http://localhost:7474`` every 10s (10 retries)
* **Nominatim**: geocode query every 30s (20 retries, 300s start grace period)

The ``nmetl health`` command returns exit code 0 (healthy), 1 (degraded), or
2 (unhealthy), making it compatible with Docker ``HEALTHCHECK`` and Kubernetes
liveness/readiness probes.

Check health status:

.. code-block:: bash

   docker compose ps
   # Shows health status for each service

   # Detailed health report from the dev container
   docker compose exec pycypher-dev uv run nmetl health --json

Kubernetes Deployment
~~~~~~~~~~~~~~~~~~~~~

The production image works directly with Kubernetes.  The ``nmetl health-server``
command provides standard HTTP endpoints for liveness and readiness probes:

.. code-block:: yaml

   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: pycypher
   spec:
     replicas: 1
     selector:
       matchLabels:
         app: pycypher
     template:
       metadata:
         labels:
           app: pycypher
       spec:
         containers:
           - name: pycypher
             image: pycypher:latest
             ports:
               - containerPort: 8079
             livenessProbe:
               httpGet:
                 path: /health
                 port: 8079
               initialDelaySeconds: 10
               periodSeconds: 30
             readinessProbe:
               httpGet:
                 path: /ready
                 port: 8079
               initialDelaySeconds: 5
               periodSeconds: 10

The ``/metrics`` endpoint serves Prometheus text format, ready for scraping
by Prometheus or any OpenMetrics-compatible collector.
