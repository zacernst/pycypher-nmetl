Deployment & Scaling
====================

This guide covers production deployment, container orchestration, scaling
strategies, monitoring, and operational best practices for PyCypher.

.. toctree::
   :maxdepth: 2
   :caption: Topics:

   security
   docker
   environment
   scaling
   monitoring
   troubleshooting


Architecture Overview
---------------------

PyCypher deployments consist of these components:

* **PyCypher core** -- the Cypher query engine (always required)
* **Spark cluster** -- optional distributed compute for large datasets
* **Neo4j** -- optional graph database sink for writing query results
* **Nominatim** -- optional geocoding service for geospatial ETL
* **FastOpenData API** -- optional REST API for data ingestion

All components are containerised and orchestrated via Docker Compose.  The
Makefile provides convenience targets for every operational task.

Deployment Modes
~~~~~~~~~~~~~~~~

.. list-table::
   :widths: 20 30 50
   :header-rows: 1

   * - Mode
     - Command
     - What it starts
   * - Development
     - ``make dev-up``
     - Dev container + Spark + Neo4j
   * - Infrastructure only
     - ``make infra-up``
     - Spark master/worker + Neo4j
   * - Spark cluster
     - ``make spark-up``
     - Spark master + worker(s)
   * - Neo4j only
     - ``make neo4j-up``
     - Neo4j 5 Community with APOC
   * - FastOpenData
     - ``make fod-up``
     - ETL pipeline container
   * - FastOpenData API
     - ``make fod-api-up``
     - REST API on port 8093
   * - Geocoding
     - ``make nominatim-up``
     - Nominatim (first start imports OSM data)

Prerequisites
~~~~~~~~~~~~~

* **Docker** (via Docker Desktop or Rancher Desktop)
* **Python 3.14+** (for local development outside containers)
* **uv** package manager (``pip install uv``)
* **make** (standard on macOS/Linux)
