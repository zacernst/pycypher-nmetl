Security Hardening
==================

This guide covers security hardening for production PyCypher deployments.
It is based on the internal security audit that identified and mitigated
vulnerabilities in the data ingestion pipeline, query engine, and external
integrations.

.. contents:: On this page
   :local:
   :depth: 2


Production Security Checklist
-----------------------------

Complete every item before going live.  Items marked **CRITICAL** must not
be deferred.

.. list-table::
   :widths: 10 60 15 15
   :header-rows: 1

   * - #
     - Item
     - Severity
     - Status
   * - 1
     - Environment secrets are generated with ``openssl rand -hex 32``
     - CRITICAL
     -
   * - 2
     - ``.env`` file is excluded from version control and has mode ``0600``
     - CRITICAL
     -
   * - 3
     - Neo4j TLS is enabled (``bolt+s://`` or ``encrypted=True``)
     - CRITICAL
     -
   * - 4
     - Containers run as non-root (UID 1000)
     - HIGH
     -
   * - 5
     - Docker socket is not mounted into containers
     - HIGH
     -
   * - 6
     - ``PYCYPHER_AST_CACHE_MAX`` is tuned for expected query diversity
     - MEDIUM
     -
   * - 7
     - ``PYROSCOPE_SERVER`` (if set) points to an internal monitoring host
     - MEDIUM
     -
   * - 8
     - Log output is directed to a centralised log aggregator (not stdout)
     - MEDIUM
     -
   * - 9
     - Network policies restrict egress from PyCypher containers
     - MEDIUM
     -
   * - 10
     - Query timeout is configured via ``--timeout`` or ``PYCYPHER_QUERY_TIMEOUT_MS``
     - MEDIUM
     -


Input Validation
----------------

PyCypher applies defence-in-depth to every user-controlled input.
Understanding the validation layers helps operators diagnose rejected queries
and tune security policy.

SQL Query Validation
~~~~~~~~~~~~~~~~~~~~

User-supplied SQL queries (for data source ingestion) pass through four
validation phases:

1. **Comment stripping** -- removes ``--``, ``#``, and ``/* */`` comments
   to prevent comment-based injection.
2. **Statement multiplicity** -- rejects queries with more than one
   semicolon (multi-statement injection).
3. **Keyword allowlist** -- only ``SELECT`` and ``WITH`` are permitted as
   the first keyword.
4. **Table function blocklist** -- dangerous DuckDB table functions are
   rejected even inside valid ``SELECT`` queries.

The following DuckDB functions are blocked:

.. code-block:: text

   read_csv, read_csv_auto, read_parquet, read_json, read_json_auto,
   read_json_objects, read_json_objects_auto, read_blob, read_text,
   read_ndjson, read_ndjson_auto, read_ndjson_objects, glob,
   parquet_scan, parquet_metadata, parquet_schema,
   parquet_file_metadata, parquet_kv_metadata, sniff_csv,
   query_table, query

Functions with these prefixes are also blocked: ``duckdb_``, ``pg_``,
``pragma_``, ``information_schema``.

**Why this matters:** Without the table function blocklist, a query such as
``SELECT * FROM read_csv('/etc/passwd')`` would pass the keyword allowlist
(it starts with ``SELECT``) and read arbitrary files from the filesystem.

Path Validation
~~~~~~~~~~~~~~~

File paths are validated against:

* **Path traversal** -- ``..`` components are rejected.
* **Sensitive prefixes** -- ``/etc/``, ``/proc/``, ``/sys/``, ``/dev/``,
  ``/root/``, ``/var/run/``, ``/var/lib/``, ``/boot/``, ``/sbin/`` are
  blocked.
* **SQL string literal safety** -- single quotes, NUL bytes, URL-encoded
  attacks (``%27``), and Unicode normalisation attacks are rejected when
  paths are interpolated into DuckDB SQL.

SSRF Protection
~~~~~~~~~~~~~~~

HTTP/HTTPS URIs are checked for Server-Side Request Forgery (SSRF):

* **Blocked hostnames:** ``localhost``, ``ip6-localhost``, etc.
* **Blocked IP ranges:** RFC 1918 private, loopback, link-local, reserved.
* **DNS resolution:** hostnames are resolved and the resulting IP is
  checked against the same blocklist.

.. warning::

   DNS rebinding attacks (TOCTOU) are a known limitation.  The hostname is
   resolved once during validation, but the downstream HTTP library may
   resolve it again.  For maximum protection in untrusted environments,
   use network-level egress controls (see :ref:`network-security` below).

Regex Safety
~~~~~~~~~~~~

The Cypher ``=~`` regex operator validates patterns before execution:

* **Length limit** -- patterns longer than 1,000 characters are rejected.
* **Syntax check** -- invalid regex is rejected before reaching the engine.
* **ReDoS detection** -- patterns with nested quantifiers (``(a+)+``),
  overlapping alternation (``(a|a)*``), or chained quantifiers
  (``a{n}{m}``) are rejected to prevent catastrophic backtracking.


Environment Hardening
---------------------

Secrets
~~~~~~~

Generate strong random values for all credentials:

.. code-block:: bash

   # Generate secrets
   export NEO4J_PASSWORD=$(openssl rand -hex 32)
   export SPARK_RPC_SECRET=$(openssl rand -hex 32)
   export NOMINATIM_PASSWORD=$(openssl rand -hex 32)

   # Write to .env (never commit this file)
   cat > .env << EOF
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=${NEO4J_PASSWORD}
   SPARK_RPC_SECRET=${SPARK_RPC_SECRET}
   NOMINATIM_PASSWORD=${NOMINATIM_PASSWORD}
   EOF
   chmod 600 .env

Security-Sensitive Environment Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These variables control security-relevant behaviour:

.. list-table::
   :widths: 30 15 55
   :header-rows: 1

   * - Variable
     - Default
     - Security Notes
   * - ``PYCYPHER_AST_CACHE_MAX``
     - ``1024``
     - Maximum cached AST entries.  Set lower in memory-constrained
       environments.  Set higher for ETL pipelines with many unique queries.
   * - ``PYCYPHER_QUERY_TIMEOUT_MS``
     - (none)
     - Per-query timeout in milliseconds.  **Strongly recommended** for
       production to prevent runaway queries from consuming resources.
   * - ``PYCYPHER_LOG_LEVEL``
     - ``WARNING``
     - Set to ``INFO`` for audit trails.  Avoid ``DEBUG`` in production
       (logs query text which may contain sensitive data).
   * - ``PYCYPHER_LOG_FORMAT``
     - text
     - Set to ``json`` for structured logging with log aggregators.
   * - ``PYROSCOPE_SERVER``
     - (disabled)
     - Profiling data endpoint.  **Must** point to a trusted internal host.
       An attacker who controls this variable can exfiltrate performance
       data to an external server.
   * - ``PYROSCOPE_ENABLED``
     - ``false``
     - Disable in production unless actively profiling.  Profiling adds
       CPU overhead and exposes timing information.


.. _network-security:

Network Security
----------------

Egress Restrictions
~~~~~~~~~~~~~~~~~~~

PyCypher containers should have restricted egress to prevent data
exfiltration via cloud read functions or SSRF bypasses:

.. code-block:: yaml

   # docker-compose.override.yml — production egress restrictions
   services:
     pycypher:
       networks:
         - internal
       # No access to external networks
     neo4j:
       networks:
         - internal
     spark-master:
       networks:
         - internal

   networks:
     internal:
       internal: true  # No external connectivity

If the pipeline must read from cloud storage (S3, GCS, Azure), use a
proxy or IAM role with minimum required permissions rather than granting
broad network access.

Neo4j Connection Security
~~~~~~~~~~~~~~~~~~~~~~~~~

Always enable TLS for Neo4j connections in production:

.. code-block:: python

   import os
   from pycypher.sinks.neo4j import Neo4jSink

   # Use bolt+s:// scheme for TLS
   with Neo4jSink(
       "bolt+s://neo4j-host:7687",
       "neo4j",
       os.environ["NEO4J_PASSWORD"],
   ) as sink:
       sink.write_nodes(result, mapping)

The Neo4j sink validates all identifiers (labels, property names,
relationship types) against Cypher injection, including:

* Backtick escaping attacks
* NUL byte injection
* Unicode normalisation confusables (e.g., fullwidth grave accent)


Resource Limits
---------------

Query Timeout
~~~~~~~~~~~~~

Set a per-query timeout to prevent runaway queries:

.. code-block:: bash

   # Via CLI
   nmetl query --timeout 30000 config.yaml  # 30 second limit

   # Via environment
   export PYCYPHER_QUERY_TIMEOUT_MS=30000

BFS Path Expansion
~~~~~~~~~~~~~~~~~~

Variable-length path patterns (``[*1..10]``) use BFS expansion with
two safety limits:

* **Hop limit:** unbounded paths (``[*]``) are capped at 20 hops.
* **Frontier limit:** the BFS frontier is truncated at 1,000,000 rows
  to prevent memory exhaustion in highly-connected graphs.

If your graph has high average degree (>100 edges per node), consider
adding explicit hop bounds to variable-length patterns.

AST Cache
~~~~~~~~~

The query parser caches AST results with LRU eviction:

* **Default size:** 1,024 entries.
* **Configuration:** ``PYCYPHER_AST_CACHE_MAX`` environment variable.
* **Monitoring:** cache hit rate, eviction count, and current size are
  available via ``parser.cache_stats``.

For ETL pipelines with a fixed set of queries, the default is sufficient.
For applications generating many unique queries (e.g., parameterised by
user input), monitor eviction rates and increase the limit if hit rate
drops below 80%.


Monitoring and Alerting
-----------------------

Security Events to Monitor
~~~~~~~~~~~~~~~~~~~~~~~~~~

Configure alerts for these log patterns:

.. list-table::
   :widths: 40 30 30
   :header-rows: 1

   * - Log Pattern
     - Indicates
     - Action
   * - ``SecurityError``
     - Blocked SQL injection, path traversal, or SSRF attempt
     - Investigate source IP / user
   * - ``Dangerous DuckDB table function``
     - Attempted file system access via SQL query
     - Block source, review query logs
   * - ``SSRF protection``
     - Attempted access to private/internal network
     - Review URI source, tighten egress
   * - ``BFS frontier.*exceeds safety limit``
     - Possible resource exhaustion attack via complex queries
     - Review query patterns, add hop bounds
   * - ``catastrophic backtracking``
     - ReDoS attack via regex ``=~`` operator
     - Block pattern, review query source
   * - ``Query.*timed out``
     - Runaway query (accidental or malicious)
     - Review query, tune timeout


Structured Logging
~~~~~~~~~~~~~~~~~~

Enable JSON logging for machine-parseable security audit trails:

.. code-block:: bash

   export PYCYPHER_LOG_FORMAT=json
   export PYCYPHER_LOG_LEVEL=INFO


Docker Security
---------------

The production Dockerfile follows container security best practices:

* **Non-root user:** runs as UID 1000 (not root).
* **Minimal base image:** Python slim variant.
* **No shell access needed:** consider using ``--read-only`` filesystem.
* **Health checks:** Docker health checks verify the engine is responsive.

Additional hardening for production:

.. code-block:: yaml

   services:
     pycypher:
       read_only: true
       tmpfs:
         - /tmp
       security_opt:
         - no-new-privileges:true
       cap_drop:
         - ALL
       mem_limit: 4g          # Prevent OOM from taking down the host
       memswap_limit: 4g      # Disable swap to avoid performance collapse


Function Registration Security
------------------------------

The YAML configuration system supports registering custom Python functions
as Cypher scalar functions.  This is protected by:

* **Module blocklist:** imports from ``os``, ``subprocess``, ``sys``,
  ``socket``, ``pickle``, ``ctypes``, ``importlib``, and 13 other
  dangerous modules are rejected.
* **Import path validation:** only dotted Python identifiers
  (``package.module.function``) are accepted.
* **Top-level check:** the first component of the import path is checked
  against the blocklist.

.. warning::

   Custom function registration executes arbitrary Python code from the
   imported module.  Only register functions from trusted, audited packages.
   Never allow end users to specify import paths directly.


Incident Response
-----------------

If a security event is detected:

1. **Contain** -- stop the affected container immediately:

   .. code-block:: bash

      docker compose stop pycypher

2. **Preserve evidence** -- save logs before they rotate:

   .. code-block:: bash

      docker compose logs pycypher > incident_$(date +%s).log

3. **Investigate** -- search logs for the triggering event:

   .. code-block:: bash

      grep -i "SecurityError\|SSRF\|injection\|backtracking" incident_*.log

4. **Remediate** -- apply the appropriate fix:

   * SQL injection attempt → review and tighten query validation
   * SSRF attempt → add network egress restrictions
   * Resource exhaustion → lower timeout, add hop bounds
   * ReDoS → the pattern is already blocked; check for bypass attempts

5. **Post-incident** -- update monitoring rules and review access controls.
