Threat Model & Trust Boundaries
================================

This document defines the formal trust boundaries, data flow analysis,
and security-sensitive operations in the PyCypher-nmetl system. It serves
as the baseline for systematic security review per Amendment Cycle 2.

.. contents:: On this page
   :local:
   :depth: 3

Overview
--------

PyCypher-nmetl is an ETL pipeline system that:

1. Parses user-supplied Cypher queries against in-process data.
2. Ingests data from external sources (files, databases, cloud storage).
3. Transforms data through a query execution engine.
4. Outputs results to files or graph databases (Neo4j).

The system runs as a single-process application with no built-in
authentication or multi-tenancy. Security controls focus on input
validation, resource limits, and preventing unintended data access.

Trust Boundaries
----------------

The system has five trust boundaries where data crosses from a
less-trusted domain into a more-trusted one.

.. code-block:: text

   ┌─────────────────────────────────────────────────────────────────┐
   │                    UNTRUSTED ZONE                               │
   │  User Cypher queries, YAML configs, file paths, SQL fragments  │
   │  Environment variables, CLI arguments, external URIs           │
   └──────────────┬──────────────────────────────────┬──────────────┘
                  │ TB-1: User Input                 │ TB-2: Config
                  ▼                                  ▼
   ┌──────────────────────┐        ┌─────────────────────────────┐
   │  Query Parser &      │        │  Config Loader              │
   │  Semantic Validator   │        │  (YAML + env var subst.)    │
   │  input_validator.py   │        │  ingestion/config.py        │
   └──────────┬───────────┘        └──────────┬──────────────────┘
              │                               │
              ▼                               ▼
   ┌──────────────────────────────────────────────────────────────┐
   │              VALIDATED ZONE (in-process)                      │
   │  Parsed AST, validated config, sanitized identifiers          │
   └───────┬──────────────────┬──────────────────┬────────────────┘
           │ TB-3: Data I/O   │ TB-4: SQL Exec   │ TB-5: Output
           ▼                  ▼                   ▼
   ┌──────────────┐  ┌───────────────┐  ┌─────────────────────┐
   │ File System  │  │ DuckDB Engine │  │ Neo4j / File Output │
   │ Cloud (S3/GS)│  │ (in-process)  │  │ (external)          │
   └──────────────┘  └───────────────┘  └─────────────────────┘

TB-1: User Input Boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Crossing point:** User-supplied Cypher queries enter the system.

**Entry points:**

- ``star.execute_query()`` — primary query API
- ``star.add_cypher_query()`` — pipeline query registration
- CLI ``pycypher run`` command

**Protections:**

- Cypher parser rejects malformed syntax
  (``pycypher/parser.py``)
- Semantic validator detects undefined variables, type mismatches
  (``pycypher/semantic_validator.py``)
- Input validator enforces query ID uniqueness and non-empty content
  (``pycypher/input_validator.py:44-141``)
- Resource limits: cross-join row ceiling, query timeout, memory budget

**Residual risks:**

- Valid but expensive queries (e.g., deep recursive traversals) may
  still consume significant resources within configured limits.

TB-2: Configuration Boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Crossing point:** YAML pipeline configs and environment variables.

**Entry points:**

- ``load_pipeline_config()`` — YAML config loading
  (``ingestion/config.py:829-854``)
- Environment variable substitution ``${VAR}`` in config values
  (``ingestion/config.py:743-826``)

**Protections:**

- ``yaml.safe_load()`` prevents deserialization attacks
  (``ingestion/config.py:852``)
- Blocked environment variables: ``AWS_SECRET_ACCESS_KEY``,
  ``AWS_SESSION_TOKEN``, ``AZURE_CLIENT_SECRET``,
  ``GOOGLE_APPLICATION_CREDENTIALS``, ``GCP_SERVICE_ACCOUNT_KEY``,
  ``API_SECRET``, ``SECRET_KEY``, ``PRIVATE_KEY``, ``GITHUB_TOKEN``,
  ``GITLAB_TOKEN``, ``NPM_TOKEN``, ``PYPI_TOKEN``,
  ``SSH_PRIVATE_KEY``, ``SSL_KEY``, ``TLS_KEY``
  (``ingestion/config.py:745-771``)
- Blocked prefixes: ``PRIVATE_``, ``CREDENTIAL_``
- Blocked module imports in config context: ``os``, ``subprocess``,
  ``pickle``, ``ctypes``, ``socket``, ``shutil``, etc.
  (``ingestion/config.py:379-400``)
- Unresolved or blocked vars left as ``${VAR}`` placeholders (fail-safe)

**Residual risks:**

- Non-blocked env vars containing secrets (custom naming conventions)
  will be substituted. Operators should audit env var names used in
  configs.

TB-3: Data I/O Boundary
~~~~~~~~~~~~~~~~~~~~~~~~~

**Crossing point:** Reading external data from files, databases, cloud.

**Entry points:**

- ``FileDataSource.read()`` — file-based data ingestion
  (``ingestion/data_sources.py:432``)
- URI scheme handler: ``file://``, ``s3://``, ``gs://``, ``https://``,
  ``postgresql://``, ``mysql://``, ``sqlite://``, ``duckdb://``, bare paths

**Protections:**

- **Path traversal prevention:**
  ``sanitize_file_path()`` rejects ``..`` components, blocks sensitive
  directories (``/etc/``, ``/root/``, ``/proc/``, ``/sys/``, ``/dev/``,
  ``/var/run/``, ``/boot/``, ``/sbin/``), resolves symlinks to prevent
  TOCTOU attacks (``ingestion/security.py:120-189``)

- **URI scheme allowlist:**
  ``validate_uri_scheme()`` restricts to known-safe schemes
  (``ingestion/security.py:560-609``)

- **SSRF protection:**
  ``_check_ssrf_hostname()`` blocks RFC 1918 private ranges, loopback,
  link-local, reserved addresses, and internal hostnames
  (``ingestion/security.py:612-697``). DNS resolution failures are
  rejected (fail-closed).

- **Credential detection:**
  ``mask_uri_credentials()`` masks passwords in URIs before logging
  (``ingestion/security.py:770``). CLI ``security_check`` command warns
  on embedded credentials (``cli/security.py:120-143``).

**Residual risks:**

- Cloud storage access relies on ambient credentials (IAM roles, env
  vars). Misconfigured IAM policies could expose unintended data.
- TOCTOU window between ``resolve()`` and actual open is minimal but
  non-zero on adversarial filesystems.

TB-4: SQL Execution Boundary
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Crossing point:** SQL fragments passed to DuckDB for data reading.

**Entry points:**

- ``validate_sql_query()`` — SQL validation gate
  (``ingestion/security.py:402-481``)
- ``sanitize_sql_identifier()`` — table/column name validation
  (``ingestion/security.py:484``)
- ``validate_identifier()`` — DuckDB backend identifier validation
  (``backends/_helpers.py:10-39``)

**Protections:**

- **Statement stacking prevention:** rejects multiple semicolons
  (``ingestion/security.py:441``)
- **DDL/DML blocking:** rejects ``DROP``, ``ALTER``, ``CREATE``,
  ``TRUNCATE``, ``INSERT``, ``UPDATE``, ``DELETE``
- **Dangerous function blocking:** ``read_csv``, ``read_parquet``,
  ``read_json``, ``glob``, and DuckDB system functions blocked
  (``ingestion/security.py:317-341``)
- **Dangerous prefix blocking:** ``duckdb_``, ``pg_``, ``pragma_``,
  ``information_schema`` (``ingestion/security.py:344-349``)
- **Comment stripping:** removes ``--`` and ``/* */`` before validation
  (``ingestion/security.py:192``)
- **NUL byte rejection:** prevents C-level string truncation attacks
  (``ingestion/security.py:503, 715``)
- **Identifier pattern:** ``^[A-Za-z_][A-Za-z0-9_]*$`` enforced

**Residual risks:**

- DuckDB runs in-process with full host permissions. A bypass of SQL
  validation could access arbitrary host resources.
- New DuckDB versions may introduce functions not yet on the blocklist.

TB-5: Output Boundary
~~~~~~~~~~~~~~~~~~~~~~~

**Crossing point:** Query results written to files or external databases.

**Entry points:**

- ``write_dataframe_to_uri()`` — file output (CSV, Parquet, JSON)
  (``ingestion/output_writer.py:37-119``)
- ``Neo4jSink`` — graph database output
  (``sinks/neo4j.py:1-349``)

**Protections:**

- **File output:** path validated via ``sanitize_file_path()``
  (``output_writer.py:86``)
- **Neo4j Cypher injection prevention:**
  ``_validate_cypher_identifier()`` rejects backticks, NUL bytes,
  curly braces, square brackets, backslashes
  (``sinks/neo4j.py:241-298``). Unicode normalization check for
  lookalike character attacks (``neo4j.py:277``). All identifiers
  are backtick-quoted. Parameterized queries used for data values.
- **Internal column filtering:** ``__ID__``, ``__SOURCE__``,
  ``__TARGET__`` automatically excluded from results via
  ``properties()``, ``keys()``, ``RETURN *``.

**Residual risks:**

- Neo4j connection credentials stored in config or env vars. See
  TB-2 for credential handling.
- Output file permissions inherit from process umask.

Security-Sensitive Operations
-----------------------------

The following operations require security review when modified:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Operation
     - Module
     - Review Trigger
   * - SQL query validation
     - ``ingestion/security.py``
     - Any change to blocklists or validation logic
   * - File path sanitization
     - ``ingestion/security.py``
     - Any change to path validation or sensitive dirs
   * - SSRF hostname checking
     - ``ingestion/security.py``
     - Any change to blocked IP ranges or hostnames
   * - URI scheme allowlist
     - ``ingestion/security.py``
     - Adding new URI schemes
   * - Env var substitution
     - ``ingestion/config.py``
     - Changes to blocked var list or substitution logic
   * - Module import blocking
     - ``ingestion/config.py``
     - Changes to blocked module list
   * - DuckDB identifier validation
     - ``backends/_helpers.py``
     - Changes to identifier pattern
   * - Neo4j identifier validation
     - ``sinks/neo4j.py``
     - Changes to Cypher identifier validation
   * - Audit logging
     - ``audit.py``
     - Changes to what is/isn't logged
   * - Health server endpoints
     - ``health_server.py``
     - Adding endpoints or changing response content
   * - Credential masking
     - ``cli/security.py``
     - Changes to URI credential detection
   * - Output path handling
     - ``output_writer.py``
     - Changes to file write paths

Audit & Logging Architecture
-----------------------------

**Security audit log** (``pycypher.audit``):

- Opt-in via ``PYCYPHER_AUDIT_LOG`` environment variable
- Structured JSON output for SIEM integration
- Never logs parameter values — only parameter names
- Query text truncated to 2048 characters
- Separate logger from application logs

**Security event log** (``pycypher.security``):

- Opt-in via ``PYCYPHER_SECURITY_LOG`` environment variable
- Logs: event type, timestamp, truncated input (256 chars), source
  function, detail
- Intended for incident response correlation

Data Flow Summary
-----------------

.. code-block:: text

   User Query ──► Parser ──► Semantic Validator ──► Query Planner
        │                                               │
        ▼                                               ▼
   YAML Config ──► safe_load ──► env subst ──► Pipeline Config
        │                                          │
        ▼                                          ▼
   File/DB URI ──► scheme check ──► SSRF check ──► path sanitize
        │                                          │
        ▼                                          ▼
   SQL fragment ──► comment strip ──► blocklist ──► DuckDB exec
        │                                          │
        ▼                                          ▼
   Results ──► internal col filter ──► output path validate ──► Write
                                           │
                                           ▼
                                    Neo4j ──► identifier validate
                                           ──► parameterized query

Coordination Requirements
-------------------------

Per Amendment Cycle 2, changes to the following require security
specialist coordination before merge:

1. **Data entry/exit points** — any new data source type, output
   format, or URI scheme.
2. **File I/O operations** — changes to file reading/writing paths
   or permission handling.
3. **Config parsing** — changes to YAML loading, env var substitution,
   or module import blocking.
4. **Credential handling** — changes to credential detection, masking,
   or storage.
5. **Input validation** — changes to SQL validation, path sanitization,
   SSRF checking, or identifier validation.
6. **Network operations** — changes to health server, SSRF protections,
   or any new network endpoints.

Security Testing
----------------

Existing security test coverage:

- ``tests/test_security_sql_injection_tdd.py`` — SQL injection TDD
- ``tests/test_security_sql_injection_proof.py`` — SQL injection proofs
- ``tests/test_backend_engine_security.py`` — backend engine security
- ``tests/test_data_source_security.py`` — data source URI validation
- ``tests/test_duckdb_reader_security.py`` — DuckDB reader security
- ``tests/test_helpers_serialization_security.py`` — serialization safety
- ``tests/test_neo4j_sink_security.py`` — Neo4j sink security
- ``tests/test_dask_distributed_security.py`` — distributed security
- ``tests/test_distributed_security_contracts.py`` — distributed contracts

Run all security tests:

.. code-block:: bash

   uv run pytest tests/ -k security -q

Document History
----------------

- **2026-04-06** — Initial threat model and trust boundary documentation
  established per Amendment Cycle 2 requirements.
