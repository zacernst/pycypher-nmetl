# Project Requirements

**Project**: pycypher-nmetl
**Version**: 0.0.19 (Alpha)
**Created**: 2026-04-09
**Framework**: Unified Multi-Agent Framework v2.0

This document systematically tracks all capabilities, features, and correctness conditions for the pycypher-nmetl monorepo. Requirements are continuously validated and amended through the Meta-Improvement Protocol.

---

## Core Functionality Requirements

### Cypher Query Processing
- [REQ-F001] System must parse complete Cypher query syntax via Lark grammar (MATCH, WHERE, WITH, RETURN, CREATE, SET, DELETE clauses)
- [REQ-F002] AST models must maintain type safety through Pydantic validation with zero serialization errors
- [REQ-F003] Query execution must support all standard Cypher clause combinations without semantic errors
- [REQ-F004] Parser must handle nested subqueries up to PYCYPHER_MAX_QUERY_NESTING_DEPTH (default: 200 levels)
- [REQ-F005] Query string size must not exceed PYCYPHER_MAX_QUERY_SIZE_BYTES (default: 1 MiB)

### Relational Algebra Translation
- [REQ-F006] Cypher queries must translate to optimized relational algebra operations with measurable performance
- [REQ-F007] Join operations must preserve ID-only column strategy for memory efficiency
- [REQ-F008] Type-based column namespacing (EntityType__PropertyName) must prevent attribute collisions
- [REQ-F009] Shadow write transactions must isolate mutations until atomic commit completion
- [REQ-F010] Unbounded path queries ([*]) must respect PYCYPHER_MAX_UNBOUNDED_PATH_HOPS (default: 20)

### Multi-Backend Execution
- [REQ-F011] Pandas backend must serve as default with full feature support for all Cypher operations
- [REQ-F012] DuckDB backend must provide OLAP optimization with lazy evaluation and columnar performance
- [REQ-F013] Polars backend integration must maintain functional parity when polars extra installed
- [REQ-F014] Backend switching must be transparent to user queries with consistent result semantics
- [REQ-F015] Cross-join operations must not exceed PYCYPHER_MAX_CROSS_JOIN_ROWS (default: 1M)

### Query Optimization
- [REQ-F016] LeapfrogTriejoin algorithm must provide O(N^{w/2}) complexity for 3+ way joins
- [REQ-F017] Cardinality estimation must use adaptive EMA feedback with historical learning
- [REQ-F018] Query fingerprinting must enable plan reuse across structurally similar queries
- [REQ-F019] ML learning subsystem must track predicate selectivity per (entity_type, property, operator)
- [REQ-F020] Query plan cache must implement LRU with TTL and mutation-based invalidation

### Data Ingestion & Sources
- [REQ-F021] CSV, Parquet, JSON file ingestion must work via Arrow/DuckDB with schema inference
- [REQ-F022] Cloud storage (S3, GCS) must be accessible when cloud extras installed
- [REQ-F023] Streaming data processing must handle large datasets without memory exhaustion
- [REQ-F024] Neo4j integration must function when neo4j extra installed with driver compatibility
- [REQ-F025] FastOpenData package must provide Census, TIGER, OSM data extraction capabilities

### ETL Pipeline Framework (nmetl)
- [REQ-F026] YAML pipeline configuration must support multi-stage ETL workflows
- [REQ-F027] CLI interface must provide REPL mode with interactive query execution
- [REQ-F028] Pipeline stages must be composable with dependency resolution
- [REQ-F029] Data validation must occur at stage boundaries with configurable rules
- [REQ-F030] Error handling must provide detailed context for pipeline failures

### Terminal UI Run-Pipeline (pycypher-tui)
- [REQ-TUI-RUN-001] The TUI must surface a Run-Pipeline action that executes the loaded YAML pipeline configuration end-to-end
- [REQ-TUI-RUN-002] Pipeline Overview screen must display per-section status (sources, entities, relationships, queries, outputs) with VIM-style drill-down navigation
- [REQ-TUI-RUN-003] Pipeline Testing screen must allow ad-hoc execution and validation of individual pipeline stages without running the full pipeline
- [REQ-TUI-RUN-004] Pipeline run state, progress, and errors must be reported back to the UI without blocking the Textual event loop
- [REQ-TUI-RUN-005] Run-Pipeline integration must be covered by tests in `packages/pycypher-tui/tests/test_pipeline_run_integration.py` and corresponding screen-level tests

---

## Performance Requirements

### Query Execution Performance
- [REQ-P001] Query timeout enforcement must respect PYCYPHER_QUERY_TIMEOUT_S when configured
- [REQ-P002] Result cache must limit memory usage to PYCYPHER_RESULT_CACHE_MAX_MB (default: 100 MB)
- [REQ-P003] AST parsing cache must maintain PYCYPHER_AST_CACHE_MAX entries (default: 1024) with LRU eviction
- [REQ-P004] Rate limiting must enforce PYCYPHER_RATE_LIMIT_QPS when enabled (0=disabled)
- [REQ-P005] Collection operations must not exceed PYCYPHER_MAX_COLLECTION_SIZE (default: 1M items)

### Memory Management
- [REQ-P006] Lazy evaluation must defer DataFrame materialization until result consumption
- [REQ-P007] Streaming operations must process data in chunks to maintain constant memory usage
- [REQ-P008] Query result cache must implement TTL-based expiration via PYCYPHER_RESULT_CACHE_TTL_S
- [REQ-P009] Large dataset operations must not cause memory exhaustion in production environments
- [REQ-P010] Garbage collection must properly release DataFrame references after query completion

### Concurrency & Threading
- [REQ-P011] Thread-safe operations must use fine-grained locking with <1ms lock acquisition overhead
- [REQ-P012] Concurrent query execution must not cause data corruption or race conditions
- [REQ-P013] Shadow transaction isolation must prevent read-write conflicts during mutations
- [REQ-P014] Cache operations must be thread-safe with consistent read/write semantics
- [REQ-P015] Telemetry collection must not impact query performance by >1%

---

## Quality Requirements

### Test Coverage & Validation
- [REQ-Q001] Test coverage must maintain ≥80% for all core modules as enforced by CI
- [REQ-Q002] Zero regression tolerance: no existing functionality may break from new changes
- [REQ-Q003] Test suite must execute with 100% pass rate before any release
- [REQ-Q004] Property-based testing must validate query semantics with Hypothesis
- [REQ-Q005] Performance tests must establish measurable baselines for optimization claims

### Code Quality Standards
- [REQ-Q006] Type checking must pass cleanly with ty across all source files
- [REQ-Q007] Ruff linting must show zero violations for format and style rules
- [REQ-Q008] Bandit security analysis must identify no high-severity vulnerabilities
- [REQ-Q009] pip-audit must report no known security vulnerabilities in dependencies
- [REQ-Q010] Docstring coverage must meet D* rule requirements via ruff

### Testing Framework Standards
- [REQ-Q011] @pytest.mark.slow tests must complete within reasonable CI time budgets
- [REQ-Q012] @pytest.mark.integration tests must validate multi-component workflows
- [REQ-Q013] Backend equivalence tests must verify consistent results across Pandas/DuckDB/Polars
- [REQ-Q014] Scalability tests must demonstrate performance characteristics under load
- [REQ-Q015] Security tests must validate input sanitization and injection prevention
- [REQ-Q016] `pytest-asyncio` must be installed and configured (declared in `packages/pycypher-tui/pyproject.toml` dev deps and synced via `uv sync --all-extras`) so async TUI tests can be collected and executed

---

## Configuration Requirements

### Environment Variable System
- [REQ-C001] All runtime behavior must be configurable via environment variables read at import time
- [REQ-C002] Configuration preset functions (production, development, high_performance) must apply consistent defaults
- [REQ-C003] Invalid configuration values must raise clear validation errors at startup
- [REQ-C004] Configuration changes must not require application restart when safely mutable
- [REQ-C005] Default values must provide secure, production-appropriate behavior

### Security Configuration
- [REQ-C006] PYCYPHER_AUDIT_LOG must enable comprehensive query audit logging when enabled
- [REQ-C007] Query complexity scoring must prevent resource exhaustion when PYCYPHER_MAX_COMPLEXITY_SCORE set
- [REQ-C008] File path validation must prevent directory traversal in data ingestion
- [REQ-C009] URL validation must prevent SSRF attacks in cloud storage access
- [REQ-C010] Parameter masking must protect sensitive data in audit logs

---

## Integration Requirements

### Backend Engine Integration
- [REQ-I001] Backend dispatch must transparently route operations to appropriate engine
- [REQ-I002] DuckDB backend must maintain lazy evaluation for OLAP query patterns
- [REQ-I003] Pandas backend must handle all query types as primary reference implementation
- [REQ-I004] Neo4j optional backend must connect via official neo4j driver when available
- [REQ-I005] Backend-specific optimizations must not break cross-backend result consistency

### Data Source Integration
- [REQ-I006] Arrow format compatibility must enable zero-copy operations where possible
- [REQ-I007] Cloud storage integration must support authentication via standard credential providers
- [REQ-I008] Streaming data sources must handle backpressure and connection failures gracefully
- [REQ-I009] Schema evolution must adapt to changing data source structures
- [REQ-I010] Multi-source data fusion must resolve schema conflicts deterministically

### External System Integration
- [REQ-I011] OpenTelemetry tracing must export spans to configured OTEL collectors
- [REQ-I016] FastOpenData `/site` must serve a static landing page mounted by `app.mount("/site", ...)` in `packages/fastopendata/src/fastopendata/api.py:363-365`. As of 2026-05-27 the mount serves a minimal placeholder (`packages/fastopendata/site/index.html`) committed to the repository; richer content (dataset catalog, usage examples) is planned for a later release. The mount remains conditional on `_SITE_DIR.exists()` and must continue to degrade gracefully if assets are absent.
- [REQ-I012] Prometheus metrics must expose standard query performance indicators
- [REQ-I013] Structured logging must integrate with centralized log aggregation systems
- [REQ-I014] Health check endpoints must report system operational status
- [REQ-I015] Graceful shutdown must complete in-flight queries before termination

---

## API Stability Requirements

### Public API Surface (Stable)
- [REQ-A001] Star class interface must remain backward compatible across 0.0.x releases
- [REQ-A002] ContextBuilder API must maintain consistent data loading patterns
- [REQ-A003] Exception hierarchy must preserve error type inheritance and messages
- [REQ-A004] validate_query() function signature must remain stable for pre-execution validation
- [REQ-A005] Public constants and enums must not change values without major version increment

### Provisional API Surface (May Change)
- [REQ-A006] Pipeline and Stage classes may evolve until 0.1.0 release with deprecation warnings
- [REQ-A007] ResultCache internals may change while maintaining functional behavior
- [REQ-A008] ML learning APIs may be refactored based on optimization effectiveness evidence
- [REQ-A009] Backend engine registration may change to support additional backends
- [REQ-A010] Configuration preset implementations may evolve based on operational feedback

### API Evolution Protocol
- [REQ-A011] Breaking changes must be tracked in api_baseline.json with automated detection
- [REQ-A012] Deprecation warnings must appear at least one minor version before removal
- [REQ-A013] Migration guides must provide clear upgrade paths for breaking changes
- [REQ-A014] Semantic versioning must reflect API stability guarantees accurately
- [REQ-A015] Release notes must document all API changes with examples

---

## Dependency Requirements

### Core Dependencies (Required)
- [REQ-D001] Pydantic ≥2.9.2 must provide type-safe AST model validation
- [REQ-D002] Lark ≥1.3.1 must handle complete Cypher grammar parsing
- [REQ-D003] Pandas ≥3.0.0 must serve as primary DataFrame backend
- [REQ-D004] PyArrow ≥18.0.0 must enable high-performance data interchange
- [REQ-D005] DuckDB ≥1.0.0 must provide OLAP query acceleration

### Optional Dependencies (Extras)
- [REQ-D006] [neo4j] extra must provide Neo4j driver 5.0.0-7.0.0 compatibility
- [REQ-D007] [large-dataset] extra must enable Dask distributed computing
- [REQ-D008] [cloud] extra must support S3/GCS via s3fs/gcsfs libraries
- [REQ-D009] [polars] extra must maintain Polars 1.x compatibility
- [REQ-D010] [all] extra must install all optional features without conflicts

### Build System Requirements
- [REQ-D011] uv package manager must provide fast, reliable dependency resolution
- [REQ-D012] Workspace lockfile must ensure reproducible builds across environments
- [REQ-D013] Python 3.12+ must be supported with 3.14 optimization target
- [REQ-D014] Hatchling build backend must support VCS versioning
- [REQ-D015] Development dependencies must not be included in distribution builds

---

## Documentation Requirements

### User Documentation
- [REQ-DOC001] Sphinx documentation must provide comprehensive API reference
- [REQ-DOC002] 12+ runnable examples must demonstrate common usage patterns
- [REQ-DOC003] Installation guide must cover all optional dependency configurations
- [REQ-DOC004] Performance tuning guide must document configuration best practices
- [REQ-DOC005] Migration guides must assist users upgrading between versions

### Developer Documentation
- [REQ-DOC006] Architecture decision records must document major design choices
- [REQ-DOC007] Contributing guide must explain development workflow and standards
- [REQ-DOC008] Testing guide must cover test categories and execution procedures
- [REQ-DOC009] Release process documentation must ensure consistent releases
- [REQ-DOC010] Troubleshooting guide must address common development issues

### Code Documentation
- [REQ-DOC011] Public APIs must have comprehensive docstrings with examples
- [REQ-DOC012] Complex algorithms must include implementation comments and references
- [REQ-DOC013] Configuration options must document purpose, defaults, and impact
- [REQ-DOC014] Error messages must provide actionable guidance for resolution
- [REQ-DOC015] Changelog must document all user-visible changes between releases

---

## Operational Requirements

### Production Deployment
- [REQ-O001] System must operate reliably in production with configured resource limits
- [REQ-O002] Graceful degradation must occur when optional dependencies unavailable
- [REQ-O003] Error recovery must handle transient failures without data corruption
- [REQ-O004] Resource monitoring must track memory, CPU, and I/O utilization
- [REQ-O005] Health checks must detect system degradation before user impact

### Development Environment
- [REQ-O006] Local development must work with minimal configuration requirements
- [REQ-O007] Test execution must complete within reasonable developer workflow timeframes
- [REQ-O008] Hot reload must enable rapid iteration during development
- [REQ-O009] Debugging support must provide clear stack traces and variable inspection
- [REQ-O010] Profile-guided optimization must enable performance analysis and tuning

### Security Operations
- [REQ-O011] Input validation must prevent code injection via Cypher query parsing
- [REQ-O012] Audit logging must comply with organizational security requirements when enabled
- [REQ-O013] Dependency scanning must identify and report security vulnerabilities
- [REQ-O014] Secret management must prevent credential exposure in logs or errors
- [REQ-O015] Access controls must restrict administrative operations appropriately

---

## Amendment Protocol

This REQUIREMENTS.md file implements the Unified Multi-Agent Framework's systematic requirement tracking protocol. When changes are made to the system:

1. **Consult this file** before considering any change complete
2. **Validate all requirements** are still met by the modified system
3. **Identify discrepancies** where requirements are no longer satisfied
4. **Escalate unmet requirements** to P1 or P0 priority immediately
5. **Amend requirements** when new capabilities or constraints are discovered

### Amendment Categories
- **REQ-INT**: Integration requirements (end-to-end functionality validation)
- **REQ-E2E**: End-to-end workflow requirements (complete user journey testing)
- **REQ-PROD**: Production readiness requirements (real-world usage scenarios)
- **REQ-SIL**: Silent failure detection requirements (functionality verification beyond unit tests)

### Amendment History
- **2026-04-09**: Initial requirements capture based on v0.0.19 analysis
- **2026-05-27**: Added [REQ-TUI-RUN-001..005] for the Terminal UI Run-Pipeline feature landing on `feat/tui-run-pipeline`. Rationale: the TUI now drives end-to-end pipeline execution (Pipeline Overview + Pipeline Testing screens, integration tests under `tests/test_pipeline_run_integration.py`); requirements ensure UI/event-loop isolation, status surfacing, and test coverage are codified.
- **2026-05-27**: Added [REQ-Q016] mandating `pytest-asyncio` installation. Rationale: the dependency was declared in `packages/pycypher-tui/pyproject.toml` dev deps but not present in `.venv`, causing 387 async TUI tests to fail collection. Encoding the install/sync requirement prevents recurrence.
- **2026-05-27**: Added [REQ-I016] codifying FastOpenData `/site` mount. Rationale: the conditional mount previously returned 404 because the `site/` asset directory was not committed; user decision (T10) was to ship a minimal placeholder `index.html`. The placeholder is now committed at `packages/fastopendata/site/index.html` and `/site` is functional. Richer content is planned for a later release.
- **2026-05-27**: Recorded backend-dispatch wiring as a near-term fix landing on `feat/tui-run-pipeline`. Rationale: T9 investigation confirmed `Star._analyze_and_plan` did not dispatch to the registered backends during `execute_query`, leaving `context.backend_name` as metadata only and the TUI's backend-engine selector with no runtime effect. The fix re-establishes the integration surface required by [REQ-I001] and the Backend Engine Integration cluster.

---

*This document is continuously evolved through the Meta-Improvement Protocol to ensure requirements remain accurate, complete, and actionable for multi-agent development teams.*