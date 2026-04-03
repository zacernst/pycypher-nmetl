# Changelog

All notable changes to PyCypher will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project is in **Alpha** and does not yet follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html) strictly. Breaking
changes may occur in any 0.0.x release.

## [Unreleased]

### Added

#### Performance & Indexing
- Graph-native index system (`GraphIndexManager`, `AdjacencyIndex`, `PropertyValueIndex`, `EntityLabelIndex`) — transforms pattern matching from O(E) full table scans to O(degree) neighbor lookups
- LeapfrogTriejoin algorithm for worst-case-optimal multi-way joins — O(N^{w/2}) vs O(N^{w-1}) for iterated binary joins (Veldhuizen 2014)
- Vectorized property store with O(k log N) bulk resolution — eliminates O(N log N) build cost per type on first property access
- DataFrame copy elimination via RangeIndex guards — removes 5 redundant copies per query in hot paths
- Cardinality estimator for query optimization
- JOIN algorithm selection optimizer (hash join vs. sort-merge vs. leapfrog)
- Learned join selectivity feedback loops with geometric mean correction factors for self-improving query plans

#### Caching & Resource Management
- Intelligent query result cache with SLRU hybrid eviction and per-type invalidation
- Query rate limiting framework (`QueryRateLimiter`) — thread-safe token bucket with per-session and per-caller limiting via `PYCYPHER_RATE_LIMIT_QPS` and `PYCYPHER_RATE_LIMIT_BURST`

#### Security & Observability
- Enterprise audit logging (`pycypher.audit`) — opt-in JSON-structured query audit log activated via `PYCYPHER_AUDIT_LOG`, records query_id, timing, status, and row counts (never logs parameter values)
- Unified backend integration protocol (`backend_engine`) for pluggable DataFrame engines with DuckDB, Polars, and Pandas backends

#### Developer Experience
- Keyword typo detection in `CypherSyntaxError` (50+ Cypher keywords)
- Improved error messages with contextual guidance and "Did you mean?" suggestions
- Sphinx documentation builds with zero warnings
- Diagnostic logging for silent query optimizer fallbacks
- Per-marker test timeouts for CI reliability
- `nmetl config --show-effective` CLI command for runtime configuration inspection

### Fixed
- SecurityError bypass in error policy configuration
- Shortest path algorithm result filtering
- Multi-match clause variable binding bug
- README typo corrections

### Changed
- Intersphinx configuration handles SSL certificate issues gracefully

## [0.0.19] - 2026-03-20

### Added
- PySpark backend integration via PR #11
- Property graph mutation support (CREATE, SET, DELETE)
- Multi-query composition for ETL pipelines with cross-query optimization
- DuckDB-based data ingestion from CSV, Parquet, JSON, and SQL sources
- YAML pipeline configuration with environment variable interpolation
- `nmetl` CLI tool with `run`, `validate`, and `list-queries` subcommands
- Interactive REPL with readline history and multi-line query support
- Query profiler for execution time analysis
- Query complexity analyzer
- Input validator with query size and nesting depth limits
- Comprehensive security module for URI and path validation
- Cloud storage support (S3, GCS, Azure Blob) via optional dependencies
- Delta Lake storage backend
- Dask integration for large-dataset processing
- Polars backend (optional)
- Neo4j sink for writing results to graph databases
- 7,600+ tests

### Changed
- Upgraded to Python 3.14+ requirement
- Migrated to `uv` for dependency management
- Adopted strict type checking with `ty check`

## [0.0.18] - 2026-02-21

### Added
- RETURN clause support
- WITH clause functionality
- Relationship attribute access in queries
- Relational algebra conversion for query execution
- Filter and Projection operations
- Disambiguated variable mapping in relationship patterns

### Fixed
- Relationship join correctness
- Variable mapping in multi-hop relationship patterns
- Long pattern path disambiguation

## [0.0.17] - 2026-01-20

### Added
- Algebra-based query execution engine (v2 rewrite)
- Sphinx documentation infrastructure with autodoc, napoleon, viewcode
- Google-style docstring coverage across core modules
- Auto-generated Cypher grammar from BNF specification
- AST parsing pipeline
- Filter operations on relational algebra

### Removed
- Redundant MultiJoin class

## [0.0.16] - 2025-12-07

### Added
- SAT solver integration (pycosat) for constraint-based pattern matching
- Algebra experiment for relational query representation

### Fixed
- Interface between Cypher parser and SAT solver
- Unit test stabilization

## [0.0.15] - 2025-11-09

### Added
- Docker Compose development environment
- FoundationDB container integration
- Neo4j and FastAPI container support
- Thread manager for concurrent operations

### Fixed
- Container compatibility with FoundationDB builds
- Dockerfile simplification for development

## [0.0.14] - 2025-04-06

### Added
- GitHub Pages documentation deployment
- DVC pipeline for data processing
- DataSource URI configuration with options extraction
- Trigger support for relationships

### Fixed
- Linting problems across codebase
- CLI path resolution

## [0.0.1] - 2025-03-09

### Added
- Initial monorepo structure with `pycypher` and `shared` packages
- Cypher query parser using Lark grammar
- Pattern matching engine for graph queries
- Pandas-based query execution backend
- MATCH clause with node and relationship patterns
- WHERE clause with property filters
- Basic variable binding and scope management

[Unreleased]: https://github.com/zacernst/pycypher/compare/main...HEAD
