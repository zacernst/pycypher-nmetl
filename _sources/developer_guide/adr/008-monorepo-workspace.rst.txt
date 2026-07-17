ADR-008: Monorepo Workspace Structure
======================================

:Status: Accepted
:Date: 2024
:Relates to: ``pyproject.toml`` (root workspace configuration)

Context
-------

The project contains three logical components: shared utilities, the Cypher
query engine, and an ETL pipeline for open data.  These share common logging,
configuration, and helper code.  They need to be developed, tested, and (in
some cases) released independently.

Decision
--------

Use a **uv workspace monorepo** with three packages:

.. code-block:: text

   packages/
   ├── shared/         # Common utilities, logging (no internal deps)
   ├── pycypher/       # Cypher parser and query engine (depends on shared)
   └── fastopendata/   # ETL pipeline (depends on shared + pycypher)

Cross-package dependencies are declared in ``tool.uv.sources`` as workspace
references.  Dependency groups (``dev-core``, ``dev``, ``dev-full``) provide
layered developer environments from lightweight to comprehensive.

Alternatives Considered
-----------------------

1. **Separate git repositories** with pip/uv dependencies — Adds friction to
   cross-package changes (separate PRs, version pinning, publish cycles).

2. **Single monolithic package** — Simpler packaging but forces all users to
   install ETL dependencies even if they only need the query engine.

3. **Git submodules** — Historically fragile; adds clone complexity and makes
   atomic cross-package commits difficult.

Consequences
------------

- Cross-package changes are atomic (single commit, single PR).
- Dependency order (``shared`` → ``pycypher`` → ``fastopendata``) prevents
  circular imports at the package level.
- ``uv sync`` must be run after ``pyproject.toml`` changes — CI enforces this
  with a ``lock-check`` job.
- Users can install ``pycypher`` alone without ``fastopendata`` dependencies.
- All tests live in a single ``tests/`` directory for unified test execution.
