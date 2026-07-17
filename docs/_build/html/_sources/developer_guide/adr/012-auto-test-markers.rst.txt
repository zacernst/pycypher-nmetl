ADR-012: Automatic Test Markers and Isolation Fixtures
======================================================

:Status: Accepted
:Date: 2026-03
:Relates to: ``tests/conftest.py``

Context
-------

With ~300 test files across multiple directories, manually applying pytest
markers (``@pytest.mark.integration``, ``@pytest.mark.performance``, etc.)
was inconsistent — many tests lacked markers, making it difficult to run
targeted subsets (e.g., ``-m "not slow"``).  Additionally, two classes of
cross-test contamination caused flaky failures:

1. **SIGALRM leakage** — Tests using ``timeout_seconds`` set a POSIX alarm
   that could fire during a subsequent, unrelated test, causing spurious
   ``QueryTimeoutError``.

2. **Logger level contamination** — Tests using ``caplog.at_level(DEBUG)``
   left ``shared.logger.LOGGER`` in DEBUG mode, causing subsequent tests
   to emit unexpected log output.

Decision
--------

Add automatic marker inference and isolation fixtures to ``conftest.py``:

**Auto-markers** (applied when no explicit marker is present):

- ``tests/benchmarks/`` → ``performance``
- ``tests/large_dataset/`` → ``integration`` + ``slow``
- ``tests/load_testing/`` → ``performance`` + ``slow``
- ``tests/property_based/`` → ``unit``
- Filenames containing ``_e2e_`` or ``_end_to_end`` → ``integration``
- Filenames containing ``_performance_`` → ``performance``
- Filenames containing ``_security_`` → ``security``

**Per-marker timeouts:**

- ``integration``: 120s (vs 30s default)
- ``slow``: 300s (vs 30s default)

**Isolation fixtures** (auto-use, session-wide):

- ``_clear_pending_sigalrm()`` — Resets SIGALRM after each test.
- ``_restore_shared_logger_level()`` — Saves and restores logger level.

Alternatives Considered
-----------------------

1. **Manual markers everywhere** — Reliable but requires discipline; 300+
   files means markers would drift out of date.

2. **pytest plugin with custom collection hook** — More powerful but adds
   external dependency or maintenance burden for a custom plugin.

3. **Directory-based conftest.py files** — Each subdirectory gets its own
   ``conftest.py`` that applies markers.  Works but scatters configuration
   across many files.

Consequences
------------

- Running ``pytest -m "not slow"`` now reliably excludes all slow tests,
  even those that were never manually marked.
- Explicit ``@pytest.mark.X`` always takes precedence — auto-markers never
  override developer intent.
- SIGALRM and logger contamination eliminated, reducing flaky test rate.
- ``perf_threshold()`` helper scales timing assertions for CI environments
  (configurable via ``PYCYPHER_PERF_MULTIPLIER``).
- New test files automatically get appropriate markers based on their
  location — no action needed from the developer.
