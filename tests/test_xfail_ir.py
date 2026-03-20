"""Known-broken patterns that the IR refactor must fix.

Every test here is currently expected to fail.  As the refactor progresses,
failing tests will be removed from this file and promoted to
``test_golden_ir.py`` as confirmed-passing golden tests.

When a phase completes a pattern, the workflow is:
1. Remove the xfail marker from the test here.
2. Run it — confirm it passes.
3. Move the test (verbatim) to test_golden_ir.py.
4. Delete the now-duplicate entry from this file.

DO NOT change a test's logic to make it pass — only remove the xfail marker
when the underlying implementation actually supports the pattern.

Run with:
    uv run pytest tests/test_xfail_ir.py -v
"""

# All patterns from Phases 1–7 have been promoted to test_golden_ir.py.
# Phase 8+ xfail tests will be added here as new unsupported patterns are
# identified.
