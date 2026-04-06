"""Performance test helpers — CI-aware timing thresholds.

Usage in test files::

    from _perf_helpers import perf_threshold

    assert elapsed < perf_threshold(0.5), f"took {elapsed:.2f}s"
"""

from __future__ import annotations

import os

_CI_PERF_MULTIPLIER = float(os.environ.get("PYCYPHER_PERF_MULTIPLIER", "3.0"))
_IN_CI = os.environ.get("CI", "").lower() in ("true", "1", "yes")
# pytest-xdist sets PYTEST_XDIST_WORKER on worker processes
_IN_XDIST = "PYTEST_XDIST_WORKER" in os.environ
_XDIST_PERF_MULTIPLIER = float(os.environ.get("PYCYPHER_XDIST_PERF_MULTIPLIER", "2.0"))


def perf_threshold(seconds: float) -> float:
    """Return *seconds* scaled up when running in CI or under heavy load.

    On a local machine this returns ``seconds`` unchanged.  In CI it returns
    ``seconds * _CI_PERF_MULTIPLIER`` (default 3x) to absorb variance from
    shared runners, parallel test sessions, and Python version differences.

    When running under pytest-xdist (parallel workers), applies a 2x
    multiplier to account for CPU contention between workers.

    Override the multiplier via ``PYCYPHER_PERF_MULTIPLIER=5.0`` if needed.
    """
    if _IN_CI:
        return seconds * _CI_PERF_MULTIPLIER
    if _IN_XDIST:
        return seconds * _XDIST_PERF_MULTIPLIER
    return seconds
