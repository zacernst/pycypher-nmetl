"""Test-session bootstrap for the fastopendata package.

Some modules (e.g. ``processing/extract_osm_nodes.py``) resolve
``Config().data_path`` at *import* time, so ``DATA_DIR`` must be set before
pytest imports any test module. An autouse fixture runs too late for
that — ``pytest_configure`` is the earliest hook available.
"""

from __future__ import annotations

import os
import tempfile

import pytest


def pytest_configure(config: pytest.Config) -> None:
    os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="fastopendata-test-"))
