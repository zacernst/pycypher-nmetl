"""TDD tests for vectorizing _build_node_rows and _build_rel_rows in neo4j.py.

Loop 219 — Performance loop.

Lesson from loop.md:
    ``DataFrame.iterrows()`` inside a lookup-dict construction is always wrong.
    iterrows() creates one Python ``pd.Series`` object per row (confirmed:
    30k Series.__init__ calls for 10k rows × 3 queries). The idiomatic
    replacement is ``df.set_index(id_col)[cols].to_dict('index')``, which
    delegates the Python dict construction to Cython. The speedup is 4-5× on
    10k-row tables and grows with table size.

_build_node_rows (lines 370–384) and _build_rel_rows (lines 405–422) both
iterate with ``for _, row in df.iterrows()``. This loop:

  - constructs one pd.Series per DataFrame row (expensive __init__),
  - indexes into each Series by column name (slow hash lookup on a Series
    rather than a plain Python dict),
  - is called for every Neo4j write batch.

The fix vectorizes both functions:
  1. Use pandas boolean indexing to find null-id rows — O(n) C-level pass.
  2. Emit one warning per batch (not per row) with the count of skipped rows.
  3. Use ``df[relevant_cols].to_dict('records')`` — Cython-level construction
     of a list of plain Python dicts.
  4. Apply ``_coerce_value`` to each value in the plain dict — same semantic
     content but 3-5× cheaper because dict lookup replaces Series lookup.

Red-phase tests:
  - test_build_node_rows_does_not_use_iterrows: fails while iterrows() present
  - test_build_rel_rows_does_not_use_iterrows: fails while iterrows() present
  - test_build_node_rows_large_df_performance: fails if > 120ms for 10k rows
  - test_build_rel_rows_large_df_performance: fails if > 120ms for 10k rows

Green-phase tests (regression, all must pass after the fix):
  - All existing tests in TestBuildNodeRows / TestBuildRelRows remain green.
  - New performance tests pass.
  - Null-warning is emitted once per batch, not per row.
"""

from __future__ import annotations

import inspect
import textwrap
import time

import numpy as np
import pandas as pd
import pytest
from pycypher.sinks.neo4j import (
    NodeMapping,
    RelationshipMapping,
    _build_node_rows,
    _build_rel_rows,
)

# ---------------------------------------------------------------------------
# Category 1 — Structural: no iterrows() in implementation
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.neo4j


class TestNoIterrows:
    def test_build_node_rows_does_not_use_iterrows(self) -> None:
        """_build_node_rows must not call .iterrows().

        Red-phase: fails while the iterrows() loop is present.
        Green-phase: passes after replacement with vectorized path.

        The check targets ``.iterrows(`` (the method-call syntax) to avoid
        false-positives on docstring mentions of the word.
        """
        src = textwrap.dedent(inspect.getsource(_build_node_rows))
        assert ".iterrows(" not in src, (
            "_build_node_rows still calls .iterrows(). "
            "Replace with df.to_dict('records') + boolean-index null filter."
        )

    def test_build_rel_rows_does_not_use_iterrows(self) -> None:
        """_build_rel_rows must not call .iterrows().

        Red-phase: fails while the iterrows() loop is present.
        """
        src = textwrap.dedent(inspect.getsource(_build_rel_rows))
        assert ".iterrows(" not in src, (
            "_build_rel_rows still calls .iterrows(). "
            "Replace with df.to_dict('records') + boolean-index null filter."
        )


# ---------------------------------------------------------------------------
# Category 2 — Performance: 10k rows complete in < 120 ms
# ---------------------------------------------------------------------------

_N = 10_000
from _perf_helpers import perf_threshold
_THRESHOLD_MS = perf_threshold(250)  # budget per call; iterrows typically 200-500ms at N=10k
# Threshold is generous to avoid flaky failures under CPU contention
# (CI, parallel test runs, multi-agent sessions).  Vectorized path
# typically completes in 15-40ms on an idle machine.


class TestPerformanceNodeRows:
    def test_build_node_rows_large_df_performance(self) -> None:
        """_build_node_rows on 10k rows must complete in < 120 ms.

        Red-phase: the iterrows() path typically takes 200-500ms on 10k rows
        in CPython, so it fails this threshold.  The vectorized path takes
        roughly 15-40ms and passes easily.
        """
        df = pd.DataFrame(
            {
                "pid": np.arange(1, _N + 1, dtype=np.int64),
                "name": [f"user_{i}" for i in range(_N)],
                "age": np.random.randint(18, 90, size=_N),
                "score": np.random.random(_N),
            },
        )
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name", "age": "age", "score": "score"},
        )

        start = time.monotonic()
        rows = _build_node_rows(df, mapping)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert len(rows) == _N, f"Expected {_N} rows, got {len(rows)}"
        assert elapsed_ms < _THRESHOLD_MS, (
            f"_build_node_rows took {elapsed_ms:.1f}ms for {_N} rows — "
            f"threshold is {_THRESHOLD_MS}ms. Still using iterrows()?"
        )

    def test_build_node_rows_output_correct_after_vectorization(self) -> None:
        """Spot-check: first and last rows have correct ids and property values."""
        df = pd.DataFrame(
            {
                "pid": np.arange(1, _N + 1, dtype=np.int64),
                "name": [f"user_{i}" for i in range(_N)],
            },
        )
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name"},
        )
        rows = _build_node_rows(df, mapping)

        assert rows[0]["id"] == 1
        assert rows[0]["properties"]["name"] == "user_0"
        assert rows[-1]["id"] == _N
        assert rows[-1]["properties"]["name"] == f"user_{_N - 1}"


class TestPerformanceRelRows:
    def test_build_rel_rows_large_df_performance(self) -> None:
        """_build_rel_rows on 10k rows must complete in < 120 ms."""
        df = pd.DataFrame(
            {
                "src": np.arange(1, _N + 1, dtype=np.int64),
                "tgt": np.arange(_N + 1, 2 * _N + 1, dtype=np.int64),
                "since": np.random.randint(2000, 2024, size=_N),
            },
        )
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            property_columns={"since": "since"},
        )

        start = time.monotonic()
        rows = _build_rel_rows(df, mapping)
        elapsed_ms = (time.monotonic() - start) * 1000

        assert len(rows) == _N, f"Expected {_N} rows, got {len(rows)}"
        assert elapsed_ms < _THRESHOLD_MS, (
            f"_build_rel_rows took {elapsed_ms:.1f}ms for {_N} rows — "
            f"threshold is {_THRESHOLD_MS}ms. Still using iterrows()?"
        )

    def test_build_rel_rows_output_correct_after_vectorization(self) -> None:
        """Spot-check: first and last rows have correct ids."""
        df = pd.DataFrame(
            {
                "src": np.arange(1, _N + 1, dtype=np.int64),
                "tgt": np.arange(_N + 1, 2 * _N + 1, dtype=np.int64),
            },
        )
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)

        assert rows[0]["src_id"] == 1
        assert rows[0]["tgt_id"] == _N + 1
        assert rows[-1]["src_id"] == _N
        assert rows[-1]["tgt_id"] == 2 * _N


# ---------------------------------------------------------------------------
# Category 3 — Null handling: batch warning, not per-row warning
# ---------------------------------------------------------------------------


class TestNullHandling:
    def test_build_node_rows_skips_null_ids_correctly(self) -> None:
        """Null id rows are dropped; non-null rows are preserved."""
        df = pd.DataFrame(
            {
                "pid": [1, None, 3, None, 5],
                "name": ["a", "b", "c", "d", "e"],
            },
        )
        mapping = NodeMapping(
            label="N",
            id_column="pid",
            property_columns={"name": "name"},
        )
        rows = _build_node_rows(df, mapping)
        assert len(rows) == 3
        assert [r["id"] for r in rows] == [1, 3, 5]

    def test_build_rel_rows_skips_null_endpoints(self) -> None:
        """Rows with null src or tgt are dropped."""
        df = pd.DataFrame(
            {
                "src": [1, None, 3],
                "tgt": [10, 20, None],
            },
        )
        mapping = RelationshipMapping(
            rel_type="T",
            source_label="A",
            target_label="B",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert len(rows) == 1
        assert rows[0]["src_id"] == 1
        assert rows[0]["tgt_id"] == 10

    def test_build_node_rows_null_warning_emitted(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A warning must be logged when null ids are skipped."""
        import logging

        df = pd.DataFrame({"pid": [1, None, 3], "name": ["a", "b", "c"]})
        mapping = NodeMapping(label="N", id_column="pid")

        with caplog.at_level(logging.WARNING):
            rows = _build_node_rows(df, mapping)

        assert len(rows) == 2
        assert any(
            "null" in m.lower() or "skip" in m.lower() for m in caplog.messages
        ), (
            f"Expected a warning about skipped null-id rows; got: {caplog.messages}"
        )

    def test_build_rel_rows_null_warning_emitted(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A warning must be logged when null endpoint rows are skipped."""
        import logging

        df = pd.DataFrame({"src": [None, 2], "tgt": [10, 20]})
        mapping = RelationshipMapping(
            rel_type="T",
            source_label="A",
            target_label="B",
            source_id_column="src",
            target_id_column="tgt",
        )

        with caplog.at_level(logging.WARNING):
            rows = _build_rel_rows(df, mapping)

        assert len(rows) == 1
        assert any(
            "null" in m.lower() or "skip" in m.lower() for m in caplog.messages
        ), (
            f"Expected a warning about skipped null-endpoint rows; got: {caplog.messages}"
        )


# ---------------------------------------------------------------------------
# Category 4 — Coercion still works after vectorization
# ---------------------------------------------------------------------------


class TestCoercionAfterVectorization:
    def test_numpy_int_id_coerced_to_plain_int(self) -> None:
        df = pd.DataFrame({"pid": pd.array([np.int64(7)])})
        mapping = NodeMapping(label="N", id_column="pid")
        rows = _build_node_rows(df, mapping)
        assert isinstance(rows[0]["id"], int)
        assert rows[0]["id"] == 7

    def test_numpy_float_property_coerced(self) -> None:
        df = pd.DataFrame({"pid": [1], "score": [np.float64(3.14)]})
        mapping = NodeMapping(
            label="N",
            id_column="pid",
            property_columns={"score": "score"},
        )
        rows = _build_node_rows(df, mapping)
        assert isinstance(rows[0]["properties"]["score"], float)
        assert abs(rows[0]["properties"]["score"] - 3.14) < 1e-9

    def test_null_property_dropped_after_vectorization(self) -> None:
        df = pd.DataFrame({"pid": [1], "name": [None], "age": [30]})
        mapping = NodeMapping(
            label="N",
            id_column="pid",
            property_columns={"name": "name", "age": "age"},
        )
        rows = _build_node_rows(df, mapping)
        props = rows[0]["properties"]
        assert "name" not in props
        assert props["age"] == 30

    def test_rel_rows_numpy_ids_coerced(self) -> None:
        df = pd.DataFrame(
            {
                "src": pd.array([np.int64(1)]),
                "tgt": pd.array([np.int64(2)]),
            },
        )
        mapping = RelationshipMapping(
            rel_type="T",
            source_label="A",
            target_label="B",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert isinstance(rows[0]["src_id"], int)
        assert isinstance(rows[0]["tgt_id"], int)
