"""Performance benchmarks for optimized components.

Validates that optimizations produce correct results and measures
throughput for key hot paths.
"""

from __future__ import annotations

import random
import time

import pytest
from fastopendata.streaming.core import DeduplicationStore, StreamRecord
from fastopendata.streaming.joins import StreamTableJoin, TableSnapshot
from fastopendata.streaming.windows import (
    SessionWindow,
    TumblingWindow,
    WindowManager,
)

# ---------------------------------------------------------------------------
# StreamRecord.content_hash — MD5 vs SHA256 correctness
# ---------------------------------------------------------------------------


class TestContentHash:
    """Verify content_hash produces deterministic, distinct hashes."""

    def test_deterministic(self) -> None:
        record = StreamRecord(key="k1", value={"a": 1, "b": 2}, event_time=1.0)
        h1 = record.content_hash()
        h2 = record.content_hash()
        assert h1 == h2

    def test_different_records_different_hashes(self) -> None:
        r1 = StreamRecord(key="k1", value={"a": 1}, event_time=1.0)
        r2 = StreamRecord(key="k2", value={"a": 1}, event_time=1.0)
        r3 = StreamRecord(key="k1", value={"a": 2}, event_time=1.0)
        hashes = {r1.content_hash(), r2.content_hash(), r3.content_hash()}
        assert len(hashes) == 3

    def test_hash_throughput(self) -> None:
        """Measure hashing throughput for batch dedup scenarios."""
        records = [
            StreamRecord(
                key=f"k{i}",
                value={"idx": i, "data": "x" * 100},
                event_time=float(i),
            )
            for i in range(10_000)
        ]
        start = time.perf_counter()
        for r in records:
            r.content_hash()
        elapsed_ms = (time.perf_counter() - start) * 1000
        # 10k hashes should complete in well under 500ms
        assert elapsed_ms < 500, f"Too slow: {elapsed_ms:.1f}ms for 10k hashes"


# ---------------------------------------------------------------------------
# DeduplicationStore throughput
# ---------------------------------------------------------------------------


class TestDeduplicationPerformance:
    """Verify dedup store handles high-throughput insert/check patterns."""

    def test_throughput(self) -> None:
        store = DeduplicationStore(capacity=50_000)
        ids = [f"record_{i}" for i in range(100_000)]

        start = time.perf_counter()
        for record_id in ids:
            store.check_and_add(record_id)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # 100k checks should be fast
        assert elapsed_ms < 500, (
            f"Too slow: {elapsed_ms:.1f}ms for 100k dedup checks"
        )

    def test_duplicate_detection_accuracy(self) -> None:
        store = DeduplicationStore(capacity=1000)
        # Insert 500 unique
        for i in range(500):
            assert store.check_and_add(f"id_{i}")
        # Re-insert same 500
        for i in range(500):
            assert not store.check_and_add(f"id_{i}")
        assert store.duplicates_dropped == 500


# ---------------------------------------------------------------------------
# WindowManager.fire — optimized single-pass
# ---------------------------------------------------------------------------


class TestWindowManagerFirePerformance:
    """Verify optimized fire() produces correct results under load."""

    def test_fire_correctness(self) -> None:
        mgr = WindowManager(TumblingWindow(size=10.0))
        # Add records across many windows
        for i in range(100):
            r = StreamRecord(key="k1", value={"i": i}, event_time=float(i))
            mgr.add(r)
        # Fire windows up to t=50
        fired = mgr.fire(50.0)
        # Windows [0,10), [10,20), [20,30), [30,40), [40,50) should fire
        assert len(fired) == 5
        assert mgr.total_fired == 5

    def test_fire_high_cardinality_keys(self) -> None:
        """Many distinct keys with many windows each."""
        mgr = WindowManager(TumblingWindow(size=1.0))
        n_keys = 100
        n_records_per_key = 100
        for k in range(n_keys):
            for t in range(n_records_per_key):
                r = StreamRecord(
                    key=f"key_{k}",
                    value={"t": t},
                    event_time=float(t),
                )
                mgr.add(r)

        start = time.perf_counter()
        fired = mgr.fire(50.0)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # 100 keys * 50 windows = 5000 fired windows
        assert len(fired) == 5000
        assert elapsed_ms < 200, (
            f"Too slow: {elapsed_ms:.1f}ms for firing 5k windows"
        )

    def test_session_window_merge_correctness(self) -> None:
        mgr = WindowManager(SessionWindow(gap=5.0))
        # Records at t=1,2,3 should merge into one session
        for t in [1.0, 2.0, 3.0]:
            mgr.add(StreamRecord(key="k1", value={}, event_time=t))
        mgr.merge_sessions()
        # Record at t=20 should be a separate session
        mgr.add(StreamRecord(key="k1", value={}, event_time=20.0))
        mgr.merge_sessions()
        fired = mgr.fire(30.0)
        assert len(fired) == 2

    def test_sorted_index_partial_fire(self) -> None:
        """Fire only a subset of windows, verify remaining windows survive."""
        mgr = WindowManager(TumblingWindow(size=10.0))
        for i in range(100):
            mgr.add(
                StreamRecord(key="k1", value={"i": i}, event_time=float(i))
            )
        # Fire first 3 windows [0,10), [10,20), [20,30)
        fired1 = mgr.fire(30.0)
        assert len(fired1) == 3
        # Fire next 2 windows [30,40), [40,50)
        fired2 = mgr.fire(50.0)
        assert len(fired2) == 2
        assert mgr.total_fired == 5

    def test_sorted_index_no_double_fire(self) -> None:
        """Windows must not fire twice even with repeated fire() calls."""
        mgr = WindowManager(TumblingWindow(size=10.0))
        for i in range(20):
            mgr.add(StreamRecord(key="k1", value={}, event_time=float(i)))
        fired1 = mgr.fire(20.0)
        assert len(fired1) == 2
        fired2 = mgr.fire(20.0)
        assert len(fired2) == 0
        assert mgr.total_fired == 2

    def test_sorted_index_high_cardinality_scaling(self) -> None:
        """Sorted index should handle 1000 keys x 100 windows efficiently."""
        mgr = WindowManager(TumblingWindow(size=1.0))
        n_keys = 1000
        n_records = 50
        for k in range(n_keys):
            for t in range(n_records):
                mgr.add(
                    StreamRecord(
                        key=f"key_{k}",
                        value={"t": t},
                        event_time=float(t),
                    ),
                )

        start = time.perf_counter()
        fired = mgr.fire(25.0)
        elapsed_ms = (time.perf_counter() - start) * 1000

        # 1000 keys * 25 windows = 25000 fired windows
        assert len(fired) == 25_000
        assert elapsed_ms < 500, (
            f"Too slow: {elapsed_ms:.1f}ms for firing 25k windows"
        )


# ---------------------------------------------------------------------------
# Batch-optimized StreamTableJoin
# ---------------------------------------------------------------------------


class TestBatchStreamTableJoin:
    """Verify optimized process_batch produces correct results."""

    @staticmethod
    def _make_table(n: int) -> TableSnapshot:
        table = TableSnapshot(name="ref")
        for i in range(n):
            table.put(f"k{i}", {"ref_val": i * 10})
        return table

    @staticmethod
    def _make_records(n: int, key_range: int) -> list[StreamRecord]:
        return [
            StreamRecord(
                key=f"k{i % key_range}",
                value={"idx": i},
                event_time=float(i),
            )
            for i in range(n)
        ]

    def test_batch_correctness_inner(self) -> None:
        """Batch inner join should match per-record results."""
        table = self._make_table(5)
        records = self._make_records(10, key_range=8)  # some will miss

        join = StreamTableJoin(table, lambda r: r.key, join_type="inner")
        batch_results = join.process_batch(records)

        # Verify: only matching records appear
        for r in batch_results:
            assert r.value["__table_match__"] is True
            assert "ref_val" in r.value

        # All misses should be filtered out
        assert join.matched + join.unmatched == 10

    def test_batch_correctness_left(self) -> None:
        """Batch left join should preserve unmatched records."""
        table = self._make_table(3)
        records = self._make_records(6, key_range=5)

        join = StreamTableJoin(table, lambda r: r.key, join_type="left")
        batch_results = join.process_batch(records)

        # Left join: all records preserved
        assert len(batch_results) == 6
        matched_count = sum(
            1 for r in batch_results if r.value["__table_match__"]
        )
        unmatched_count = sum(
            1 for r in batch_results if not r.value["__table_match__"]
        )
        assert matched_count == join.matched
        assert unmatched_count == join.unmatched

    def test_batch_matches_individual_process(self) -> None:
        """Batch results should be identical to individual process() calls."""
        table = self._make_table(10)
        records = self._make_records(20, key_range=15)

        # Individual processing
        join1 = StreamTableJoin(table, lambda r: r.key, join_type="inner")
        individual = [
            r for rec in records if (r := join1.process(rec)) is not None
        ]

        # Batch processing
        join2 = StreamTableJoin(table, lambda r: r.key, join_type="inner")
        batch = join2.process_batch(records)

        assert len(individual) == len(batch)
        for a, b in zip(individual, batch):
            assert a.key == b.key
            assert a.value == b.value

    def test_batch_throughput(self) -> None:
        """Batch processing should handle 100k records efficiently."""
        table = self._make_table(1000)
        records = self._make_records(100_000, key_range=1500)

        join = StreamTableJoin(table, lambda r: r.key, join_type="inner")
        start = time.perf_counter()
        results = join.process_batch(records)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(results) > 0
        assert elapsed_ms < 2000, (
            f"Too slow: {elapsed_ms:.1f}ms for 100k batch join"
        )

    def test_batch_empty(self) -> None:
        """Empty batch should return empty results."""
        table = self._make_table(5)
        join = StreamTableJoin(table, lambda r: r.key)
        assert join.process_batch([]) == []
        assert join.matched == 0
        assert join.unmatched == 0
