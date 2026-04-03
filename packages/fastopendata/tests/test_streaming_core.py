"""Tests for streaming core primitives: StreamRecord, StreamBuffer, WatermarkTracker, DeduplicationStore."""

from __future__ import annotations

import asyncio

import pytest
from fastopendata.streaming.core import (
    DeduplicationStore,
    RecordType,
    StreamBuffer,
    StreamRecord,
    WatermarkTracker,
)

# ---------------------------------------------------------------------------
# StreamRecord
# ---------------------------------------------------------------------------


class TestStreamRecord:
    def test_creation_with_defaults(self) -> None:
        r = StreamRecord(key="k1", value={"x": 1}, event_time=100.0)
        assert r.key == "k1"
        assert r.value == {"x": 1}
        assert r.event_time == 100.0
        assert r.record_type == RecordType.INSERT
        assert r.record_id
        assert r.source == ""

    def test_frozen(self) -> None:
        r = StreamRecord(key="k", value={}, event_time=0.0)
        with pytest.raises(AttributeError):
            r.key = "other"  # type: ignore[misc]

    def test_watermark_factory(self) -> None:
        r = StreamRecord.watermark(42.0, source="src")
        assert r.record_type == RecordType.WATERMARK
        assert r.event_time == 42.0
        assert r.key == "__watermark__"
        assert r.source == "src"

    def test_content_hash_deterministic(self) -> None:
        r1 = StreamRecord(key="k", value={"a": 1, "b": 2}, event_time=0.0)
        r2 = StreamRecord(key="k", value={"b": 2, "a": 1}, event_time=0.0)
        assert r1.content_hash() == r2.content_hash()

    def test_content_hash_differs_for_different_values(self) -> None:
        r1 = StreamRecord(key="k", value={"a": 1}, event_time=0.0)
        r2 = StreamRecord(key="k", value={"a": 2}, event_time=0.0)
        assert r1.content_hash() != r2.content_hash()

    def test_record_types(self) -> None:
        for rt in RecordType:
            r = StreamRecord(key="k", value={}, event_time=0.0, record_type=rt)
            assert r.record_type == rt


# ---------------------------------------------------------------------------
# StreamBuffer
# ---------------------------------------------------------------------------


class TestStreamBuffer:
    def test_put_and_get(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=10)
            r = StreamRecord(key="k", value={"v": 1}, event_time=1.0)
            await buf.put(r)
            assert buf.size == 1
            out = await buf.get(timeout=1.0)
            assert out is not None
            assert out.key == "k"
            assert buf.total_ingested == 1
            assert buf.total_emitted == 1

        asyncio.run(_run())

    def test_get_timeout_returns_none(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=10)
            result = await buf.get(timeout=0.01)
            assert result is None

        asyncio.run(_run())

    def test_drain_batch(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            for i in range(5):
                await buf.put(
                    StreamRecord(key=f"k{i}", value={}, event_time=float(i))
                )
            batch = await buf.drain(max_batch=3)
            assert len(batch) == 3
            assert buf.size == 2

        asyncio.run(_run())

    def test_drain_empty_buffer(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=10)
            batch = await buf.drain()
            assert batch == []

        asyncio.run(_run())

    def test_close_prevents_put(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=10)
            buf.close()
            with pytest.raises(RuntimeError, match="closed"):
                await buf.put(StreamRecord(key="k", value={}, event_time=0.0))

        asyncio.run(_run())

    def test_is_full(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=2)
            await buf.put(StreamRecord(key="a", value={}, event_time=0.0))
            assert not buf.is_full
            await buf.put(StreamRecord(key="b", value={}, event_time=0.0))
            assert buf.is_full

        asyncio.run(_run())


# ---------------------------------------------------------------------------
# WatermarkTracker
# ---------------------------------------------------------------------------


class TestWatermarkTracker:
    def test_initial_watermark_is_zero(self) -> None:
        wt = WatermarkTracker()
        assert wt.current == 0.0

    def test_advance(self) -> None:
        wt = WatermarkTracker()
        wt.advance(10.0)
        assert wt.current == 10.0
        wt.advance(5.0)
        assert wt.current == 10.0
        wt.advance(20.0)
        assert wt.current == 20.0

    def test_is_late_within_allowed_lateness(self) -> None:
        wt = WatermarkTracker(allowed_lateness=5.0)
        wt.advance(100.0)
        r = StreamRecord(key="k", value={}, event_time=96.0)
        assert not wt.is_late(r)

    def test_is_late_beyond_allowed_lateness(self) -> None:
        wt = WatermarkTracker(allowed_lateness=5.0)
        wt.advance(100.0)
        r = StreamRecord(key="k", value={}, event_time=94.0)
        assert wt.is_late(r)
        assert wt.late_count == 1

    def test_late_count_accumulates(self) -> None:
        wt = WatermarkTracker(allowed_lateness=0.0)
        wt.advance(100.0)
        for _ in range(3):
            r = StreamRecord(key="k", value={}, event_time=50.0)
            wt.is_late(r)
        assert wt.late_count == 3


# ---------------------------------------------------------------------------
# DeduplicationStore
# ---------------------------------------------------------------------------


class TestDeduplicationStore:
    def test_new_record_is_accepted(self) -> None:
        ds = DeduplicationStore(capacity=10)
        assert ds.check_and_add("id-1") is True

    def test_duplicate_is_rejected(self) -> None:
        ds = DeduplicationStore(capacity=10)
        ds.check_and_add("id-1")
        assert ds.check_and_add("id-1") is False
        assert ds.duplicates_dropped == 1

    def test_lru_eviction(self) -> None:
        ds = DeduplicationStore(capacity=3)
        ds.check_and_add("a")
        ds.check_and_add("b")
        ds.check_and_add("c")
        ds.check_and_add("d")  # evicts "a"
        assert ds.check_and_add("a") is True  # "a" was evicted
        assert ds.check_and_add("d") is False  # "d" still present

    def test_capacity_respected(self) -> None:
        ds = DeduplicationStore(capacity=5)
        for i in range(10):
            ds.check_and_add(f"id-{i}")
        assert ds.check_and_add("id-9") is False
        assert ds.check_and_add("id-0") is True
