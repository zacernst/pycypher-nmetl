"""Tests for the StreamEngine orchestrator — end-to-end pipeline validation."""

from __future__ import annotations

import asyncio

from fastopendata.streaming.core import (
    DeduplicationStore,
    StreamBuffer,
    StreamRecord,
    WatermarkTracker,
)
from fastopendata.streaming.engine import PipelineMetrics, StreamEngine
from fastopendata.streaming.joins import StreamTableJoin, TableSnapshot
from fastopendata.streaming.views import IncrementalView
from fastopendata.streaming.windows import TumblingWindow, WindowManager

# ---------------------------------------------------------------------------
# PipelineMetrics
# ---------------------------------------------------------------------------


class TestPipelineMetrics:
    def test_initial_state(self) -> None:
        m = PipelineMetrics()
        assert m.records_processed == 0
        assert m.errors == 0

    def test_throughput(self) -> None:
        m = PipelineMetrics()
        m.records_processed = 100
        assert m.throughput > 0

    def test_to_dict(self) -> None:
        m = PipelineMetrics()
        d = m.to_dict()
        assert "records_processed" in d
        assert "uptime_seconds" in d
        assert "throughput_rps" in d


# ---------------------------------------------------------------------------
# StreamEngine — integration tests
# ---------------------------------------------------------------------------


class TestStreamEngine:
    def test_basic_pipeline(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            engine = StreamEngine(buf, cycle_interval=0.01)
            view = IncrementalView(name="output")
            engine.add_view(view)

            await engine.start()
            assert engine.is_running

            for i in range(5):
                await buf.put(
                    StreamRecord(
                        key=f"k{i}", value={"v": i}, event_time=float(i)
                    ),
                )

            await asyncio.sleep(0.15)
            await engine.stop()

            assert not engine.is_running
            assert engine.metrics.records_processed == 5
            assert len(view.snapshot) == 5

        asyncio.run(_run())

    def test_deduplication(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            dedup = DeduplicationStore(capacity=100)
            engine = StreamEngine(buf, dedup=dedup, cycle_interval=0.01)
            view = IncrementalView(name="output")
            engine.add_view(view)

            await engine.start()
            await buf.put(
                StreamRecord(
                    key="k",
                    value={"v": 1},
                    event_time=1.0,
                    record_id="dup-id",
                ),
            )
            await buf.put(
                StreamRecord(
                    key="k",
                    value={"v": 2},
                    event_time=2.0,
                    record_id="dup-id",
                ),
            )
            await asyncio.sleep(0.15)
            await engine.stop()

            assert engine.metrics.records_processed == 1
            assert engine.metrics.records_dropped_dedup == 1

        asyncio.run(_run())

    def test_late_event_filtering(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            wm = WatermarkTracker(allowed_lateness=2.0)
            engine = StreamEngine(buf, watermark=wm, cycle_interval=0.01)
            view = IncrementalView(name="output")
            engine.add_view(view)

            await engine.start()
            await buf.put(
                StreamRecord(key="k1", value={"v": 1}, event_time=100.0)
            )
            await asyncio.sleep(0.05)
            await buf.put(
                StreamRecord(key="k2", value={"v": 2}, event_time=50.0)
            )
            await asyncio.sleep(0.1)
            await engine.stop()

            assert engine.metrics.records_dropped_late == 1
            assert engine.metrics.records_processed == 1

        asyncio.run(_run())

    def test_transform_pipeline(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            engine = StreamEngine(buf, cycle_interval=0.01)
            view = IncrementalView(name="output")
            engine.add_view(view)

            def double_value(r: StreamRecord) -> StreamRecord:
                return StreamRecord(
                    key=r.key,
                    value={"v": r.value["v"] * 2},
                    event_time=r.event_time,
                    record_id=r.record_id,
                    source=r.source,
                )

            engine.add_transform(double_value)
            await engine.start()
            await buf.put(
                StreamRecord(key="k", value={"v": 5}, event_time=1.0)
            )
            await asyncio.sleep(0.1)
            await engine.stop()

            assert view.snapshot["k"]["v"] == 10

        asyncio.run(_run())

    def test_filter_transform(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            engine = StreamEngine(buf, cycle_interval=0.01)
            view = IncrementalView(name="output")
            engine.add_view(view)

            def filter_fn(r: StreamRecord) -> StreamRecord | None:
                if r.value.get("v", 0) < 3:
                    return None
                return r

            engine.add_transform(filter_fn)
            await engine.start()
            for i in range(5):
                await buf.put(
                    StreamRecord(
                        key=f"k{i}", value={"v": i}, event_time=float(i)
                    ),
                )
            await asyncio.sleep(0.15)
            await engine.stop()

            assert len(view.snapshot) == 2

        asyncio.run(_run())

    def test_stream_table_join_integration(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            engine = StreamEngine(buf, cycle_interval=0.01)
            table = TableSnapshot(name="users")
            table.put("u1", {"name": "Alice"})
            join = StreamTableJoin(
                table,
                key_extractor=lambda r: r.value.get("uid", ""),
                join_type="inner",
            )
            engine.add_join(join)
            view = IncrementalView(name="enriched")
            engine.add_view(view)

            await engine.start()
            await buf.put(
                StreamRecord(
                    key="e1",
                    value={"uid": "u1", "action": "click"},
                    event_time=1.0,
                ),
            )
            await buf.put(
                StreamRecord(
                    key="e2",
                    value={"uid": "missing", "action": "view"},
                    event_time=2.0,
                ),
            )
            await asyncio.sleep(0.15)
            await engine.stop()

            assert len(view.snapshot) == 1
            assert "name" in view.snapshot["e1"]

        asyncio.run(_run())

    def test_windowed_aggregation(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            wm = WatermarkTracker(allowed_lateness=0.0)
            engine = StreamEngine(buf, watermark=wm, cycle_interval=0.01)
            window_mgr = WindowManager(TumblingWindow(size=10.0))
            engine.add_window_manager(window_mgr)

            fired_records: list[StreamRecord] = []
            engine.add_sink(lambda r: fired_records.append(r))

            await engine.start()
            for i in range(5):
                await buf.put(
                    StreamRecord(key="k", value={"v": i}, event_time=float(i)),
                )
            await asyncio.sleep(0.05)
            await buf.put(
                StreamRecord(key="k", value={"v": 99}, event_time=11.0)
            )
            await asyncio.sleep(0.15)
            await engine.stop()

            assert engine.metrics.windows_fired >= 1

        asyncio.run(_run())

    def test_sink_receives_records(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            engine = StreamEngine(buf, cycle_interval=0.01)
            collected: list[StreamRecord] = []
            engine.add_sink(lambda r: collected.append(r))

            await engine.start()
            await buf.put(
                StreamRecord(key="k", value={"v": 1}, event_time=1.0)
            )
            await asyncio.sleep(0.1)
            await engine.stop()

            assert len(collected) == 1

        asyncio.run(_run())

    def test_watermark_record_advances_watermark(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=100)
            wm = WatermarkTracker()
            engine = StreamEngine(buf, watermark=wm, cycle_interval=0.01)

            await engine.start()
            await buf.put(StreamRecord.watermark(50.0))
            await asyncio.sleep(0.1)
            await engine.stop()

            assert wm.current == 50.0
            assert engine.metrics.records_processed == 0

        asyncio.run(_run())

    def test_start_stop_idempotent(self) -> None:
        async def _run() -> None:
            buf = StreamBuffer(max_size=10)
            engine = StreamEngine(buf, cycle_interval=0.01)
            await engine.start()
            await engine.start()  # no-op
            assert engine.is_running
            await engine.stop()
            await engine.stop()  # no-op
            assert not engine.is_running

        asyncio.run(_run())
