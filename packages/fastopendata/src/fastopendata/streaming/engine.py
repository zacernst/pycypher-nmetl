"""Top-level streaming query engine orchestrator.

:class:`StreamEngine` wires together sources, operators (windowing,
joins, views), and sinks into a running pipeline.  It manages the
event loop, watermark advancement, exactly-once dedup, and graceful
shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fastopendata.streaming.core import (
    DeduplicationStore,
    RecordType,
    StreamBuffer,
    StreamRecord,
    WatermarkTracker,
)
from fastopendata.streaming.joins import StreamTableJoin
from fastopendata.streaming.views import IncrementalView
from fastopendata.streaming.windows import WindowManager, WindowState

_logger = logging.getLogger(__name__)

SinkCallback = Callable[[StreamRecord], Any]


@dataclass
class PipelineMetrics:
    """Runtime metrics for the streaming pipeline."""

    records_processed: int = 0
    records_dropped_late: int = 0
    records_dropped_dedup: int = 0
    windows_fired: int = 0
    view_changes: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)

    @property
    def uptime(self) -> float:
        return time.time() - self.start_time

    @property
    def throughput(self) -> float:
        elapsed = self.uptime
        if elapsed <= 0:
            return 0.0
        return self.records_processed / elapsed

    def to_dict(self) -> dict[str, Any]:
        return {
            "records_processed": self.records_processed,
            "records_dropped_late": self.records_dropped_late,
            "records_dropped_dedup": self.records_dropped_dedup,
            "windows_fired": self.windows_fired,
            "view_changes": self.view_changes,
            "errors": self.errors,
            "uptime_seconds": round(self.uptime, 2),
            "throughput_rps": round(self.throughput, 2),
        }


class StreamEngine:
    """Orchestrates the streaming query pipeline.

    The engine consumes records from a :class:`StreamBuffer`, applies
    deduplication and watermark filtering, routes records through
    optional window managers and stream-table joins, materializes
    results into incremental views, and forwards outputs to sink
    callbacks.

    Parameters
    ----------
    buffer : StreamBuffer
        Input record buffer.
    watermark : WatermarkTracker
        Watermark tracker for event-time progress.
    dedup : DeduplicationStore | None
        Optional deduplication store for exactly-once semantics.
    batch_size : int
        Maximum records to drain per processing cycle.
    cycle_interval : float
        Seconds between processing cycles (controls latency).

    """

    def __init__(
        self,
        buffer: StreamBuffer,
        watermark: WatermarkTracker | None = None,
        dedup: DeduplicationStore | None = None,
        *,
        batch_size: int = 256,
        cycle_interval: float = 0.05,
    ) -> None:
        self._buffer = buffer
        self._watermark = watermark or WatermarkTracker()
        self._dedup = dedup or DeduplicationStore()
        self._batch_size = batch_size
        self._cycle_interval = cycle_interval

        self._window_managers: list[WindowManager] = []
        self._joins: list[StreamTableJoin] = []
        self._views: list[IncrementalView] = []
        self._sinks: list[SinkCallback] = []
        self._transforms: list[Callable[[StreamRecord], StreamRecord | None]] = []

        self._metrics = PipelineMetrics()
        self._running = False
        self._task: asyncio.Task[None] | None = None

    @property
    def metrics(self) -> PipelineMetrics:
        return self._metrics

    @property
    def is_running(self) -> bool:
        return self._running

    def add_window_manager(self, wm: WindowManager) -> None:
        self._window_managers.append(wm)

    def add_join(self, join: StreamTableJoin) -> None:
        self._joins.append(join)

    def add_view(self, view: IncrementalView) -> None:
        self._views.append(view)

    def add_sink(self, sink: SinkCallback) -> None:
        self._sinks.append(sink)

    def add_transform(self, fn: Callable[[StreamRecord], StreamRecord | None]) -> None:
        """Add a stateless map/filter transform to the pipeline."""
        self._transforms.append(fn)

    async def start(self) -> None:
        """Start the engine's processing loop as a background task."""
        if self._running:
            return
        self._running = True
        self._metrics = PipelineMetrics()
        self._task = asyncio.create_task(self._run_loop())
        _logger.info("StreamEngine started")

    async def stop(self) -> None:
        """Gracefully stop the engine and await final flush."""
        self._running = False
        if self._task is not None:
            await self._task
            self._task = None
        _logger.info(
            "StreamEngine stopped — %s",
            self._metrics.to_dict(),
        )

    async def _run_loop(self) -> None:
        """Main processing loop: drain → filter → transform → route → materialize."""
        while self._running:
            batch = await self._buffer.drain(self._batch_size)
            if not batch:
                await asyncio.sleep(self._cycle_interval)
                continue

            for record in batch:
                try:
                    await self._process_record(record)
                except Exception:
                    _logger.exception("Error processing record %s", record.record_id)
                    self._metrics.errors += 1

            # Fire windows after processing the batch
            for wm in self._window_managers:
                fired = wm.fire(self._watermark.current)
                for key, window_state in fired:
                    self._metrics.windows_fired += 1
                    await self._handle_window_fire(key, window_state)

    async def _process_record(self, record: StreamRecord) -> None:
        # Handle watermark advancement records
        if record.record_type == RecordType.WATERMARK:
            self._watermark.advance(record.event_time)
            return

        # Filter: dedup and lateness checks
        if not self._filter_record(record):
            return

        self._watermark.advance(record.event_time)

        # Transform and enrich
        current = self._apply_pipeline(record)
        if current is None:
            return

        # Route to downstream operators
        await self._route_record(current)
        self._metrics.records_processed += 1

    def _filter_record(self, record: StreamRecord) -> bool:
        """Return True if the record passes dedup and lateness checks."""
        if not self._dedup.check_and_add(record.record_id):
            self._metrics.records_dropped_dedup += 1
            return False
        if self._watermark.is_late(record):
            self._metrics.records_dropped_late += 1
            return False
        return True

    def _apply_pipeline(self, record: StreamRecord) -> StreamRecord | None:
        """Apply transforms and stream-table joins, returning None if filtered."""
        current: StreamRecord | None = record
        for transform in self._transforms:
            if current is None:
                return None
            current = transform(current)
        for join in self._joins:
            if current is None:
                return None
            current = join.process(current)
        return current

    async def _route_record(self, record: StreamRecord) -> None:
        """Route a processed record to windows, views, and sinks."""
        for wm in self._window_managers:
            wm.add(record)
        for view in self._views:
            entry = await view.apply(record)
            if entry is not None:
                self._metrics.view_changes += 1
        for sink in self._sinks:
            sink(record)

    async def _handle_window_fire(self, key: str, state: WindowState) -> None:
        """Aggregate a fired window's records and push through views/sinks."""
        if not state.records:
            return
        # Create a summary record for the window
        summary = StreamRecord(
            key=key,
            value={
                "__window_start__": state.spec.start,
                "__window_end__": state.spec.end,
                "__window_count__": state.count,
                "__window_records__": [r.value for r in state.records],
            },
            event_time=state.spec.end,
            source="window_aggregate",
        )
        for view in self._views:
            entry = await view.apply(summary)
            if entry is not None:
                self._metrics.view_changes += 1
        for sink in self._sinks:
            sink(summary)
