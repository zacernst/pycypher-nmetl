"""Real-time streaming query engine for FastOpenData.

Provides incremental materialized views, temporal windowing, stream-table
joins, event-time processing, and exactly-once semantics with sub-second
latency for streaming updates.

Core components
---------------
* :class:`StreamRecord` — immutable event wrapper with event-time metadata
* :class:`StreamBuffer` — thread-safe, bounded async ring buffer
* :class:`WatermarkTracker` — monotonic watermark with allowed lateness
* :class:`WindowAssigner` — tumbling, sliding, and session window assignment
* :class:`IncrementalView` — differential materialized view with changelog
* :class:`StreamTableJoin` — enriches stream records against a snapshot table
* :class:`StreamEngine` — top-level orchestrator wiring sources → operators → sinks
"""

from __future__ import annotations

from fastopendata.streaming.core import (
    DeduplicationStore,
    StreamBuffer,
    StreamRecord,
    WatermarkTracker,
)
from fastopendata.streaming.engine import StreamEngine
from fastopendata.streaming.joins import StreamTableJoin
from fastopendata.streaming.views import IncrementalView
from fastopendata.streaming.windows import (
    SessionWindow,
    SlidingWindow,
    TumblingWindow,
    WindowAssigner,
    WindowSpec,
)

__all__: list[str] = [
    "DeduplicationStore",
    "IncrementalView",
    "SessionWindow",
    "SlidingWindow",
    "StreamBuffer",
    "StreamEngine",
    "StreamRecord",
    "StreamTableJoin",
    "TumblingWindow",
    "WatermarkTracker",
    "WindowAssigner",
    "WindowSpec",
]
