"""Core streaming primitives: records, buffers, watermarks, deduplication.

This module provides the foundational data structures for the streaming
query engine. All components are designed for concurrent access using
asyncio and are safe for use in a multi-consumer pipeline.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Self


class RecordType(Enum):
    """Discriminator for stream record semantics."""

    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()
    WATERMARK = auto()


@dataclass(frozen=True, slots=True)
class StreamRecord:
    """Immutable event wrapper carrying payload and temporal metadata.

    Attributes
    ----------
    key : str
        Partition / grouping key for the record.
    value : dict[str, Any]
        The event payload.
    event_time : float
        When the event *actually* happened (epoch seconds).
    processing_time : float
        When the engine first observed the record.
    record_type : RecordType
        Semantic intent (insert / update / delete / watermark).
    record_id : str
        Globally unique identifier for exactly-once dedup.
    source : str
        Identifier of the originating stream or topic.

    """

    key: str
    value: dict[str, Any]
    event_time: float
    processing_time: float = field(default_factory=time.time)
    record_type: RecordType = RecordType.INSERT
    record_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    source: str = ""

    @classmethod
    def watermark(cls, timestamp: float, *, source: str = "") -> Self:
        """Factory for a watermark-only sentinel record."""
        return cls(
            key="__watermark__",
            value={},
            event_time=timestamp,
            record_type=RecordType.WATERMARK,
            source=source,
        )

    def content_hash(self) -> str:
        """Deterministic hash of key + value for deduplication.

        Uses MD5 for speed — this is not a security context, just
        content-addressable dedup where collision resistance of MD5
        is more than sufficient.
        """
        raw = f"{self.key}:{sorted(self.value.items())}"
        return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()


class StreamBuffer:
    """Bounded, async ring buffer for backpressure-aware record ingestion.

    When the buffer is full, :meth:`put` awaits until a consumer drains
    space — providing natural backpressure without dropping records.

    Parameters
    ----------
    max_size : int
        Maximum number of records the buffer can hold before blocking
        producers.

    """

    def __init__(self, max_size: int = 10_000) -> None:
        self._queue: asyncio.Queue[StreamRecord] = asyncio.Queue(maxsize=max_size)
        self._max_size = max_size
        self._total_ingested: int = 0
        self._total_emitted: int = 0
        self._closed = False
        self._counter_lock = threading.Lock()

    @property
    def size(self) -> int:
        """Current number of buffered records."""
        return self._queue.qsize()

    @property
    def is_full(self) -> bool:
        return self._queue.full()

    @property
    def total_ingested(self) -> int:
        return self._total_ingested

    @property
    def total_emitted(self) -> int:
        return self._total_emitted

    async def put(self, record: StreamRecord) -> None:
        """Enqueue a record, awaiting if the buffer is full."""
        if self._closed:
            msg = "Cannot put into a closed buffer"
            raise RuntimeError(msg)
        await self._queue.put(record)
        with self._counter_lock:
            self._total_ingested += 1

    async def get(self, timeout: float | None = None) -> StreamRecord | None:
        """Dequeue the next record, returning *None* on timeout."""
        try:
            record = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        with self._counter_lock:
            self._total_emitted += 1
        return record

    async def drain(self, max_batch: int = 256) -> list[StreamRecord]:
        """Non-blocking drain of up to *max_batch* records."""
        batch: list[StreamRecord] = []
        for _ in range(max_batch):
            try:
                record = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            batch.append(record)
        if batch:
            with self._counter_lock:
                self._total_emitted += len(batch)
        return batch

    def close(self) -> None:
        """Signal that no more records will arrive."""
        self._closed = True


class WatermarkTracker:
    """Monotonically advancing watermark with configurable allowed lateness.

    The watermark represents the engine's belief that *all* events with
    event-time ≤ watermark have been observed.  Late events arriving after
    the watermark has advanced beyond their event-time by more than
    ``allowed_lateness`` are dropped.

    Parameters
    ----------
    allowed_lateness : float
        Maximum seconds an event may arrive after the watermark has
        passed its event-time before being considered *too late*.

    """

    def __init__(self, allowed_lateness: float = 5.0) -> None:
        self._watermark: float = 0.0
        self._allowed_lateness = allowed_lateness
        self._late_count: int = 0

    @property
    def current(self) -> float:
        """Current watermark value (epoch seconds)."""
        return self._watermark

    @property
    def late_count(self) -> int:
        """Number of records dropped for being too late."""
        return self._late_count

    def advance(self, event_time: float) -> None:
        """Advance the watermark if *event_time* exceeds the current value."""
        self._watermark = max(self._watermark, event_time)

    def is_late(self, record: StreamRecord) -> bool:
        """Return *True* if the record's event-time is unacceptably late."""
        if record.event_time < self._watermark - self._allowed_lateness:
            self._late_count += 1
            return True
        return False


class DeduplicationStore:
    """Bounded LRU store for exactly-once record deduplication.

    Retains up to ``capacity`` record IDs.  When the store is full the
    oldest entry is evicted.

    Parameters
    ----------
    capacity : int
        Maximum number of record IDs to remember.

    """

    def __init__(self, capacity: int = 100_000) -> None:
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._capacity = capacity
        self._duplicates_dropped: int = 0

    @property
    def duplicates_dropped(self) -> int:
        return self._duplicates_dropped

    def check_and_add(self, record_id: str) -> bool:
        """Return *True* if the record is new (not a duplicate).

        If the record has been seen before, it is counted as a duplicate
        and *False* is returned.
        """
        if record_id in self._seen:
            self._duplicates_dropped += 1
            return False
        self._seen[record_id] = None
        if len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        return True
