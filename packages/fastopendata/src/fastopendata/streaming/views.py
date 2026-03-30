"""Incremental materialized views with differential changelog.

An :class:`IncrementalView` maintains a live, incrementally updated
snapshot of query results.  Instead of recomputing the entire result on
every event, only the *delta* (inserts / updates / deletes) is applied.
Consumers can subscribe to the changelog for downstream propagation.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from fastopendata.streaming.core import RecordType, StreamRecord


class ChangeType(Enum):
    """Type of change applied to the materialized view."""

    INSERT = auto()
    UPDATE = auto()
    DELETE = auto()


@dataclass(frozen=True, slots=True)
class ChangelogEntry:
    """A single differential change in the view's changelog."""

    change_type: ChangeType
    key: str
    old_value: dict[str, Any] | None
    new_value: dict[str, Any] | None
    timestamp: float = field(default_factory=time.time)


AggregateFunction = Callable[[list[dict[str, Any]]], dict[str, Any]]


class IncrementalView:
    """Live materialized view with incremental maintenance.

    The view stores the current snapshot keyed by record key, and emits
    a changelog of differential updates that downstream operators or
    sinks can consume.

    Parameters
    ----------
    name : str
        Human-readable name for the view (used in metrics).
    aggregate_fn : AggregateFunction | None
        Optional aggregation applied to grouped records before
        materializing.  When *None*, the view stores the latest value
        per key (last-write-wins).

    """

    def __init__(
        self,
        name: str,
        aggregate_fn: AggregateFunction | None = None,
    ) -> None:
        self._name = name
        self._aggregate_fn = aggregate_fn
        # Current snapshot: key → materialized value
        self._snapshot: dict[str, dict[str, Any]] = {}
        # All records grouped by key (needed for re-aggregation on update/delete)
        self._records_by_key: dict[str, list[StreamRecord]] = {}
        # Ordered changelog
        self._changelog: list[ChangelogEntry] = []
        # Async subscribers
        self._subscribers: list[asyncio.Queue[ChangelogEntry]] = []
        self._total_changes: int = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Read-only reference to the current materialized state."""
        return self._snapshot

    @property
    def changelog(self) -> list[ChangelogEntry]:
        return list(self._changelog)

    @property
    def total_changes(self) -> int:
        return self._total_changes

    def subscribe(self) -> asyncio.Queue[ChangelogEntry]:
        """Create a new subscriber queue that receives future changelog entries."""
        q: asyncio.Queue[ChangelogEntry] = asyncio.Queue()
        self._subscribers.append(q)
        return q

    async def apply(self, record: StreamRecord) -> ChangelogEntry | None:
        """Apply a stream record to the view and return the changelog delta."""
        key = record.key

        if record.record_type == RecordType.DELETE:
            return await self._handle_delete(key, record)
        # INSERT or UPDATE
        return await self._handle_upsert(key, record)

    async def _handle_upsert(
        self,
        key: str,
        record: StreamRecord,
    ) -> ChangelogEntry | None:
        old_value = self._snapshot.get(key)

        # Track raw records for aggregation
        if key not in self._records_by_key:
            self._records_by_key[key] = []
        self._records_by_key[key].append(record)

        # Compute new materialized value
        if self._aggregate_fn is not None:
            new_value = self._aggregate_fn(
                [r.value for r in self._records_by_key[key]],
            )
        else:
            new_value = dict(record.value)

        # Skip no-op updates
        if old_value == new_value:
            return None

        self._snapshot[key] = new_value
        change_type = ChangeType.UPDATE if old_value is not None else ChangeType.INSERT
        entry = ChangelogEntry(
            change_type=change_type,
            key=key,
            old_value=old_value,
            new_value=new_value,
        )
        await self._emit(entry)
        return entry

    async def _handle_delete(
        self,
        key: str,
        record: StreamRecord,
    ) -> ChangelogEntry | None:
        old_value = self._snapshot.pop(key, None)
        self._records_by_key.pop(key, None)
        if old_value is None:
            return None
        entry = ChangelogEntry(
            change_type=ChangeType.DELETE,
            key=key,
            old_value=old_value,
            new_value=None,
        )
        await self._emit(entry)
        return entry

    async def _emit(self, entry: ChangelogEntry) -> None:
        self._changelog.append(entry)
        self._total_changes += 1
        for q in self._subscribers:
            await q.put(entry)

    def query(
        self,
        predicate: Callable[[str, dict[str, Any]], bool] | None = None,
    ) -> list[dict[str, Any]]:
        """Point-in-time query against the current snapshot.

        Parameters
        ----------
        predicate : callable, optional
            Filter ``(key, value) → bool``.  When *None*, returns all rows.

        """
        if predicate is None:
            return list(self._snapshot.values())
        return [v for k, v in self._snapshot.items() if predicate(k, v)]
