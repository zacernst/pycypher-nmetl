"""Tests for incremental materialized views with differential changelog."""

from __future__ import annotations

import asyncio
from typing import Any

from fastopendata.streaming.core import RecordType, StreamRecord
from fastopendata.streaming.views import ChangeType, IncrementalView


class TestIncrementalView:
    def test_insert_creates_entry(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            r = StreamRecord(key="k1", value={"x": 10}, event_time=1.0)
            entry = await view.apply(r)
            assert entry is not None
            assert entry.change_type == ChangeType.INSERT
            assert entry.key == "k1"
            assert entry.new_value == {"x": 10}
            assert entry.old_value is None
            assert view.snapshot["k1"] == {"x": 10}

        asyncio.run(_run())

    def test_update_existing_key(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="k1", value={"x": 1}, event_time=1.0))
            entry = await view.apply(
                StreamRecord(key="k1", value={"x": 2}, event_time=2.0),
            )
            assert entry is not None
            assert entry.change_type == ChangeType.UPDATE
            assert entry.old_value == {"x": 1}
            assert entry.new_value == {"x": 2}

        asyncio.run(_run())

    def test_noop_update_returns_none(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="k1", value={"x": 1}, event_time=1.0))
            entry = await view.apply(
                StreamRecord(key="k1", value={"x": 1}, event_time=2.0),
            )
            assert entry is None

        asyncio.run(_run())

    def test_delete(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="k1", value={"x": 1}, event_time=1.0))
            entry = await view.apply(
                StreamRecord(
                    key="k1",
                    value={},
                    event_time=2.0,
                    record_type=RecordType.DELETE,
                ),
            )
            assert entry is not None
            assert entry.change_type == ChangeType.DELETE
            assert "k1" not in view.snapshot

        asyncio.run(_run())

    def test_delete_nonexistent_returns_none(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            entry = await view.apply(
                StreamRecord(
                    key="missing",
                    value={},
                    event_time=1.0,
                    record_type=RecordType.DELETE,
                ),
            )
            assert entry is None

        asyncio.run(_run())

    def test_changelog_accumulates(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="a", value={"v": 1}, event_time=1.0))
            await view.apply(StreamRecord(key="b", value={"v": 2}, event_time=2.0))
            assert len(view.changelog) == 2
            assert view.total_changes == 2

        asyncio.run(_run())

    def test_subscriber_receives_changes(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            sub = view.subscribe()
            await view.apply(StreamRecord(key="k", value={"v": 1}, event_time=1.0))
            entry = await asyncio.wait_for(sub.get(), timeout=1.0)
            assert entry.change_type == ChangeType.INSERT
            assert entry.key == "k"

        asyncio.run(_run())

    def test_aggregate_function(self) -> None:
        def sum_agg(records: list[dict[str, Any]]) -> dict[str, Any]:
            total = sum(r.get("amount", 0) for r in records)
            return {"total": total, "count": len(records)}

        async def _run() -> None:
            view = IncrementalView(name="agg_test", aggregate_fn=sum_agg)
            await view.apply(
                StreamRecord(key="k", value={"amount": 10}, event_time=1.0),
            )
            assert view.snapshot["k"] == {"total": 10, "count": 1}
            await view.apply(
                StreamRecord(key="k", value={"amount": 20}, event_time=2.0),
            )
            assert view.snapshot["k"] == {"total": 30, "count": 2}

        asyncio.run(_run())

    def test_query_no_predicate(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="a", value={"x": 1}, event_time=1.0))
            await view.apply(StreamRecord(key="b", value={"x": 2}, event_time=2.0))
            results = view.query()
            assert len(results) == 2

        asyncio.run(_run())

    def test_query_with_predicate(self) -> None:
        async def _run() -> None:
            view = IncrementalView(name="test")
            await view.apply(StreamRecord(key="a", value={"x": 1}, event_time=1.0))
            await view.apply(StreamRecord(key="b", value={"x": 2}, event_time=2.0))
            results = view.query(predicate=lambda k, v: v["x"] > 1)
            assert len(results) == 1
            assert results[0]["x"] == 2

        asyncio.run(_run())

    def test_name_property(self) -> None:
        view = IncrementalView(name="my_view")
        assert view.name == "my_view"
