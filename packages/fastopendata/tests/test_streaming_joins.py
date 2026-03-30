"""Tests for stream-table joins."""

from __future__ import annotations

import pytest
from fastopendata.streaming.core import StreamRecord
from fastopendata.streaming.joins import StreamTableJoin, TableSnapshot


class TestTableSnapshot:
    def test_put_and_get(self) -> None:
        t = TableSnapshot(name="users")
        t.put("u1", {"name": "Alice"})
        assert t.get("u1") == {"name": "Alice"}
        assert t.size == 1

    def test_get_missing_returns_none(self) -> None:
        t = TableSnapshot(name="users")
        assert t.get("missing") is None

    def test_delete(self) -> None:
        t = TableSnapshot(name="users")
        t.put("u1", {"name": "Alice"})
        assert t.delete("u1") is True
        assert t.get("u1") is None
        assert t.size == 0

    def test_delete_missing_returns_false(self) -> None:
        t = TableSnapshot(name="users")
        assert t.delete("missing") is False

    def test_version_increments(self) -> None:
        t = TableSnapshot(name="users")
        v0 = t.version
        t.put("u1", {"name": "Alice"})
        assert t.version == v0 + 1
        t.delete("u1")
        assert t.version == v0 + 2

    def test_bulk_load(self) -> None:
        t = TableSnapshot(name="users")
        t.put("old", {"name": "Old"})
        t.bulk_load({"u1": {"name": "A"}, "u2": {"name": "B"}})
        assert t.get("old") is None
        assert t.size == 2
        assert t.get("u1") == {"name": "A"}


class TestStreamTableJoin:
    def _make_record(self, key: str, user_id: str) -> StreamRecord:
        return StreamRecord(
            key=key,
            value={"user_id": user_id, "action": "click"},
            event_time=1.0,
        )

    def test_inner_join_match(self) -> None:
        table = TableSnapshot(name="users")
        table.put("u1", {"name": "Alice"})
        join = StreamTableJoin(
            table,
            key_extractor=lambda r: r.value["user_id"],
            join_type="inner",
        )
        result = join.process(self._make_record("e1", "u1"))
        assert result is not None
        assert result.value["name"] == "Alice"
        assert result.value["action"] == "click"
        assert result.value["__table_match__"] is True
        assert join.matched == 1

    def test_inner_join_miss(self) -> None:
        table = TableSnapshot(name="users")
        join = StreamTableJoin(
            table,
            key_extractor=lambda r: r.value["user_id"],
            join_type="inner",
        )
        result = join.process(self._make_record("e1", "missing"))
        assert result is None
        assert join.unmatched == 1

    def test_left_join_miss(self) -> None:
        table = TableSnapshot(name="users")
        join = StreamTableJoin(
            table,
            key_extractor=lambda r: r.value["user_id"],
            join_type="left",
        )
        result = join.process(self._make_record("e1", "missing"))
        assert result is not None
        assert result.value["__table_match__"] is False
        assert join.unmatched == 1

    def test_invalid_join_type(self) -> None:
        table = TableSnapshot(name="t")
        with pytest.raises(ValueError, match="join_type"):
            StreamTableJoin(table, key_extractor=lambda r: r.key, join_type="cross")

    def test_process_batch(self) -> None:
        table = TableSnapshot(name="users")
        table.put("u1", {"name": "Alice"})
        table.put("u2", {"name": "Bob"})
        join = StreamTableJoin(
            table,
            key_extractor=lambda r: r.value["user_id"],
            join_type="inner",
        )
        records = [
            self._make_record("e1", "u1"),
            self._make_record("e2", "missing"),
            self._make_record("e3", "u2"),
        ]
        results = join.process_batch(records)
        assert len(results) == 2
        assert join.matched == 2
        assert join.unmatched == 1

    def test_preserves_record_metadata(self) -> None:
        table = TableSnapshot(name="users")
        table.put("u1", {"name": "Alice"})
        join = StreamTableJoin(table, key_extractor=lambda r: r.value["user_id"])
        r = StreamRecord(
            key="e1",
            value={"user_id": "u1"},
            event_time=42.0,
            source="clicks",
            record_id="rid-1",
        )
        result = join.process(r)
        assert result is not None
        assert result.event_time == 42.0
        assert result.source == "clicks"
        assert result.record_id == "rid-1"

    def test_table_property(self) -> None:
        table = TableSnapshot(name="t")
        join = StreamTableJoin(table, key_extractor=lambda r: r.key)
        assert join.table is table
