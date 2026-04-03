"""Stream-table join operator.

A :class:`StreamTableJoin` enriches every incoming stream record by
looking up matching rows in a (possibly evolving) reference table.
The table side can be updated independently, making this suitable for
*slowly changing dimension* patterns common in ETL.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fastopendata.streaming.core import StreamRecord


@dataclass
class TableSnapshot:
    """In-memory representation of the table side of a stream-table join.

    The table is keyed by a string lookup key.  Rows can be inserted,
    updated, or deleted independently of the stream.

    Parameters
    ----------
    name : str
        Identifier for the table (used in metrics / logging).

    """

    name: str
    _rows: dict[str, dict[str, Any]] = field(default_factory=dict)
    _version: int = 0

    @property
    def version(self) -> int:
        return self._version

    @property
    def size(self) -> int:
        return len(self._rows)

    def put(self, key: str, value: dict[str, Any]) -> None:
        self._rows[key] = value
        self._version += 1

    def get(self, key: str) -> dict[str, Any] | None:
        return self._rows.get(key)

    def delete(self, key: str) -> bool:
        if key in self._rows:
            del self._rows[key]
            self._version += 1
            return True
        return False

    def bulk_load(self, rows: dict[str, dict[str, Any]]) -> None:
        """Replace the entire table contents atomically."""
        self._rows = dict(rows)
        self._version += 1


JoinKeyExtractor = Callable[[StreamRecord], str]


class StreamTableJoin:
    """Enrich stream records against a mutable reference table.

    For each incoming :class:`StreamRecord`, the join extracts a lookup
    key and probes the :class:`TableSnapshot`.  If a match is found,
    the stream record's value dict is merged with the table row.
    Unmatched records are either dropped (inner join) or passed through
    with *None* enrichment fields (left join).

    Parameters
    ----------
    table : TableSnapshot
        The reference table to join against.
    key_extractor : JoinKeyExtractor
        Function that maps a stream record to a table lookup key.
    join_type : str
        ``"inner"`` (drop unmatched) or ``"left"`` (pass through).

    """

    def __init__(
        self,
        table: TableSnapshot,
        key_extractor: JoinKeyExtractor,
        join_type: str = "inner",
    ) -> None:
        if join_type not in ("inner", "left"):
            msg = f"join_type must be 'inner' or 'left', got '{join_type}'"
            raise ValueError(msg)
        self._table = table
        self._key_extractor = key_extractor
        self._join_type = join_type
        self._matched: int = 0
        self._unmatched: int = 0

    @property
    def matched(self) -> int:
        return self._matched

    @property
    def unmatched(self) -> int:
        return self._unmatched

    @property
    def table(self) -> TableSnapshot:
        return self._table

    def process(self, record: StreamRecord) -> StreamRecord | None:
        """Enrich a single stream record.

        Returns the enriched record, or *None* if the join is inner
        and no table match was found.
        """
        lookup_key = self._key_extractor(record)
        table_row = self._table.get(lookup_key)

        if table_row is None:
            self._unmatched += 1
            if self._join_type == "inner":
                return None
            # Left join: pass through with no enrichment
            return StreamRecord(
                key=record.key,
                value={**record.value, "__table_match__": False},
                event_time=record.event_time,
                processing_time=record.processing_time,
                record_type=record.record_type,
                record_id=record.record_id,
                source=record.source,
            )

        self._matched += 1
        merged = {**record.value, **table_row, "__table_match__": True}
        return StreamRecord(
            key=record.key,
            value=merged,
            event_time=record.event_time,
            processing_time=record.processing_time,
            record_type=record.record_type,
            record_id=record.record_id,
            source=record.source,
        )

    def process_batch(self, records: list[StreamRecord]) -> list[StreamRecord]:
        """Enrich a batch of records, filtering out inner-join misses.

        Optimized for throughput: caches table and extractor references
        locally and avoids per-record method-call overhead of
        :meth:`process`. Semantics are identical to calling
        ``process()`` on each record individually.
        """
        results: list[StreamRecord] = []
        # Cache attribute lookups for the tight loop
        rows = self._table._rows
        extract = self._key_extractor
        is_inner = self._join_type == "inner"
        matched = 0
        unmatched = 0

        for record in records:
            lookup_key = extract(record)
            table_row = rows.get(lookup_key)

            if table_row is None:
                unmatched += 1
                if is_inner:
                    continue
                results.append(
                    StreamRecord(
                        key=record.key,
                        value={**record.value, "__table_match__": False},
                        event_time=record.event_time,
                        processing_time=record.processing_time,
                        record_type=record.record_type,
                        record_id=record.record_id,
                        source=record.source,
                    ),
                )
            else:
                matched += 1
                results.append(
                    StreamRecord(
                        key=record.key,
                        value={
                            **record.value,
                            **table_row,
                            "__table_match__": True,
                        },
                        event_time=record.event_time,
                        processing_time=record.processing_time,
                        record_type=record.record_type,
                        record_id=record.record_id,
                        source=record.source,
                    ),
                )

        self._matched += matched
        self._unmatched += unmatched
        return results
