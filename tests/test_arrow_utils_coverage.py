"""Coverage-gap tests for pycypher.ingestion.arrow_utils.

Targets uncovered paths:
- normalize_entity_table: ID column reorder when id_col is not the first column (lines 47-49)
- normalize_relationship_table: explicit id_col handling (lines 100-108)
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pycypher.ingestion.arrow_utils import (
    infer_attribute_map,
    normalize_entity_table,
    normalize_relationship_table,
)


class TestNormalizeEntityTableIdReorder:
    """Cover the branch where id_col is not the first column."""

    def test_id_col_not_first_is_reordered(self) -> None:
        """When id_col is not position 0, __ID__ is moved to front."""
        table = pa.table({"name": ["Alice", "Bob"], "uid": [10, 20]})
        result = normalize_entity_table(table, id_col="uid")
        assert result.schema.names[0] == "__ID__"
        assert result.column("__ID__").to_pylist() == [10, 20]
        assert "name" in result.schema.names

    def test_id_col_already_first_stays(self) -> None:
        """When id_col is already position 0, no reorder needed."""
        table = pa.table({"uid": [10, 20], "name": ["Alice", "Bob"]})
        result = normalize_entity_table(table, id_col="uid")
        assert result.schema.names[0] == "__ID__"

    def test_id_col_missing_raises(self) -> None:
        """ValueError when id_col doesn't exist."""
        table = pa.table({"name": ["Alice"]})
        with pytest.raises(ValueError, match="id_col.*not found"):
            normalize_entity_table(table, id_col="nonexistent")


class TestNormalizeEntityTableDedup:
    """Cover __ID__ dedup performed when id_col is provided."""

    def test_drops_duplicate_ids_keeps_first(self) -> None:
        table = pa.table(
            {
                "uid": [1, 2, 1, 3, 2],
                "name": ["a1", "b1", "a2", "c1", "b2"],
            },
        )
        result = normalize_entity_table(table, id_col="uid")
        # 5 input rows, 3 unique IDs → 3 output rows.
        assert result.num_rows == 3
        # First-occurrence wins, original order preserved.
        assert result.column("__ID__").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["a1", "b1", "c1"]

    def test_no_duplicates_unchanged(self) -> None:
        table = pa.table({"uid": [10, 20, 30], "name": ["a", "b", "c"]})
        result = normalize_entity_table(table, id_col="uid")
        assert result.num_rows == 3
        assert result.column("__ID__").to_pylist() == [10, 20, 30]

    def test_string_id_dedup(self) -> None:
        table = pa.table(
            {
                "fips": ["01", "02", "01", "03"],
                "name": ["AL-1", "AK", "AL-2", "AZ"],
            },
        )
        result = normalize_entity_table(table, id_col="fips")
        assert result.num_rows == 3
        assert result.column("__ID__").to_pylist() == ["01", "02", "03"]
        assert result.column("name").to_pylist() == ["AL-1", "AK", "AZ"]

    def test_auto_generated_ids_not_deduped(self) -> None:
        # When id_col is None, __ID__ is sequential; dedup must not run.
        table = pa.table({"name": ["a", "b", "c"]})
        result = normalize_entity_table(table)
        assert result.num_rows == 3
        assert result.column("__ID__").to_pylist() == [0, 1, 2]


class TestNormalizeRelationshipTableDedup:
    """Cover endpoint dedup and __ID__ dedup for relationships."""

    def test_default_collapses_duplicate_endpoints(self) -> None:
        # 4 input rows, 2 unique (src, tgt) pairs → 2 output rows.
        table = pa.table(
            {
                "src": [1, 2, 1, 2],
                "tgt": [3, 4, 3, 4],
                "weight": [0.1, 0.2, 0.3, 0.4],
            },
        )
        result = normalize_relationship_table(
            table,
            source_col="src",
            target_col="tgt",
        )
        assert result.num_rows == 2
        assert result.column("__SOURCE__").to_pylist() == [1, 2]
        assert result.column("__TARGET__").to_pylist() == [3, 4]
        # First-occurrence wins for attributes.
        assert result.column("weight").to_pylist() == [0.1, 0.2]
        # Auto __ID__s are contiguous and assigned after dedup.
        assert result.column("__ID__").to_pylist() == [0, 1]

    def test_allow_multi_edges_preserves_parallel(self) -> None:
        table = pa.table(
            {
                "src": [1, 2, 1, 2],
                "tgt": [3, 4, 3, 4],
            },
        )
        result = normalize_relationship_table(
            table,
            source_col="src",
            target_col="tgt",
            allow_multi_edges=True,
        )
        assert result.num_rows == 4
        # Auto __ID__s are still added, one per row.
        assert result.column("__ID__").to_pylist() == [0, 1, 2, 3]

    def test_id_col_uniqueness_enforced_independently_of_multi_edges(
        self,
    ) -> None:
        # Two rows share rel_id=100 but have distinct endpoints — invalid
        # regardless of multi-edge policy. ID dedup keeps the first.
        table = pa.table(
            {
                "src": [1, 2, 3],
                "tgt": [10, 20, 30],
                "rel_id": [100, 100, 200],
            },
        )
        result = normalize_relationship_table(
            table,
            source_col="src",
            target_col="tgt",
            id_col="rel_id",
            allow_multi_edges=True,
        )
        assert result.num_rows == 2
        assert result.column("__ID__").to_pylist() == [100, 200]
        # First (1, 10) wins over (2, 20) because both had rel_id=100.
        assert result.column("__SOURCE__").to_pylist() == [1, 3]
        assert result.column("__TARGET__").to_pylist() == [10, 30]

    def test_id_col_dedup_then_endpoint_dedup(self) -> None:
        # rel_id=100 row dropped first (same as id=99), then endpoints (X→Y)
        # collapse the remaining duplicate-endpoint row.
        table = pa.table(
            {
                "src": ["X", "X", "X", "Z"],
                "tgt": ["Y", "Y", "Y", "W"],
                "rel_id": [100, 100, 200, 300],
            },
        )
        result = normalize_relationship_table(
            table,
            source_col="src",
            target_col="tgt",
            id_col="rel_id",
        )
        # After ID dedup: 3 rows. After endpoint dedup: 2 rows (X→Y, Z→W).
        assert result.num_rows == 2
        assert result.column("__SOURCE__").to_pylist() == ["X", "Z"]
        assert result.column("__TARGET__").to_pylist() == ["Y", "W"]


class TestNormalizeRelationshipTableIdCol:
    """Cover the explicit id_col path in normalize_relationship_table."""

    def test_explicit_id_col(self) -> None:
        """When id_col is provided, it gets renamed to __ID__."""
        table = pa.table(
            {
                "src": [1, 2],
                "tgt": [3, 4],
                "rel_id": [100, 200],
                "weight": [0.5, 0.8],
            },
        )
        result = normalize_relationship_table(
            table,
            source_col="src",
            target_col="tgt",
            id_col="rel_id",
        )
        assert "__ID__" in result.schema.names
        assert "__SOURCE__" in result.schema.names
        assert "__TARGET__" in result.schema.names
        assert result.column("__ID__").to_pylist() == [100, 200]

    def test_id_col_missing_in_rel_raises(self) -> None:
        """ValueError when id_col doesn't exist in relationship table."""
        table = pa.table({"src": [1], "tgt": [2]})
        with pytest.raises(ValueError, match="id_col.*not found"):
            normalize_relationship_table(
                table,
                source_col="src",
                target_col="tgt",
                id_col="bad",
            )

    def test_source_col_missing_raises(self) -> None:
        """ValueError when source_col doesn't exist."""
        table = pa.table({"tgt": [1]})
        with pytest.raises(ValueError, match="source_col.*not found"):
            normalize_relationship_table(
                table,
                source_col="src",
                target_col="tgt",
            )

    def test_target_col_missing_raises(self) -> None:
        """ValueError when target_col doesn't exist."""
        table = pa.table({"src": [1]})
        with pytest.raises(ValueError, match="target_col.*not found"):
            normalize_relationship_table(
                table,
                source_col="src",
                target_col="tgt",
            )


class TestInferAttributeMap:
    """Cover infer_attribute_map with reserved and non-reserved columns."""

    def test_excludes_reserved_columns(self) -> None:
        """Reserved columns (__ID__, __SOURCE__, __TARGET__) are excluded."""
        table = pa.table(
            {
                "__ID__": [1],
                "__SOURCE__": [2],
                "__TARGET__": [3],
                "name": ["Alice"],
                "age": [30],
            },
        )
        result = infer_attribute_map(table)
        assert result == {"name": "name", "age": "age"}

    def test_empty_when_only_reserved(self) -> None:
        """Empty map when table has only reserved columns."""
        table = pa.table({"__ID__": [1]})
        result = infer_attribute_map(table)
        assert result == {}
