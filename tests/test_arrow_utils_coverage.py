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
