"""Tests for RelationshipTable.from_dataframe() factory method.

Mirrors the EntityTable.from_dataframe() pattern to ensure API symmetry
and consistent low-boilerplate relationship table creation.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pycypher.constants import (
    ID_COLUMN,
    RELATIONSHIP_SOURCE_COLUMN,
    RELATIONSHIP_TARGET_COLUMN,
)
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    RelationshipMapping,
    RelationshipTable,
)
from pycypher.star import Star


class TestFromDataframeBasic:
    """Basic construction with canonical column names."""

    def test_minimal_construction(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1, 2],
                RELATIONSHIP_SOURCE_COLUMN: [10, 20],
                RELATIONSHIP_TARGET_COLUMN: [30, 40],
            },
        )
        table = RelationshipTable.from_dataframe("KNOWS", df)
        assert table.relationship_type == "KNOWS"
        assert table.source_obj is not None
        assert len(table.source_obj) == 2

    def test_with_attributes(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
                "since": [2020],
                "weight": [0.5],
            },
        )
        table = RelationshipTable.from_dataframe("KNOWS", df)
        assert "since" in table.attribute_map
        assert "weight" in table.attribute_map
        assert ID_COLUMN not in table.attribute_map
        assert RELATIONSHIP_SOURCE_COLUMN not in table.attribute_map
        assert RELATIONSHIP_TARGET_COLUMN not in table.attribute_map

    def test_column_names_set(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        table = RelationshipTable.from_dataframe("WORKS_AT", df)
        assert ID_COLUMN in table.column_names
        assert RELATIONSHIP_SOURCE_COLUMN in table.column_names
        assert RELATIONSHIP_TARGET_COLUMN in table.column_names


class TestFromDataframeCustomColumns:
    """Construction with custom column name remapping."""

    def test_custom_id_col(self) -> None:
        df = pd.DataFrame(
            {
                "rel_id": [1, 2],
                RELATIONSHIP_SOURCE_COLUMN: [10, 20],
                RELATIONSHIP_TARGET_COLUMN: [30, 40],
            },
        )
        table = RelationshipTable.from_dataframe("KNOWS", df, id_col="rel_id")
        assert ID_COLUMN in table.source_obj.columns

    def test_custom_source_col(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                "from_node": [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        table = RelationshipTable.from_dataframe(
            "KNOWS",
            df,
            source_col="from_node",
        )
        assert RELATIONSHIP_SOURCE_COLUMN in table.source_obj.columns

    def test_custom_target_col(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                "to_node": [20],
            },
        )
        table = RelationshipTable.from_dataframe(
            "KNOWS",
            df,
            target_col="to_node",
        )
        assert RELATIONSHIP_TARGET_COLUMN in table.source_obj.columns

    def test_all_custom_columns(self) -> None:
        df = pd.DataFrame(
            {
                "rid": [1],
                "src": [10],
                "dst": [20],
                "weight": [0.9],
            },
        )
        table = RelationshipTable.from_dataframe(
            "KNOWS",
            df,
            id_col="rid",
            source_col="src",
            target_col="dst",
        )
        assert ID_COLUMN in table.source_obj.columns
        assert RELATIONSHIP_SOURCE_COLUMN in table.source_obj.columns
        assert RELATIONSHIP_TARGET_COLUMN in table.source_obj.columns
        assert "weight" in table.attribute_map


class TestFromDataframeErrors:
    """Validation errors for missing or invalid columns."""

    def test_missing_id_column(self) -> None:
        df = pd.DataFrame(
            {
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        with pytest.raises(ValueError, match="__ID__"):
            RelationshipTable.from_dataframe("KNOWS", df)

    def test_missing_source_column(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        with pytest.raises(ValueError, match="__SOURCE__"):
            RelationshipTable.from_dataframe("KNOWS", df)

    def test_missing_target_column(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
            },
        )
        with pytest.raises(ValueError, match="__TARGET__"):
            RelationshipTable.from_dataframe("KNOWS", df)

    def test_bad_id_col_name(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        with pytest.raises(ValueError, match="nonexistent"):
            RelationshipTable.from_dataframe("KNOWS", df, id_col="nonexistent")

    def test_bad_source_col_name(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        with pytest.raises(ValueError, match="nonexistent"):
            RelationshipTable.from_dataframe(
                "KNOWS",
                df,
                source_col="nonexistent",
            )

    def test_bad_target_col_name(self) -> None:
        df = pd.DataFrame(
            {
                ID_COLUMN: [1],
                RELATIONSHIP_SOURCE_COLUMN: [10],
                RELATIONSHIP_TARGET_COLUMN: [20],
            },
        )
        with pytest.raises(ValueError, match="nonexistent"):
            RelationshipTable.from_dataframe(
                "KNOWS",
                df,
                target_col="nonexistent",
            )


class TestFromDataframeIntegration:
    """End-to-end integration with Star query execution."""

    def test_query_with_from_dataframe_relationship(self) -> None:
        people = pd.DataFrame(
            {
                ID_COLUMN: [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
            },
        )
        knows = pd.DataFrame(
            {
                ID_COLUMN: [100, 101],
                RELATIONSHIP_SOURCE_COLUMN: [1, 2],
                RELATIONSHIP_TARGET_COLUMN: [2, 3],
            },
        )

        entity_table = EntityTable.from_dataframe("Person", people)
        rel_table = RelationshipTable.from_dataframe("KNOWS", knows)

        star = Star(
            context=Context(
                entity_mapping=EntityMapping(
                    mapping={"Person": entity_table},
                ),
                relationship_mapping=RelationshipMapping(
                    mapping={"KNOWS": rel_table},
                ),
            ),
        )
        result = star.execute_query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) "
            "RETURN a.name, b.name ORDER BY a.name",
        )
        assert len(result) == 2
        assert list(result["a.name"]) == ["Alice", "Bob"]
        assert list(result["b.name"]) == ["Bob", "Charlie"]

    def test_api_symmetry_with_entity_table(self) -> None:
        """from_dataframe() signature mirrors EntityTable.from_dataframe()."""
        assert hasattr(RelationshipTable, "from_dataframe")
        assert hasattr(EntityTable, "from_dataframe")
        # Both are classmethods
        assert isinstance(
            RelationshipTable.__dict__["from_dataframe"],
            classmethod,
        )

    def test_from_dataframe_in_public_api(self) -> None:
        """RelationshipTable is already in pycypher.__all__."""
        import pycypher

        assert "RelationshipTable" in pycypher.__all__
