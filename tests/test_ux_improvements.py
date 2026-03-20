"""UX improvement tests for PyCypher.

Verifies:
1. Core classes are importable directly from the top-level `pycypher` package.
2. EntityTable.from_dataframe() provides a low-boilerplate factory.
3. Querying with unregistered entity / relationship types raises a friendly
   ValueError (not a raw KeyError).

TDD red phase.
"""

import pandas as pd
import pytest

# ─────────────────────────────────────────────────────────────────────────────
# 1. Top-level exports
# ─────────────────────────────────────────────────────────────────────────────


class TestTopLevelExports:
    def test_star_importable_from_pycypher(self) -> None:
        from pycypher import Star  # noqa: F401

    def test_context_importable_from_pycypher(self) -> None:
        from pycypher import Context  # noqa: F401

    def test_entity_table_importable_from_pycypher(self) -> None:
        from pycypher import EntityTable  # noqa: F401

    def test_relationship_table_importable_from_pycypher(self) -> None:
        from pycypher import RelationshipTable  # noqa: F401

    def test_id_column_importable_from_pycypher(self) -> None:
        from pycypher import ID_COLUMN  # noqa: F401

        assert ID_COLUMN == "__ID__"

    def test_context_builder_still_importable(self) -> None:
        from pycypher import ContextBuilder  # noqa: F401


# ─────────────────────────────────────────────────────────────────────────────
# 2. EntityTable.from_dataframe()
# ─────────────────────────────────────────────────────────────────────────────


class TestEntityTableFromDataframe:
    def _simple_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "__ID__": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            }
        )

    def test_from_dataframe_returns_entity_table(self) -> None:
        from pycypher import EntityTable

        df = self._simple_df()
        table = EntityTable.from_dataframe("Person", df)
        assert isinstance(table, EntityTable)

    def test_from_dataframe_entity_type_set(self) -> None:
        from pycypher import EntityTable

        table = EntityTable.from_dataframe("Person", self._simple_df())
        assert table.entity_type == "Person"

    def test_from_dataframe_attribute_map_inferred(self) -> None:
        from pycypher import EntityTable

        table = EntityTable.from_dataframe("Person", self._simple_df())
        assert "name" in table.attribute_map
        assert "age" in table.attribute_map
        # __ID__ should NOT be in attribute_map (it's the identity column)
        assert "__ID__" not in table.attribute_map

    def test_from_dataframe_source_obj_preserved(self) -> None:
        from pycypher import EntityTable

        df = self._simple_df()
        table = EntityTable.from_dataframe("Person", df)
        assert len(table.source_obj) == 3

    def test_from_dataframe_with_custom_id_col(self) -> None:
        """id_col='my_id' should rename that column to __ID__."""
        from pycypher import EntityTable

        df = pd.DataFrame({"my_id": [1, 2], "name": ["Alice", "Bob"]})
        table = EntityTable.from_dataframe("Person", df, id_col="my_id")
        assert "__ID__" in table.source_obj.columns
        assert "my_id" not in table.source_obj.columns

    def test_from_dataframe_missing_id_col_raises(self) -> None:
        """Specifying a non-existent id_col should raise ValueError."""
        from pycypher import EntityTable

        df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        with pytest.raises(ValueError, match="not found"):
            EntityTable.from_dataframe("Person", df, id_col="nonexistent")

    def test_from_dataframe_missing_id_column_raises(self) -> None:
        """If no id_col is given and __ID__ is not in the DataFrame, raise."""
        from pycypher import EntityTable

        df = pd.DataFrame({"name": ["Alice"], "age": [30]})
        with pytest.raises(ValueError, match="__ID__"):
            EntityTable.from_dataframe("Person", df)

    def test_from_dataframe_supports_full_query(self) -> None:
        """End-to-end: from_dataframe() table can power a real query."""
        from pycypher import Context, EntityTable, Star
        from pycypher.relational_models import (
            EntityMapping,
            RelationshipMapping,
        )

        df = pd.DataFrame(
            {
                "__ID__": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 25, 35],
            }
        )
        table = EntityTable.from_dataframe("Person", df)
        context = Context(
            entity_mapping=EntityMapping(mapping={"Person": table}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        star = Star(context=context)
        result = star.execute_query(
            "MATCH (p:Person) RETURN p.name, p.age ORDER BY p.age ASC LIMIT 1"
        )
        assert len(result) == 1
        assert result["name"].iloc[0] == "Bob"
        assert result["age"].iloc[0] == 25


# ─────────────────────────────────────────────────────────────────────────────
# 3. Friendly error messages for unregistered types
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def minimal_context() -> "Context":
    from pycypher import Context, EntityTable
    from pycypher.relational_models import EntityMapping, RelationshipMapping

    df = pd.DataFrame({"__ID__": [1], "name": ["Alice"]})
    table = EntityTable.from_dataframe("Person", df)
    return Context(
        entity_mapping=EntityMapping(mapping={"Person": table}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


class TestFriendlyErrors:
    def test_unregistered_entity_type_gives_friendly_error(
        self, minimal_context: "Context"
    ) -> None:
        """Querying an unknown entity type should raise ValueError naming the type."""
        from pycypher import Star

        star = Star(context=minimal_context)
        with pytest.raises(ValueError, match="Company"):
            star.execute_query("MATCH (c:Company) RETURN c.name")

    def test_friendly_error_mentions_available_types(
        self, minimal_context: "Context"
    ) -> None:
        """The error message for unknown entity should list known types."""
        from pycypher import Star

        star = Star(context=minimal_context)
        with pytest.raises(ValueError, match="Person"):
            star.execute_query("MATCH (c:Company) RETURN c.name")
