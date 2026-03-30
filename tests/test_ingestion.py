"""Tests for the Arrow/DuckDB ingestion layer.

Covers:
- arrow_utils normalisation functions
- DuckDBReader file and DataFrame readers
- EntityTable.from_arrow / RelationshipTable.from_arrow
- ContextBuilder fluent API
- End-to-end: ContextBuilder → Star.execute_query
"""

from __future__ import annotations

import csv
import os
import tempfile

import pandas as pd
import pyarrow as pa
import pytest
from pycypher.ingestion import ContextBuilder
from pycypher.ingestion.arrow_utils import (
    infer_attribute_map,
    normalize_entity_table,
    normalize_relationship_table,
)
from pycypher.ingestion.duckdb_reader import DuckDBReader
from pycypher.relational_models import (
    ID_COLUMN,
    EntityTable,
    RelationshipTable,
)

# ---------------------------------------------------------------------------
# TestArrowUtils
# ---------------------------------------------------------------------------


class TestArrowUtils:
    """Unit tests for arrow_utils normalisation helpers."""

    def test_normalize_entity_table_explicit_id_col(self) -> None:
        """Renaming an existing column to __ID__ works correctly."""
        table = pa.table(
            {"person_id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
        )
        result = normalize_entity_table(table, id_col="person_id")
        assert "__ID__" in result.schema.names
        assert "person_id" not in result.schema.names
        assert result.schema.names[0] == "__ID__"
        assert result.column("__ID__").to_pylist() == [1, 2, 3]

    def test_normalize_entity_table_auto_id(self) -> None:
        """Without id_col a sequential __ID__ column is prepended."""
        table = pa.table({"name": ["Alice", "Bob"], "age": [30, 40]})
        result = normalize_entity_table(table)
        assert result.schema.names[0] == "__ID__"
        assert result.column("__ID__").to_pylist() == [0, 1]

    def test_normalize_entity_table_missing_id_col_raises(self) -> None:
        """Specifying a non-existent id_col raises ValueError."""
        table = pa.table({"name": ["Alice"]})
        with pytest.raises(ValueError, match="id_col"):
            normalize_entity_table(table, id_col="nonexistent")

    def test_normalize_entity_table_existing_id_col_not_duplicated(
        self,
    ) -> None:
        """If __ID__ already exists no second ID column is prepended."""
        table = pa.table({"__ID__": [10, 20], "name": ["A", "B"]})
        result = normalize_entity_table(table)
        assert result.schema.names.count("__ID__") == 1

    def test_normalize_relationship_table_renames_source_target(self) -> None:
        """source_col and target_col are renamed to __SOURCE__ and __TARGET__."""
        table = pa.table(
            {"from_id": [1, 2], "to_id": [3, 4], "weight": [0.9, 0.5]},
        )
        result = normalize_relationship_table(
            table,
            source_col="from_id",
            target_col="to_id",
        )
        assert "__SOURCE__" in result.schema.names
        assert "__TARGET__" in result.schema.names
        assert "from_id" not in result.schema.names
        assert "to_id" not in result.schema.names

    def test_normalize_relationship_table_auto_id(self) -> None:
        """Without id_col a sequential __ID__ column is prepended."""
        table = pa.table({"from_id": [1], "to_id": [2]})
        result = normalize_relationship_table(
            table,
            source_col="from_id",
            target_col="to_id",
        )
        assert "__ID__" in result.schema.names

    def test_normalize_relationship_table_missing_source_raises(self) -> None:
        table = pa.table({"to_id": [1]})
        with pytest.raises(ValueError, match="source_col"):
            normalize_relationship_table(
                table,
                source_col="from_id",
                target_col="to_id",
            )

    def test_normalize_relationship_table_missing_target_raises(self) -> None:
        table = pa.table({"from_id": [1]})
        with pytest.raises(ValueError, match="target_col"):
            normalize_relationship_table(
                table,
                source_col="from_id",
                target_col="to_id",
            )

    def test_infer_attribute_map_excludes_reserved(self) -> None:
        """infer_attribute_map excludes __ID__, __SOURCE__, __TARGET__."""
        table = pa.table(
            {
                "__ID__": [1],
                "__SOURCE__": [2],
                "__TARGET__": [3],
                "name": ["Alice"],
                "age": [30],
            },
        )
        attr_map = infer_attribute_map(table)
        assert "__ID__" not in attr_map
        assert "__SOURCE__" not in attr_map
        assert "__TARGET__" not in attr_map
        assert attr_map == {"name": "name", "age": "age"}

    def test_infer_attribute_map_only_id(self) -> None:
        """Returns empty dict when only reserved columns are present."""
        table = pa.table({"__ID__": [1], "__SOURCE__": [2], "__TARGET__": [3]})
        assert infer_attribute_map(table) == {}


# ---------------------------------------------------------------------------
# TestDuckDBReader
# ---------------------------------------------------------------------------


class TestDuckDBReader:
    """Unit tests for DuckDBReader file and DataFrame loaders."""

    def test_from_csv(self) -> None:
        """Reads a temp CSV file and returns an Arrow table."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            newline="",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "age"])
            writer.writerow([1, "Alice", 30])
            writer.writerow([2, "Bob", 40])
            path = f.name
        try:
            result = DuckDBReader.from_csv(path)
            assert isinstance(result, pa.Table)
            assert set(result.schema.names) == {"id", "name", "age"}
            assert len(result) == 2
        finally:
            os.unlink(path)

    def test_from_parquet(self) -> None:
        """Reads a temp Parquet file and returns an Arrow table."""
        df = pd.DataFrame({"id": [1, 2], "value": [10.0, 20.0]})
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            path = f.name
        df.to_parquet(path, index=False)
        try:
            result = DuckDBReader.from_parquet(path)
            assert isinstance(result, pa.Table)
            assert set(result.schema.names) == {"id", "value"}
            assert len(result) == 2
        finally:
            os.unlink(path)

    def test_from_dataframe(self) -> None:
        """Converts a pandas DataFrame to an Arrow table via DuckDB."""
        df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        result = DuckDBReader.from_dataframe(df)
        assert isinstance(result, pa.Table)
        assert set(result.schema.names) == {"x", "y"}
        assert len(result) == 3

    def test_from_arrow(self) -> None:
        """from_arrow returns an Arrow table (passthrough)."""
        original = pa.table({"a": [1, 2], "b": [3, 4]})
        result = DuckDBReader.from_arrow(original)
        assert isinstance(result, pa.Table)
        assert set(result.schema.names) == {"a", "b"}
        assert len(result) == 2


# ---------------------------------------------------------------------------
# TestEntityTableFromArrow
# ---------------------------------------------------------------------------


class TestEntityTableFromArrow:
    """Tests for EntityTable.from_arrow and its to_pandas() with Arrow source."""

    def _make_arrow_entity(self) -> pa.Table:
        return normalize_entity_table(
            pa.table({"name": ["Alice", "Bob"], "age": [30, 40]}),
        )

    def test_from_arrow_produces_correct_attribute_map(self) -> None:
        """attribute_map contains all non-reserved columns."""
        table = self._make_arrow_entity()
        et = EntityTable.from_arrow("Person", table)
        assert et.attribute_map == {"name": "name", "age": "age"}
        assert et.source_obj_attribute_map == {"name": "name", "age": "age"}

    def test_from_arrow_entity_type_set(self) -> None:
        table = self._make_arrow_entity()
        et = EntityTable.from_arrow("Person", table)
        assert et.entity_type == "Person"

    def test_from_arrow_to_pandas_prefixes_columns(self) -> None:
        """to_pandas() with Arrow source_obj returns prefixed columns."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )

        table = self._make_arrow_entity()
        et = EntityTable.from_arrow("Person", table)
        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": et}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )
        df = et.to_pandas(ctx)
        assert "Person____ID__" in df.columns
        assert "Person__name" in df.columns
        assert "Person__age" in df.columns

    def test_from_arrow_to_pandas_matches_pandas_path(self) -> None:
        """Arrow and pandas paths produce identical prefixed DataFrames."""
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )

        # pandas path
        pd_df = pd.DataFrame(
            {ID_COLUMN: [0, 1], "name": ["Alice", "Bob"], "age": [30, 40]},
        )
        et_pd = EntityTable(
            entity_type="Person",
            column_names=[ID_COLUMN, "name", "age"],
            source_obj=pd_df,
            attribute_map={"name": "name", "age": "age"},
            source_obj_attribute_map={"name": "name", "age": "age"},
        )

        # arrow path
        arrow_table = normalize_entity_table(
            pa.table({"name": ["Alice", "Bob"], "age": [30, 40]}),
        )
        et_arrow = EntityTable.from_arrow("Person", arrow_table)

        ctx = Context(
            entity_mapping=EntityMapping(mapping={"Person": et_pd}),
            relationship_mapping=RelationshipMapping(mapping={}),
        )

        df_pd = et_pd.to_pandas(ctx)
        df_arrow = et_arrow.to_pandas(ctx)

        pd.testing.assert_frame_equal(
            df_pd.reset_index(drop=True),
            df_arrow.reset_index(drop=True),
        )


# ---------------------------------------------------------------------------
# TestRelationshipTableFromArrow
# ---------------------------------------------------------------------------


class TestRelationshipTableFromArrow:
    """Tests for RelationshipTable.from_arrow."""

    def _make_arrow_rel(self) -> pa.Table:
        raw = pa.table(
            {"from_id": [1, 2], "to_id": [2, 3], "weight": [0.9, 0.5]},
        )
        return normalize_relationship_table(
            raw,
            source_col="from_id",
            target_col="to_id",
        )

    def test_from_arrow_attribute_map(self) -> None:
        table = self._make_arrow_rel()
        rt = RelationshipTable.from_arrow("KNOWS", table)
        assert rt.attribute_map == {"weight": "weight"}

    def test_from_arrow_to_pandas_prefixes_columns(self) -> None:
        from pycypher.relational_models import (
            Context,
            EntityMapping,
            RelationshipMapping,
        )

        table = self._make_arrow_rel()
        rt = RelationshipTable.from_arrow("KNOWS", table)
        ctx = Context(
            entity_mapping=EntityMapping(mapping={}),
            relationship_mapping=RelationshipMapping(mapping={"KNOWS": rt}),
        )
        df = rt.to_pandas(ctx)
        assert "KNOWS____ID__" in df.columns
        assert "KNOWS____SOURCE__" in df.columns
        assert "KNOWS____TARGET__" in df.columns
        assert "KNOWS__weight" in df.columns


# ---------------------------------------------------------------------------
# TestContextBuilder
# ---------------------------------------------------------------------------


class TestContextBuilder:
    """Tests for the ContextBuilder fluent API."""

    def test_add_entity_from_dataframe(self) -> None:
        """add_entity with a pandas DataFrame builds the correct EntityMapping."""
        df = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
        ctx = ContextBuilder().add_entity("Person", df, id_col="__ID__").build()
        assert "Person" in ctx.entity_mapping.mapping
        et = ctx.entity_mapping.mapping["Person"]
        assert et.entity_type == "Person"

    def test_add_entity_from_arrow(self) -> None:
        """add_entity with a pa.Table builds the correct EntityMapping."""
        table = pa.table({"person_id": [1, 2], "name": ["Alice", "Bob"]})
        ctx = ContextBuilder().add_entity("Person", table, id_col="person_id").build()
        assert "Person" in ctx.entity_mapping.mapping

    def test_add_entity_from_csv(self) -> None:
        """add_entity with a CSV file path builds the correct EntityMapping."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            newline="",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["person_id", "name"])
            writer.writerow([1, "Alice"])
            path = f.name
        try:
            ctx = (
                ContextBuilder().add_entity("Person", path, id_col="person_id").build()
            )
            assert "Person" in ctx.entity_mapping.mapping
        finally:
            os.unlink(path)

    def test_add_relationship_from_dataframe(self) -> None:
        """add_relationship with a pandas DataFrame builds RelationshipMapping."""
        df = pd.DataFrame({"src": [1], "tgt": [2]})
        ctx = (
            ContextBuilder()
            .add_relationship("KNOWS", df, source_col="src", target_col="tgt")
            .build()
        )
        assert "KNOWS" in ctx.relationship_mapping.mapping
        rt = ctx.relationship_mapping.mapping["KNOWS"]
        assert rt.relationship_type == "KNOWS"

    def test_build_returns_context(self) -> None:
        """build() returns a Context instance."""
        from pycypher.relational_models import Context

        ctx = ContextBuilder().build()
        assert isinstance(ctx, Context)

    def test_source_dispatch_unknown_extension_raises(self) -> None:
        """Unknown file extension raises ValueError."""
        with pytest.raises(ValueError, match="extension"):
            ContextBuilder().add_entity("Person", "/some/file.xyz")

    def test_source_dispatch_wrong_type_raises(self) -> None:
        """Non-string/DataFrame/Table source raises TypeError."""
        with pytest.raises(TypeError, match="str, pd.DataFrame, or pa.Table"):
            ContextBuilder().add_entity("Person", 42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TestEndToEnd
# ---------------------------------------------------------------------------


class TestEndToEnd:
    """End-to-end: ContextBuilder → Star.execute_query returns correct results."""

    def test_match_return_from_csv(self) -> None:
        """Load a CSV, run a simple MATCH query, verify result."""
        from pycypher.star import Star

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".csv",
            delete=False,
            newline="",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(["person_id", "name", "age"])
            writer.writerow([1, "Alice", 30])
            writer.writerow([2, "Bob", 40])
            writer.writerow([3, "Carol", 25])
            path = f.name
        try:
            ctx = (
                ContextBuilder().add_entity("Person", path, id_col="person_id").build()
            )
            star = Star(context=ctx)
            result = star.execute_query(
                "MATCH (p:Person) WITH p.name AS name RETURN name AS name",
            )
            assert isinstance(result, pd.DataFrame)
            names = set(result["name"].tolist())
            assert names == {"Alice", "Bob", "Carol"}
        finally:
            os.unlink(path)

    def test_match_return_from_dataframe(self) -> None:
        """Load a DataFrame, run MATCH with WHERE, verify filtered result."""
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                "__ID__": [1, 2, 3],
                "name": ["Alice", "Bob", "Carol"],
                "age": [30, 40, 25],
            },
        )
        ctx = ContextBuilder().add_entity("Person", df).build()
        star = Star(context=ctx)
        result = star.execute_query(
            "MATCH (p:Person) WITH p.name AS name, p.age AS age RETURN name AS name, age AS age",
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert set(result["name"].tolist()) == {"Alice", "Bob", "Carol"}
