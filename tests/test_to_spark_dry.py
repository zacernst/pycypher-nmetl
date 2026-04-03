"""TDD tests for the to_spark() DRY fix in relational_models.py.

Loop 217 — Design Pattern: DRY violation

EntityTable.to_spark() (lines 597-616) and RelationshipTable.to_spark()
(lines 683-702) are identical except for the type-name variable:

  EntityTable     uses: self.entity_type
  RelationshipTable uses: self.relationship_type

Both contain the same 20-line structure:
  1. try/except ImportError guard for pyspark
  2. SparkSession.builder.appName(...).getOrCreate()
  3. if hasattr(self.source_obj, 'toPandas'): rename columns with prefix
  4. else: pandas_df = self.to_pandas(); spark.createDataFrame(pandas_df)

The fix is to extract a shared helper method
  `Relation._to_spark_with_prefix(context, type_name, source_obj)`
onto the base Relation class, and have both subclasses delegate to it.

Red-phase tests that fail before the fix:
  - test_relation_has_to_spark_with_prefix_helper (AttributeError)
  - test_entity_table_delegates_to_helper (helper not yet called)
  - test_relationship_table_delegates_to_helper (helper not yet called)

Green-phase tests (verify behaviour is preserved after fix):
  - test_helper_detects_native_pyspark_df_and_renames_columns
  - test_helper_falls_back_to_pandas_when_not_native_spark
  - test_entity_table_to_spark_uses_entity_type_as_prefix
  - test_relationship_table_to_spark_uses_relationship_type_as_prefix
  - test_missing_pyspark_raises_import_error
  - test_base_relation_to_spark_uses_class_name_as_prefix (base fallback)
"""

from __future__ import annotations

import importlib.util
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pycypher.relational_models import (
    Context,
    EntityMapping,
    EntityTable,
    Relation,
    RelationshipMapping,
    RelationshipTable,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.spark


def _make_context() -> Context:
    """Minimal empty context sufficient for to_pandas() calls."""
    return Context(
        entity_mapping=EntityMapping(mapping={}),
        relationship_mapping=RelationshipMapping(mapping={}),
    )


def _make_entity_table() -> EntityTable:
    df = pd.DataFrame({"__ID__": [1, 2], "name": ["Alice", "Bob"]})
    return EntityTable.from_dataframe("Person", df)


def _make_relationship_table() -> RelationshipTable:
    df = pd.DataFrame(
        {
            "__ID__": [10],
            "__SOURCE__": [1],
            "__TARGET__": [2],
            "since": [2020],
        },
    )
    return RelationshipTable(
        relationship_type="KNOWS",
        source_obj=df,
        attribute_map={
            "__ID__": "__ID__",
            "__SOURCE__": "__SOURCE__",
            "__TARGET__": "__TARGET__",
            "since": "since",
        },
        source_obj_attribute_map={
            "__ID__": "__ID__",
            "__SOURCE__": "__SOURCE__",
            "__TARGET__": "__TARGET__",
            "since": "since",
        },
        column_names=["__ID__", "__SOURCE__", "__TARGET__", "since"],
    )


# ---------------------------------------------------------------------------
# Category 1 — Red phase: helper must exist on the base class
# ---------------------------------------------------------------------------


class TestHelperExistsOnBaseClass:
    def test_relation_has_to_spark_with_prefix_helper(self) -> None:
        """Relation base class must expose _to_spark_with_prefix helper.

        This is the primary red-phase test: it fails with AttributeError before
        the helper is extracted, and passes after.
        """
        assert hasattr(Relation, "_to_spark_with_prefix"), (
            "Relation._to_spark_with_prefix not found. "
            "The shared helper method must be extracted to the base class."
        )

    def test_to_spark_with_prefix_is_callable(self) -> None:
        """The helper must be callable."""
        assert callable(getattr(Relation, "_to_spark_with_prefix", None))


# ---------------------------------------------------------------------------
# Category 2 — Delegation: subclasses must call the shared helper
# ---------------------------------------------------------------------------


class TestSubclassesDelegateToHelper:
    def test_entity_table_delegates_to_helper(self) -> None:
        """EntityTable.to_spark() must call self._to_spark_with_prefix."""
        table = _make_entity_table()
        ctx = _make_context()

        mock_result = MagicMock(name="spark_df")

        with patch.object(
            table,
            "_to_spark_with_prefix",
            return_value=mock_result,
        ) as mock_helper:
            result = table.to_spark(ctx)

        mock_helper.assert_called_once()
        assert result is mock_result

    def test_entity_table_passes_entity_type_to_helper(self) -> None:
        """EntityTable.to_spark() must pass self.entity_type to the helper."""
        table = _make_entity_table()
        ctx = _make_context()

        with patch.object(
            table,
            "_to_spark_with_prefix",
            return_value=MagicMock(),
        ) as mock_helper:
            table.to_spark(ctx)

        args, kwargs = mock_helper.call_args
        # type_name must be 'Person'
        assert "Person" in args or kwargs.get("type_name") == "Person", (
            f"Expected 'Person' as type_name, got args={args!r} kwargs={kwargs!r}"
        )

    def test_relationship_table_delegates_to_helper(self) -> None:
        """RelationshipTable.to_spark() must call self._to_spark_with_prefix."""
        table = _make_relationship_table()
        ctx = _make_context()

        mock_result = MagicMock(name="spark_df")

        with patch.object(
            table,
            "_to_spark_with_prefix",
            return_value=mock_result,
        ) as mock_helper:
            result = table.to_spark(ctx)

        mock_helper.assert_called_once()
        assert result is mock_result

    def test_relationship_table_passes_relationship_type_to_helper(
        self,
    ) -> None:
        """RelationshipTable.to_spark() must pass self.relationship_type to the helper."""
        table = _make_relationship_table()
        ctx = _make_context()

        with patch.object(
            table,
            "_to_spark_with_prefix",
            return_value=MagicMock(),
        ) as mock_helper:
            table.to_spark(ctx)

        args, kwargs = mock_helper.call_args
        assert "KNOWS" in args or kwargs.get("type_name") == "KNOWS", (
            f"Expected 'KNOWS' as type_name, got args={args!r} kwargs={kwargs!r}"
        )


# ---------------------------------------------------------------------------
# Category 3 — Behavior: helper correctly handles native PySpark DataFrames
# ---------------------------------------------------------------------------


_pyspark_available = importlib.util.find_spec("pyspark") is not None


@pytest.mark.skipif(not _pyspark_available, reason="pyspark not installed")
class TestHelperNativeSparkPath:
    def test_helper_renames_columns_on_native_pyspark_df(self) -> None:
        """When source_obj has toPandas, columns are renamed with type prefix."""
        table = _make_entity_table()
        ctx = _make_context()

        # Build a mock PySpark-like DataFrame
        mock_spark_df = MagicMock()
        mock_spark_df.columns = ["__ID__", "name"]

        # Each withColumnRenamed call returns the same mock (fluent API)
        renamed_df = MagicMock()
        renamed_df.columns = ["Person____ID__", "Person__name"]
        mock_spark_df.withColumnRenamed.return_value = renamed_df
        renamed_df.withColumnRenamed.return_value = renamed_df

        mock_spark_session = MagicMock()

        with patch("pyspark.sql.SparkSession") as mock_spark_class:
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = mock_spark_session
            result = table._to_spark_with_prefix(ctx, "Person", mock_spark_df)

        # withColumnRenamed must have been called for each column
        assert mock_spark_df.withColumnRenamed.called, (
            "withColumnRenamed should be called to add type prefix to columns"
        )

    def test_helper_does_not_call_to_pandas_for_native_spark(self) -> None:
        """When source_obj is already a PySpark DF, to_pandas() is not called."""
        table = _make_entity_table()
        ctx = _make_context()

        mock_spark_df = MagicMock()
        mock_spark_df.columns = ["__ID__"]
        mock_spark_df.withColumnRenamed.return_value = mock_spark_df

        # Pydantic blocks setattr on instances; patch at the class level instead.
        with (
            patch.object(EntityTable, "to_pandas") as mock_to_pandas,
            patch("pyspark.sql.SparkSession") as mock_spark_class,
        ):
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = MagicMock()
            table._to_spark_with_prefix(ctx, "Person", mock_spark_df)

        mock_to_pandas.assert_not_called()


# ---------------------------------------------------------------------------
# Category 4 — Behavior: helper falls back to pandas path
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _pyspark_available, reason="pyspark not installed")
class TestHelperPandasFallbackPath:
    def test_helper_calls_to_pandas_when_source_is_not_spark(self) -> None:
        """When source_obj is a pandas DF, helper converts via to_pandas()."""
        table = _make_entity_table()
        ctx = _make_context()

        pandas_df = pd.DataFrame(
            {"Person____ID__": [1, 2], "Person__name": ["Alice", "Bob"]},
        )

        mock_spark_df = MagicMock()
        mock_spark_session = MagicMock()
        mock_spark_session.createDataFrame.return_value = mock_spark_df

        # Patch at the class level to avoid Pydantic's __setattr__ guard.
        with (
            patch.object(
                EntityTable,
                "to_pandas",
                return_value=pandas_df,
            ) as mock_to_pandas,
            patch("pyspark.sql.SparkSession") as mock_spark_class,
        ):
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = mock_spark_session
            result = table._to_spark_with_prefix(ctx, "Person", None)

        mock_to_pandas.assert_called_once()
        mock_spark_session.createDataFrame.assert_called_once_with(pandas_df)

    def test_helper_returns_spark_df_from_create_dataframe(self) -> None:
        """Helper returns the result of spark.createDataFrame()."""
        table = _make_entity_table()
        ctx = _make_context()

        expected = MagicMock(name="expected_spark_df")
        mock_spark_session = MagicMock()
        mock_spark_session.createDataFrame.return_value = expected

        with (
            patch.object(
                EntityTable,
                "to_pandas",
                return_value=pd.DataFrame({"x": [1]}),
            ),
            patch("pyspark.sql.SparkSession") as mock_spark_class,
        ):
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = mock_spark_session
            result = table._to_spark_with_prefix(ctx, "Person", None)

        assert result is expected


# ---------------------------------------------------------------------------
# Category 5 — Error guard: ImportError when pyspark is absent
# ---------------------------------------------------------------------------


class TestHelperImportError:
    def test_helper_raises_import_error_when_pyspark_not_available(
        self,
    ) -> None:
        """_to_spark_with_prefix raises ImportError when pyspark cannot be imported."""
        table = _make_entity_table()
        ctx = _make_context()

        with patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
            with pytest.raises(ImportError, match="PySpark"):
                table._to_spark_with_prefix(ctx, "Person", None)

    def test_entity_table_to_spark_raises_import_error_without_pyspark(
        self,
    ) -> None:
        """EntityTable.to_spark() raises ImportError end-to-end when pyspark absent."""
        table = _make_entity_table()
        ctx = _make_context()

        with patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
            with pytest.raises(ImportError, match="PySpark"):
                table.to_spark(ctx)

    def test_relationship_table_to_spark_raises_import_error_without_pyspark(
        self,
    ) -> None:
        """RelationshipTable.to_spark() raises ImportError end-to-end when pyspark absent."""
        table = _make_relationship_table()
        ctx = _make_context()

        with patch.dict(sys.modules, {"pyspark": None, "pyspark.sql": None}):
            with pytest.raises(ImportError, match="PySpark"):
                table.to_spark(ctx)


# ---------------------------------------------------------------------------
# Category 6 — Column prefix correctness (integration-style)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _pyspark_available, reason="pyspark not installed")
class TestColumnPrefixCorrectness:
    def test_entity_table_pandas_path_produces_prefixed_columns(self) -> None:
        """EntityTable.to_spark() pandas path should produce prefixed columns in spark_df."""
        table = _make_entity_table()
        ctx = _make_context()

        captured_df: list[pd.DataFrame] = []

        mock_spark_session = MagicMock()

        def capture_create(df: pd.DataFrame) -> MagicMock:
            captured_df.append(df)
            return MagicMock(name="spark_df")

        mock_spark_session.createDataFrame.side_effect = capture_create

        with patch("pyspark.sql.SparkSession") as mock_spark_class:
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = mock_spark_session
            table.to_spark(ctx)

        assert len(captured_df) == 1
        df = captured_df[0]
        # to_pandas() prefixes with entity_type; ensure the prefix is there
        assert any("Person" in col for col in df.columns), (
            f"Expected 'Person__' prefix in columns, got: {list(df.columns)}"
        )

    def test_relationship_table_pandas_path_produces_prefixed_columns(
        self,
    ) -> None:
        """RelationshipTable.to_spark() pandas path should produce prefixed columns."""
        table = _make_relationship_table()
        ctx = _make_context()

        captured_df: list[pd.DataFrame] = []

        mock_spark_session = MagicMock()

        def capture_create(df: pd.DataFrame) -> MagicMock:
            captured_df.append(df)
            return MagicMock(name="spark_df")

        mock_spark_session.createDataFrame.side_effect = capture_create

        with patch("pyspark.sql.SparkSession") as mock_spark_class:
            mock_spark_class.builder.appName.return_value.getOrCreate.return_value = mock_spark_session
            table.to_spark(ctx)

        assert len(captured_df) == 1
        df = captured_df[0]
        assert any("KNOWS" in col for col in df.columns), (
            f"Expected 'KNOWS__' prefix in columns, got: {list(df.columns)}"
        )
