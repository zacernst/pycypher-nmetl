"""Unit tests for pycypher.sinks.neo4j.

All tests here are fully self-contained: the Neo4j driver is mocked so no
live database is required.  The ``@pytest.mark.unit`` marker is applied to
the whole module; run with::

    uv run pytest tests/test_neo4j_sink.py -m unit -v
"""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from pycypher.sinks.neo4j import (
    Neo4jSink,
    NodeMapping,
    RelationshipMapping,
    _build_node_rows,
    _build_rel_rows,
    _coerce_row,
    _coerce_value,
    _drop_nulls,
    _node_merge_cypher,
    _rel_merge_cypher,
    _validate_columns,
)

pytestmark = pytest.mark.unit


# ===========================================================================
# Fixtures
# ===========================================================================


@pytest.fixture
def mock_driver_ctx() -> Any:
    """Patch GraphDatabase.driver and yield (driver_mock, session_mock)."""
    with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
        driver = MagicMock()
        session = MagicMock()
        session.__enter__ = MagicMock(return_value=session)
        session.__exit__ = MagicMock(return_value=False)
        driver.session.return_value = session
        mock_gdb.driver.return_value = driver
        yield driver, session


@pytest.fixture
def sink(mock_driver_ctx: Any) -> Neo4jSink:
    """A Neo4jSink backed by a fully mocked driver."""
    driver, _ = mock_driver_ctx
    with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
        mock_gdb.driver.return_value = driver
        return Neo4jSink("bolt://localhost:7687", "neo4j", "pw")


@pytest.fixture
def sink_batch2(mock_driver_ctx: Any) -> Neo4jSink:
    """A Neo4jSink with batch_size=2 for batching tests."""
    driver, _ = mock_driver_ctx
    with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
        mock_gdb.driver.return_value = driver
        return Neo4jSink("bolt://localhost:7687", "neo4j", "pw", batch_size=2)


@pytest.fixture
def persons_df() -> pd.DataFrame:
    """Three-row persons DataFrame."""
    return pd.DataFrame(
        {
            "pid": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "age": [30, 25, 40],
        },
    )


@pytest.fixture
def rels_df() -> pd.DataFrame:
    """Two-row relationships DataFrame."""
    return pd.DataFrame(
        {
            "src": [1, 2],
            "tgt": [2, 3],
            "since": [2020, 2021],
        },
    )


@pytest.fixture
def node_mapping() -> NodeMapping:
    """Basic NodeMapping for persons_df."""
    return NodeMapping(
        label="Person",
        id_column="pid",
        property_columns={"name": "name", "age": "age"},
    )


@pytest.fixture
def rel_mapping() -> RelationshipMapping:
    """Basic RelationshipMapping for rels_df."""
    return RelationshipMapping(
        rel_type="KNOWS",
        source_label="Person",
        target_label="Person",
        source_id_column="src",
        target_id_column="tgt",
        property_columns={"since": "since"},
    )


# ===========================================================================
# NodeMapping model
# ===========================================================================


class TestNodeMapping:
    """Tests for NodeMapping Pydantic model."""

    def test_required_fields_label_and_id_column(self) -> None:
        mapping = NodeMapping(label="Person", id_column="pid")
        assert mapping.label == "Person"
        assert mapping.id_column == "pid"

    def test_id_property_defaults_to_id(self) -> None:
        mapping = NodeMapping(label="Person", id_column="pid")
        assert mapping.id_property == "id"

    def test_property_columns_defaults_to_empty(self) -> None:
        mapping = NodeMapping(label="Person", id_column="pid")
        assert mapping.property_columns == {}

    def test_custom_id_property(self) -> None:
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            id_property="person_id",
        )
        assert mapping.id_property == "person_id"

    def test_property_columns_stored_correctly(self) -> None:
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "full_name", "age": "years"},
        )
        assert mapping.property_columns == {
            "name": "full_name",
            "age": "years",
        }

    def test_missing_label_raises(self) -> None:
        with pytest.raises(Exception):
            NodeMapping(id_column="pid")  # type: ignore[call-arg]

    def test_missing_id_column_raises(self) -> None:
        with pytest.raises(Exception):
            NodeMapping(label="Person")  # type: ignore[call-arg]

    def test_label_accepts_spaces_and_unicode(self) -> None:
        mapping = NodeMapping(label="My Node Ñ", id_column="id")
        assert mapping.label == "My Node Ñ"


# ===========================================================================
# RelationshipMapping model
# ===========================================================================


class TestRelationshipMapping:
    """Tests for RelationshipMapping Pydantic model."""

    def test_required_fields(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert mapping.rel_type == "KNOWS"
        assert mapping.source_label == "Person"
        assert mapping.target_label == "Person"
        assert mapping.source_id_column == "src"
        assert mapping.target_id_column == "tgt"

    def test_source_id_property_defaults_to_id(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert mapping.source_id_property == "id"

    def test_target_id_property_defaults_to_id(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert mapping.target_id_property == "id"

    def test_property_columns_defaults_to_empty(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert mapping.property_columns == {}

    def test_custom_id_properties(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            source_id_property="person_id",
            target_id_property="person_id",
        )
        assert mapping.source_id_property == "person_id"
        assert mapping.target_id_property == "person_id"

    def test_property_columns_stored(self) -> None:
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            property_columns={"since": "year_col"},
        )
        assert mapping.property_columns == {"since": "year_col"}

    def test_missing_rel_type_raises(self) -> None:
        with pytest.raises(Exception):
            RelationshipMapping(  # type: ignore[call-arg]
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )

    def test_different_source_and_target_labels(self) -> None:
        mapping = RelationshipMapping(
            rel_type="LIVES_IN",
            source_label="Person",
            target_label="City",
            source_id_column="person_id",
            target_id_column="city_id",
        )
        assert mapping.source_label == "Person"
        assert mapping.target_label == "City"


# ===========================================================================
# _coerce_value
# ===========================================================================


class TestCoerceValue:
    """Tests for the _coerce_value scalar conversion helper."""

    def test_numpy_int64_to_int(self) -> None:
        assert _coerce_value(np.int64(42)) == 42
        assert isinstance(_coerce_value(np.int64(42)), int)

    def test_numpy_int32_to_int(self) -> None:
        assert _coerce_value(np.int32(7)) == 7
        assert isinstance(_coerce_value(np.int32(7)), int)

    def test_numpy_float64_to_float(self) -> None:
        result = _coerce_value(np.float64(3.14))
        assert abs(result - 3.14) < 1e-9
        assert isinstance(result, float)

    def test_numpy_float32_to_float(self) -> None:
        result = _coerce_value(np.float32(1.5))
        assert isinstance(result, float)

    def test_numpy_bool_true(self) -> None:
        result = _coerce_value(np.bool_(True))
        assert result is True
        assert isinstance(result, bool)

    def test_numpy_bool_false(self) -> None:
        result = _coerce_value(np.bool_(False))
        assert result is False
        assert isinstance(result, bool)

    def test_pandas_timestamp_to_datetime(self) -> None:
        ts = pd.Timestamp("2023-06-15 12:00:00")
        result = _coerce_value(ts)
        assert isinstance(result, datetime.datetime)
        assert result.year == 2023
        assert result.month == 6
        assert result.day == 15

    def test_nan_returns_none(self) -> None:
        assert _coerce_value(float("nan")) is None

    def test_none_returns_none(self) -> None:
        assert _coerce_value(None) is None

    def test_pandas_na_returns_none(self) -> None:
        assert _coerce_value(pd.NA) is None

    def test_plain_int_passthrough(self) -> None:
        assert _coerce_value(99) == 99
        assert isinstance(_coerce_value(99), int)

    def test_plain_float_passthrough(self) -> None:
        result = _coerce_value(2.71)
        assert abs(result - 2.71) < 1e-9

    def test_plain_string_passthrough(self) -> None:
        assert _coerce_value("hello") == "hello"

    def test_plain_bool_true_passthrough(self) -> None:
        assert _coerce_value(True) is True

    def test_plain_bool_false_passthrough(self) -> None:
        assert _coerce_value(False) is False

    def test_zero_int_not_treated_as_null(self) -> None:
        assert _coerce_value(0) == 0

    def test_zero_float_not_treated_as_null(self) -> None:
        assert _coerce_value(0.0) == 0.0

    def test_empty_string_not_treated_as_null(self) -> None:
        assert _coerce_value("") == ""


# ===========================================================================
# _coerce_row
# ===========================================================================


class TestCoerceRow:
    """Tests for _coerce_row, which coerces every value in a dict."""

    def test_coerces_all_values(self) -> None:
        row = {"a": np.int64(1), "b": np.float64(2.5), "c": "plain"}
        result = _coerce_row(row)
        assert result == {"a": 1, "b": 2.5, "c": "plain"}
        assert isinstance(result["a"], int)
        assert isinstance(result["b"], float)

    def test_preserves_keys(self) -> None:
        row = {"x": np.int64(10), "y": "hello"}
        result = _coerce_row(row)
        assert set(result.keys()) == {"x", "y"}

    def test_nan_coerced_to_none(self) -> None:
        row = {"v": float("nan")}
        result = _coerce_row(row)
        assert result["v"] is None

    def test_empty_row(self) -> None:
        assert _coerce_row({}) == {}

    def test_does_not_mutate_original(self) -> None:
        original: dict[str, Any] = {"a": np.int64(5)}
        _coerce_row(original)
        assert isinstance(original["a"], np.int64)


# ===========================================================================
# _drop_nulls
# ===========================================================================


class TestDropNulls:
    """Tests for _drop_nulls."""

    def test_removes_none_values(self) -> None:
        result = _drop_nulls({"a": 1, "b": None, "c": "x"})
        assert result == {"a": 1, "c": "x"}

    def test_keeps_zero_int(self) -> None:
        result = _drop_nulls({"a": 0})
        assert result == {"a": 0}

    def test_keeps_zero_float(self) -> None:
        result = _drop_nulls({"a": 0.0})
        assert result == {"a": 0.0}

    def test_keeps_false(self) -> None:
        result = _drop_nulls({"a": False})
        assert result == {"a": False}

    def test_keeps_empty_string(self) -> None:
        result = _drop_nulls({"a": ""})
        assert result == {"a": ""}

    def test_all_none_returns_empty(self) -> None:
        result = _drop_nulls({"a": None, "b": None})
        assert result == {}

    def test_empty_dict_returns_empty(self) -> None:
        assert _drop_nulls({}) == {}

    def test_no_nulls_returns_same_content(self) -> None:
        d = {"a": 1, "b": "x"}
        assert _drop_nulls(d) == d


# ===========================================================================
# _validate_columns
# ===========================================================================


class TestValidateColumns:
    """Tests for _validate_columns."""

    def test_passes_when_all_columns_present(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2]})
        _validate_columns(df, ["a", "b"], "test")  # should not raise

    def test_raises_when_column_missing(self) -> None:
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="missing_col"):
            _validate_columns(df, ["a", "missing_col"], "test")

    def test_error_includes_context(self) -> None:
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="my_context"):
            _validate_columns(df, ["nope"], "my_context")

    def test_error_includes_available_columns(self) -> None:
        df = pd.DataFrame({"existing": [1]})
        with pytest.raises(ValueError, match="existing"):
            _validate_columns(df, ["nope"], "ctx")

    def test_passes_with_empty_required_list(self) -> None:
        df = pd.DataFrame({"a": [1]})
        _validate_columns(df, [], "ctx")  # should not raise

    def test_raises_for_all_missing_columns(self) -> None:
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError):
            _validate_columns(df, ["x", "y", "z"], "ctx")


# ===========================================================================
# Cypher template builders
# ===========================================================================


class TestNodeMergeCypher:
    """Tests for _node_merge_cypher."""

    def test_contains_unwind(self) -> None:
        assert "UNWIND" in _node_merge_cypher("Person", "id")

    def test_contains_merge(self) -> None:
        cypher = _node_merge_cypher("Person", "id")
        assert "MERGE" in cypher

    def test_contains_label(self) -> None:
        assert "Person" in _node_merge_cypher("Person", "id")

    def test_contains_id_property(self) -> None:
        cypher = _node_merge_cypher("Person", "person_id")
        assert "person_id" in cypher

    def test_contains_set(self) -> None:
        assert "SET" in _node_merge_cypher("Person", "id")

    def test_references_rows_parameter(self) -> None:
        assert "$rows" in _node_merge_cypher("Person", "id")

    def test_uses_row_id(self) -> None:
        assert "row.id" in _node_merge_cypher("Person", "id")

    def test_uses_row_properties(self) -> None:
        assert "row.properties" in _node_merge_cypher("Person", "id")

    def test_backtick_quotes_label(self) -> None:
        cypher = _node_merge_cypher("My Label", "id")
        assert "`My Label`" in cypher


class TestRelMergeCypher:
    """Tests for _rel_merge_cypher."""

    def test_contains_unwind(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "UNWIND" in cypher

    def test_uses_match_for_source(self) -> None:
        cypher = _rel_merge_cypher("Person", "City", "LIVES_IN", "id", "id")
        assert "MATCH (src" in cypher

    def test_uses_match_for_target(self) -> None:
        cypher = _rel_merge_cypher("Person", "City", "LIVES_IN", "id", "id")
        assert "MATCH (tgt" in cypher

    def test_uses_merge_for_relationship(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "MERGE (src)-[r:" in cypher

    def test_does_not_merge_source_node(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        # MERGE count should be 1 (only for the relationship)
        assert cypher.count("MERGE") == 1

    def test_contains_rel_type(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "KNOWS" in cypher

    def test_contains_src_label(self) -> None:
        cypher = _rel_merge_cypher("Person", "City", "LIVES_IN", "id", "id")
        assert "Person" in cypher

    def test_contains_tgt_label(self) -> None:
        cypher = _rel_merge_cypher("Person", "City", "LIVES_IN", "id", "id")
        assert "City" in cypher

    def test_contains_src_id_property(self) -> None:
        cypher = _rel_merge_cypher(
            "Person",
            "Person",
            "KNOWS",
            "person_id",
            "id",
        )
        assert "person_id" in cypher

    def test_contains_tgt_id_property(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "pid")
        assert "pid" in cypher

    def test_references_rows_parameter(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "$rows" in cypher

    def test_references_row_src_id(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "row.src_id" in cypher

    def test_references_row_tgt_id(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "row.tgt_id" in cypher

    def test_contains_set(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "KNOWS", "id", "id")
        assert "SET" in cypher

    def test_backtick_quotes_rel_type(self) -> None:
        cypher = _rel_merge_cypher("Person", "Person", "MY TYPE", "id", "id")
        assert "`MY TYPE`" in cypher


# ===========================================================================
# _build_node_rows
# ===========================================================================


class TestBuildNodeRows:
    """Tests for _build_node_rows serialiser."""

    def test_basic_serialisation(self) -> None:
        df = pd.DataFrame({"pid": [1], "name": ["Alice"]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name"},
        )
        rows = _build_node_rows(df, mapping)
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert rows[0]["properties"] == {"name": "Alice"}

    def test_multiple_rows(self) -> None:
        df = pd.DataFrame({"pid": [1, 2, 3], "name": ["A", "B", "C"]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name"},
        )
        rows = _build_node_rows(df, mapping)
        assert len(rows) == 3
        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_skips_null_id(self) -> None:
        df = pd.DataFrame({"pid": [1, None, 3], "name": ["A", "B", "C"]})
        mapping = NodeMapping(label="Person", id_column="pid")
        rows = _build_node_rows(df, mapping)
        assert len(rows) == 2
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 3

    def test_drops_null_property_values(self) -> None:
        df = pd.DataFrame({"pid": [1], "name": [None]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name"},
        )
        rows = _build_node_rows(df, mapping)
        assert "name" not in rows[0]["properties"]

    def test_keeps_non_null_properties(self) -> None:
        df = pd.DataFrame({"pid": [1], "age": [30]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"age": "age"},
        )
        rows = _build_node_rows(df, mapping)
        assert rows[0]["properties"]["age"] == 30

    def test_empty_property_columns(self) -> None:
        df = pd.DataFrame({"pid": [1, 2]})
        mapping = NodeMapping(label="Person", id_column="pid")
        rows = _build_node_rows(df, mapping)
        assert len(rows) == 2
        assert all(r["properties"] == {} for r in rows)

    def test_coerces_numpy_id(self) -> None:
        df = pd.DataFrame({"pid": pd.array([np.int64(5)])})
        mapping = NodeMapping(label="Person", id_column="pid")
        rows = _build_node_rows(df, mapping)
        assert isinstance(rows[0]["id"], int)

    def test_coerces_numpy_property(self) -> None:
        df = pd.DataFrame({"pid": [1], "score": [np.float64(9.5)]})
        mapping = NodeMapping(
            label="Node",
            id_column="pid",
            property_columns={"score": "score"},
        )
        rows = _build_node_rows(df, mapping)
        assert isinstance(rows[0]["properties"]["score"], float)

    def test_empty_dataframe_returns_empty_list(self) -> None:
        df = pd.DataFrame({"pid": pd.Series([], dtype=int)})
        mapping = NodeMapping(label="Person", id_column="pid")
        rows = _build_node_rows(df, mapping)
        assert rows == []

    def test_multiple_properties(self) -> None:
        df = pd.DataFrame({"pid": [1], "name": ["Alice"], "age": [30]})
        mapping = NodeMapping(
            label="Person",
            id_column="pid",
            property_columns={"name": "name", "years": "age"},
        )
        rows = _build_node_rows(df, mapping)
        assert rows[0]["properties"] == {"name": "Alice", "years": 30}


# ===========================================================================
# _build_rel_rows
# ===========================================================================


class TestBuildRelRows:
    """Tests for _build_rel_rows serialiser."""

    def test_basic_serialisation(self) -> None:
        df = pd.DataFrame({"src": [1], "tgt": [2], "since": [2020]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            property_columns={"since": "since"},
        )
        rows = _build_rel_rows(df, mapping)
        assert len(rows) == 1
        assert rows[0]["src_id"] == 1
        assert rows[0]["tgt_id"] == 2
        assert rows[0]["properties"] == {"since": 2020}

    def test_multiple_rows(self) -> None:
        df = pd.DataFrame({"src": [1, 2], "tgt": [2, 3]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert len(rows) == 2

    def test_skips_null_source_id(self) -> None:
        df = pd.DataFrame({"src": [None, 2], "tgt": [2, 3]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert len(rows) == 1
        assert rows[0]["src_id"] == 2

    def test_skips_null_target_id(self) -> None:
        df = pd.DataFrame({"src": [1, 2], "tgt": [None, 3]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert len(rows) == 1
        assert rows[0]["tgt_id"] == 3

    def test_drops_null_property_values(self) -> None:
        df = pd.DataFrame({"src": [1], "tgt": [2], "note": [None]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
            property_columns={"note": "note"},
        )
        rows = _build_rel_rows(df, mapping)
        assert "note" not in rows[0]["properties"]

    def test_empty_property_columns(self) -> None:
        df = pd.DataFrame({"src": [1], "tgt": [2]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert rows[0]["properties"] == {}

    def test_empty_dataframe_returns_empty_list(self) -> None:
        df = pd.DataFrame(
            {
                "src": pd.Series([], dtype=int),
                "tgt": pd.Series([], dtype=int),
            },
        )
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        assert _build_rel_rows(df, mapping) == []

    def test_coerces_numpy_ids(self) -> None:
        df = pd.DataFrame({"src": [np.int64(1)], "tgt": [np.int64(2)]})
        mapping = RelationshipMapping(
            rel_type="KNOWS",
            source_label="Person",
            target_label="Person",
            source_id_column="src",
            target_id_column="tgt",
        )
        rows = _build_rel_rows(df, mapping)
        assert isinstance(rows[0]["src_id"], int)
        assert isinstance(rows[0]["tgt_id"], int)


# ===========================================================================
# Neo4jSink — unit tests (driver mocked)
# ===========================================================================


class TestNeo4jSinkContextManager:
    """Tests for Neo4jSink as a context manager."""

    def test_enter_returns_sink(self) -> None:
        with patch("pycypher.sinks.neo4j.GraphDatabase"):
            s = Neo4jSink("bolt://x", "u", "p")
            assert s.__enter__() is s

    def test_exit_calls_close(self) -> None:
        with patch("pycypher.sinks.neo4j.GraphDatabase"):
            s = Neo4jSink("bolt://x", "u", "p")
            s.close = MagicMock()
            s.__exit__(None, None, None)
            s.close.assert_called_once()

    def test_close_called_on_exception(self) -> None:
        with patch("pycypher.sinks.neo4j.GraphDatabase"):
            s = Neo4jSink("bolt://x", "u", "p")
            s.close = MagicMock()
            try:
                with s:
                    msg = "deliberate"
                    raise RuntimeError(msg)
            except RuntimeError:
                pass
            s.close.assert_called_once()

    def test_with_statement_closes_driver(self, mock_driver_ctx: Any) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            with Neo4jSink("bolt://x", "u", "p"):
                pass
        driver.close.assert_called_once()


class TestNeo4jSinkWriteNodes:
    """Tests for Neo4jSink.write_nodes."""

    def test_returns_row_count(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            mapping = NodeMapping(label="Person", id_column="pid")
            count = s.write_nodes(persons_df, mapping)
        assert count == 3

    def test_empty_df_returns_zero(self, mock_driver_ctx: Any) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            empty = pd.DataFrame({"pid": pd.Series([], dtype=int)})
            count = s.write_nodes(
                empty,
                NodeMapping(label="P", id_column="pid"),
            )
        assert count == 0
        session.run.assert_not_called()

    def test_calls_session_run(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_nodes(
                persons_df,
                NodeMapping(label="Person", id_column="pid"),
            )
        session.run.assert_called_once()

    def test_cypher_contains_label(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_nodes(
                persons_df,
                NodeMapping(label="Person", id_column="pid"),
            )
        cypher = session.run.call_args[0][0]
        assert "Person" in cypher

    def test_cypher_contains_merge(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_nodes(
                persons_df,
                NodeMapping(label="Person", id_column="pid"),
            )
        cypher = session.run.call_args[0][0]
        assert "MERGE" in cypher

    def test_rows_kwarg_passed_to_run(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_nodes(
                persons_df,
                NodeMapping(label="Person", id_column="pid"),
            )
        kwargs = session.run.call_args[1]
        assert "rows" in kwargs
        assert isinstance(kwargs["rows"], list)
        assert len(kwargs["rows"]) == 3

    def test_raises_on_missing_id_column(self, mock_driver_ctx: Any) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            df = pd.DataFrame({"other_col": [1, 2]})
            with pytest.raises(ValueError, match="pid"):
                s.write_nodes(df, NodeMapping(label="P", id_column="pid"))

    def test_raises_on_missing_property_column(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            df = pd.DataFrame({"pid": [1]})
            with pytest.raises(ValueError, match="missing_col"):
                s.write_nodes(
                    df,
                    NodeMapping(
                        label="P",
                        id_column="pid",
                        property_columns={"x": "missing_col"},
                    ),
                )

    def test_reraises_driver_exception(
        self,
        mock_driver_ctx: Any,
        persons_df: pd.DataFrame,
    ) -> None:
        driver, session = mock_driver_ctx
        session.run.side_effect = RuntimeError("neo4j error")
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            with pytest.raises(RuntimeError, match="neo4j error"):
                s.write_nodes(
                    persons_df,
                    NodeMapping(label="P", id_column="pid"),
                )


class TestNeo4jSinkWriteRelationships:
    """Tests for Neo4jSink.write_relationships."""

    def test_returns_row_count(
        self,
        mock_driver_ctx: Any,
        rels_df: pd.DataFrame,
        rel_mapping: RelationshipMapping,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            count = s.write_relationships(rels_df, rel_mapping)
        assert count == 2

    def test_empty_df_returns_zero(self, mock_driver_ctx: Any) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            empty = pd.DataFrame(
                {
                    "src": pd.Series([], dtype=int),
                    "tgt": pd.Series([], dtype=int),
                },
            )
            mapping = RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )
            count = s.write_relationships(empty, mapping)
        assert count == 0
        session.run.assert_not_called()

    def test_cypher_uses_match_for_nodes(
        self,
        mock_driver_ctx: Any,
        rels_df: pd.DataFrame,
        rel_mapping: RelationshipMapping,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_relationships(rels_df, rel_mapping)
        cypher = session.run.call_args[0][0]
        assert "MATCH (src" in cypher
        assert "MATCH (tgt" in cypher

    def test_cypher_uses_merge_for_relationship(
        self,
        mock_driver_ctx: Any,
        rels_df: pd.DataFrame,
        rel_mapping: RelationshipMapping,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            s.write_relationships(rels_df, rel_mapping)
        cypher = session.run.call_args[0][0]
        assert "MERGE (src)-[r:" in cypher

    def test_raises_on_missing_source_column(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            df = pd.DataFrame({"tgt": [2]})
            mapping = RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )
            with pytest.raises(ValueError, match="src"):
                s.write_relationships(df, mapping)

    def test_raises_on_missing_target_column(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p")
            df = pd.DataFrame({"src": [1]})
            mapping = RelationshipMapping(
                rel_type="KNOWS",
                source_label="Person",
                target_label="Person",
                source_id_column="src",
                target_id_column="tgt",
            )
            with pytest.raises(ValueError, match="tgt"):
                s.write_relationships(df, mapping)


# ===========================================================================
# Batching behaviour
# ===========================================================================


class TestBatching:
    """Tests for batch-splitting logic inside _write_batches."""

    def test_single_batch_when_rows_lt_batch_size(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=10)
            df = pd.DataFrame({"pid": range(5)})
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))
        assert session.run.call_count == 1

    def test_single_batch_when_rows_eq_batch_size(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=5)
            df = pd.DataFrame({"pid": range(5)})
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))
        assert session.run.call_count == 1

    def test_two_batches_when_rows_eq_batch_size_plus_one(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=5)
            df = pd.DataFrame({"pid": range(6)})
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))
        assert session.run.call_count == 2

    def test_three_batches_for_five_rows_batch_size_two(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=2)
            df = pd.DataFrame({"pid": range(5)})
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))
        assert session.run.call_count == 3

    def test_batch_sizes_sum_to_total_rows(self, mock_driver_ctx: Any) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=3)
            df = pd.DataFrame({"pid": range(7)})
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))

        total_rows = sum(len(c[1]["rows"]) for c in session.run.call_args_list)
        assert total_rows == 7

    def test_last_batch_is_partial(self, mock_driver_ctx: Any) -> None:
        driver, session = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=3)
            df = pd.DataFrame({"pid": range(5)})  # 5 rows → [3, 2]
            s.write_nodes(df, NodeMapping(label="P", id_column="pid"))

        batch_sizes = [len(c[1]["rows"]) for c in session.run.call_args_list]
        assert batch_sizes == [3, 2]

    def test_total_count_returned_with_batching(
        self,
        mock_driver_ctx: Any,
    ) -> None:
        driver, _ = mock_driver_ctx
        with patch("pycypher.sinks.neo4j.GraphDatabase") as mock_gdb:
            mock_gdb.driver.return_value = driver
            s = Neo4jSink("bolt://x", "u", "p", batch_size=3)
            df = pd.DataFrame({"pid": range(7)})
            count = s.write_nodes(df, NodeMapping(label="P", id_column="pid"))
        assert count == 7
