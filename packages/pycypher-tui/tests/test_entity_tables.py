"""Tests for the entity tables configuration screen."""

from __future__ import annotations

import pytest

from pycypher_tui.screens.entity_tables import (
    ColumnMapping,
    EntityDetailPanel,
    EntityListItem,
    EntityTableInfo,
    EntityTablesScreen,
)


class TestEntityTableInfo:
    def test_create_entity_info(self):
        info = EntityTableInfo(
            source_id="customers",
            entity_type="Customer",
            uri="data/customers.csv",
            id_col="customer_id",
            schema_hints={"age": "INTEGER"},
            has_query=False,
        )
        assert info.source_id == "customers"
        assert info.entity_type == "Customer"
        assert info.id_col == "customer_id"
        assert info.schema_hints == {"age": "INTEGER"}

    def test_frozen(self):
        info = EntityTableInfo("id", "Type", "uri", None, {}, False)
        with pytest.raises(AttributeError):
            info.source_id = "other"

    def test_without_id_col(self):
        info = EntityTableInfo("id", "Type", "uri", None, {}, False)
        assert info.id_col is None

    def test_with_query(self):
        info = EntityTableInfo("id", "Type", "uri", None, {}, True)
        assert info.has_query is True


class TestColumnMapping:
    def test_create_id_column(self):
        col = ColumnMapping(name="customer_id", mapped_type="INTEGER", is_id=True, is_property=False)
        assert col.name == "customer_id"
        assert col.is_id is True
        assert col.is_property is False

    def test_create_property_column(self):
        col = ColumnMapping(name="name", mapped_type="VARCHAR", is_id=False, is_property=True)
        assert col.is_id is False
        assert col.is_property is True


class TestEntityTablesScreen:
    def _make_screen(self, entities=None):
        screen = EntityTablesScreen.__new__(EntityTablesScreen)
        screen._cursor = 0
        screen._items = entities or []
        screen._pending_keys = []
        return screen

    def test_entity_count_empty(self):
        screen = self._make_screen()
        assert screen.entity_count == 0

    def test_entity_count_with_items(self):
        entities = [
            EntityTableInfo("a", "A", "a.csv", None, {}, False),
            EntityTableInfo("b", "B", "b.csv", "id", {}, False),
        ]
        screen = self._make_screen(entities)
        assert screen.entity_count == 2

    def test_current_entity_empty(self):
        screen = self._make_screen()
        assert screen.current_entity is None

    def test_current_entity_valid(self):
        entities = [
            EntityTableInfo("a", "A", "a.csv", None, {}, False),
            EntityTableInfo("b", "B", "b.csv", "id", {}, False),
        ]
        screen = self._make_screen(entities)
        assert screen.current_entity == entities[0]
        screen._cursor = 1
        assert screen.current_entity == entities[1]


class TestCursorNavigation:
    def _make_screen(self):
        entities = [
            EntityTableInfo("a", "A", "a.csv", None, {}, False),
            EntityTableInfo("b", "B", "b.csv", "id", {}, False),
            EntityTableInfo("c", "C", "c.csv", None, {"x": "INT"}, True),
        ]
        screen = EntityTablesScreen.__new__(EntityTablesScreen)
        screen._cursor = 0
        screen._items = entities
        screen._pending_keys = []
        return screen

    def test_move_down(self):
        screen = self._make_screen()
        screen._move_cursor(1)
        assert screen._cursor == 1

    def test_move_up(self):
        screen = self._make_screen()
        screen._cursor = 2
        screen._move_cursor(-1)
        assert screen._cursor == 1

    def test_clamp_bottom(self):
        screen = self._make_screen()
        screen._cursor = 2
        screen._move_cursor(1)
        assert screen._cursor == 2

    def test_clamp_top(self):
        screen = self._make_screen()
        screen._move_cursor(-1)
        assert screen._cursor == 0

    def test_jump_to(self):
        screen = self._make_screen()
        screen._jump_to(2)
        assert screen._cursor == 2

    def test_jump_clamps(self):
        screen = self._make_screen()
        screen._jump_to(100)
        assert screen._cursor == 2

    def test_pending_gg(self):
        screen = self._make_screen()
        screen._cursor = 2
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0

    def test_escape_clears(self):
        screen = self._make_screen()
        screen._pending_keys = ["d"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []


class TestExtractEntities:
    def _make_screen(self):
        screen = EntityTablesScreen.__new__(EntityTablesScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        return screen

    def test_extract_empty(self):
        from pycypher.ingestion.config import PipelineConfig
        screen = self._make_screen()
        config = PipelineConfig(version="1.0")
        entities = screen._extract_entities(config)
        assert entities == []

    def test_extract_entities(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="customers",
                        uri="data/customers.csv",
                        entity_type="Customer",
                        id_col="cid",
                        schema_hints={"age": "INTEGER"},
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        entities = screen._extract_entities(config)
        assert len(entities) == 1
        assert entities[0].source_id == "customers"
        assert entities[0].entity_type == "Customer"
        assert entities[0].id_col == "cid"
        assert entities[0].schema_hints == {"age": "INTEGER"}

    def test_extract_with_query(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="filtered",
                        uri="duckdb:///:memory:",
                        entity_type="Filtered",
                        query="SELECT * FROM source WHERE active = true",
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        entities = screen._extract_entities(config)
        assert len(entities) == 1
        assert entities[0].has_query is True

    def test_ignores_relationship_sources(self):
        from pycypher.ingestion.config import (
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                relationships=[
                    RelationshipSourceConfig(
                        id="follows",
                        uri="data/follows.csv",
                        relationship_type="FOLLOWS",
                        source_col="a",
                        target_col="b",
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        entities = screen._extract_entities(config)
        assert entities == []


class TestMessages:
    def test_navigate_back(self):
        msg = EntityTablesScreen.NavigateBack()
        assert msg is not None

    def test_edit_entity(self):
        msg = EntityTablesScreen.EditEntity("customers")
        assert msg.source_id == "customers"

    def test_add_entity(self):
        msg = EntityTablesScreen.AddEntity()
        assert msg is not None

    def test_delete_entity(self):
        msg = EntityTablesScreen.DeleteEntity("customers")
        assert msg.source_id == "customers"
