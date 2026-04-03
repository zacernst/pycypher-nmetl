"""Tests for the data sources configuration screen."""

from __future__ import annotations

import pytest

from pycypher_tui.screens.data_sources import (
    DataSourcesScreen,
    SourceDetailPanel,
    SourceItem,
    SourceListItem,
)


class TestSourceItem:
    """Tests for SourceItem dataclass."""

    def test_create_entity_source(self):
        item = SourceItem(
            source_id="customers",
            uri="data/customers.csv",
            source_type="entity",
            label="Customer",
            id_col="customer_id",
            extra={},
        )
        assert item.source_id == "customers"
        assert item.source_type == "entity"
        assert item.label == "Customer"

    def test_create_relationship_source(self):
        item = SourceItem(
            source_id="follows",
            uri="data/follows.csv",
            source_type="relationship",
            label="FOLLOWS",
            id_col=None,
            extra={"source_col": "follower_id", "target_col": "followed_id"},
        )
        assert item.source_type == "relationship"
        assert item.extra["source_col"] == "follower_id"

    def test_frozen(self):
        item = SourceItem("id", "uri", "entity", "L", None, {})
        with pytest.raises(AttributeError):
            item.source_id = "other"


class TestDataSourcesScreen:
    """Tests for DataSourcesScreen logic."""

    def _make_screen(self, sources=None):
        screen = DataSourcesScreen.__new__(DataSourcesScreen)
        screen._cursor = 0
        screen._items = sources or []
        screen._filter_mode = "all"
        screen._pending_keys = []
        screen._search_pattern = ""
        return screen

    def test_source_count_empty(self):
        screen = self._make_screen()
        assert screen.source_count == 0

    def test_source_count_with_items(self):
        sources = [
            SourceItem("a", "a.csv", "entity", "A", None, {}),
            SourceItem("b", "b.csv", "entity", "B", None, {}),
        ]
        screen = self._make_screen(sources)
        assert screen.source_count == 2

    def test_current_source_empty(self):
        screen = self._make_screen()
        assert screen.current_source is None

    def test_current_source_valid(self):
        sources = [
            SourceItem("a", "a.csv", "entity", "A", None, {}),
            SourceItem("b", "b.csv", "entity", "B", None, {}),
        ]
        screen = self._make_screen(sources)
        assert screen.current_source == sources[0]
        screen._cursor = 1
        assert screen.current_source == sources[1]

    def test_current_source_out_of_bounds(self):
        sources = [SourceItem("a", "a.csv", "entity", "A", None, {})]
        screen = self._make_screen(sources)
        screen._cursor = 5
        assert screen.current_source is None


class TestCursorNavigation:
    """Tests for VIM cursor movement in data sources screen."""

    def _make_screen(self):
        sources = [
            SourceItem("a", "a.csv", "entity", "A", None, {}),
            SourceItem("b", "b.csv", "entity", "B", None, {}),
            SourceItem("c", "c.csv", "relationship", "C", None, {}),
            SourceItem("d", "d.csv", "entity", "D", None, {}),
        ]
        screen = DataSourcesScreen.__new__(DataSourcesScreen)
        screen._cursor = 0
        screen._items = sources
        screen._filter_mode = "all"
        screen._pending_keys = []
        screen._search_pattern = ""
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

    def test_clamp_at_bottom(self):
        screen = self._make_screen()
        screen._cursor = 3
        screen._move_cursor(1)
        assert screen._cursor == 3

    def test_clamp_at_top(self):
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
        assert screen._cursor == 3

    def test_move_on_empty(self):
        screen = DataSourcesScreen.__new__(DataSourcesScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        screen._move_cursor(1)
        assert screen._cursor == 0

    def test_pending_gg(self):
        screen = self._make_screen()
        screen._cursor = 3
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0

    def test_escape_clears_pending(self):
        screen = self._make_screen()
        screen._pending_keys = ["d"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []


class TestFilterModes:
    """Tests for filter cycling."""

    def test_filter_modes_defined(self):
        assert DataSourcesScreen.FILTER_MODES == ["all", "entity", "relationship"]

    def test_cycle_filter(self):
        screen = DataSourcesScreen.__new__(DataSourcesScreen)
        screen._filter_mode = "all"
        screen._pending_keys = []

        idx = DataSourcesScreen.FILTER_MODES.index(screen._filter_mode)
        screen._filter_mode = DataSourcesScreen.FILTER_MODES[
            (idx + 1) % len(DataSourcesScreen.FILTER_MODES)
        ]
        assert screen._filter_mode == "entity"

        idx = DataSourcesScreen.FILTER_MODES.index(screen._filter_mode)
        screen._filter_mode = DataSourcesScreen.FILTER_MODES[
            (idx + 1) % len(DataSourcesScreen.FILTER_MODES)
        ]
        assert screen._filter_mode == "relationship"

        idx = DataSourcesScreen.FILTER_MODES.index(screen._filter_mode)
        screen._filter_mode = DataSourcesScreen.FILTER_MODES[
            (idx + 1) % len(DataSourcesScreen.FILTER_MODES)
        ]
        assert screen._filter_mode == "all"


class TestExtractSources:
    """Tests for _extract_sources from config."""

    def _make_screen(self):
        screen = DataSourcesScreen.__new__(DataSourcesScreen)
        screen._cursor = 0
        screen._items = []
        screen._filter_mode = "all"
        screen._pending_keys = []
        screen._search_pattern = ""
        return screen

    def test_extract_empty_config(self):
        from pycypher.ingestion.config import PipelineConfig

        screen = self._make_screen()
        config = PipelineConfig(version="1.0")
        sources = screen._extract_sources(config)
        assert sources == []

    def test_extract_entity_sources(self):
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
                        id_col="customer_id",
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        sources = screen._extract_sources(config)
        assert len(sources) == 1
        assert sources[0].source_id == "customers"
        assert sources[0].source_type == "entity"
        assert sources[0].label == "Customer"
        assert sources[0].id_col == "customer_id"

    def test_extract_relationship_sources(self):
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
                        source_col="follower_id",
                        target_col="followed_id",
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        sources = screen._extract_sources(config)
        assert len(sources) == 1
        assert sources[0].source_type == "relationship"
        assert sources[0].extra["source_col"] == "follower_id"
        assert sources[0].extra["target_col"] == "followed_id"

    def test_extract_mixed_sources(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="users",
                        uri="data/users.csv",
                        entity_type="User",
                        id_col="uid",
                    ),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="knows",
                        uri="data/knows.csv",
                        relationship_type="KNOWS",
                        source_col="from_id",
                        target_col="to_id",
                    ),
                ],
            ),
        )
        screen = self._make_screen()
        sources = screen._extract_sources(config)
        assert len(sources) == 2
        assert sources[0].source_type == "entity"
        assert sources[1].source_type == "relationship"


class TestMessages:
    """Tests for screen messages."""

    def test_navigate_back(self):
        msg = DataSourcesScreen.NavigateBack()
        assert msg is not None

    def test_edit_source(self):
        msg = DataSourcesScreen.EditSource("customers", "entity")
        assert msg.source_id == "customers"
        assert msg.source_type == "entity"

    def test_add_source(self):
        msg = DataSourcesScreen.AddSource("relationship")
        assert msg.source_type == "relationship"

    def test_delete_source(self):
        msg = DataSourcesScreen.DeleteSource("follows", "relationship")
        assert msg.source_id == "follows"
        assert msg.source_type == "relationship"
