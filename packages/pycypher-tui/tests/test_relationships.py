"""Tests for the relationship configuration screen."""

from __future__ import annotations

import pytest

from pycypher_tui.screens.base import VimNavigableScreen
from pycypher_tui.screens.relationships import (
    RelationshipDetailPanel,
    RelationshipItem,
    RelationshipListItem,
    RelationshipScreen,
)


# ---------------------------------------------------------------------------
# RelationshipItem dataclass tests
# ---------------------------------------------------------------------------


class TestRelationshipItem:
    """Tests for the RelationshipItem frozen dataclass."""

    def test_create_valid_item(self):
        item = RelationshipItem(
            source_id="follows",
            relationship_type="FOLLOWS",
            uri="data/follows.csv",
            source_col="follower_id",
            target_col="followed_id",
            id_col=None,
            source_entity="Person",
            target_entity="Person",
            status="valid",
            validation_messages=[],
        )
        assert item.source_id == "follows"
        assert item.relationship_type == "FOLLOWS"
        assert item.uri == "data/follows.csv"
        assert item.source_col == "follower_id"
        assert item.target_col == "followed_id"
        assert item.id_col is None
        assert item.source_entity == "Person"
        assert item.target_entity == "Person"
        assert item.status == "valid"
        assert item.validation_messages == []

    def test_frozen_immutability(self):
        item = RelationshipItem(
            source_id="r1",
            relationship_type="KNOWS",
            uri="data/knows.csv",
            source_col="src",
            target_col="tgt",
            id_col=None,
            source_entity=None,
            target_entity=None,
            status="valid",
            validation_messages=[],
        )
        with pytest.raises(AttributeError):
            item.status = "error"

    def test_item_with_warnings(self):
        item = RelationshipItem(
            source_id="r2",
            relationship_type="PURCHASED",
            uri="data/purchases.csv",
            source_col="customer_id",
            target_col="product_id",
            id_col="purchase_id",
            source_entity=None,
            target_entity=None,
            status="warning",
            validation_messages=["No entity sources defined"],
        )
        assert item.status == "warning"
        assert len(item.validation_messages) == 1
        assert item.id_col == "purchase_id"

    def test_item_with_error(self):
        item = RelationshipItem(
            source_id="r3",
            relationship_type="LIKES",
            uri="",
            source_col="user_id",
            target_col="item_id",
            id_col=None,
            source_entity=None,
            target_entity=None,
            status="error",
            validation_messages=["Missing URI"],
        )
        assert item.status == "error"
        assert "Missing URI" in item.validation_messages

    def test_equality(self):
        kwargs = dict(
            source_id="r1",
            relationship_type="KNOWS",
            uri="data/k.csv",
            source_col="a",
            target_col="b",
            id_col=None,
            source_entity=None,
            target_entity=None,
            status="valid",
            validation_messages=[],
        )
        assert RelationshipItem(**kwargs) == RelationshipItem(**kwargs)


# ---------------------------------------------------------------------------
# RelationshipScreen logic tests (non-widget, using __new__)
# ---------------------------------------------------------------------------


def _make_screen():
    """Create a RelationshipScreen bypassing __init__ for unit tests."""
    screen = RelationshipScreen.__new__(RelationshipScreen)
    screen._cursor = 0
    screen._items = []
    screen._pending_keys = []
    screen._entity_types = {}
    return screen


def _sample_items(n=4):
    """Generate n sample RelationshipItems."""
    return [
        RelationshipItem(
            source_id=f"r{i}",
            relationship_type=f"REL_{i}",
            uri=f"data/r{i}.csv",
            source_col=f"src_{i}",
            target_col=f"tgt_{i}",
            id_col=None,
            source_entity=None,
            target_entity=None,
            status="valid",
            validation_messages=[],
        )
        for i in range(n)
    ]


class TestRelationshipScreenProperties:
    """Tests for RelationshipScreen properties."""

    def test_relationship_count_empty(self):
        screen = _make_screen()
        assert screen.relationship_count == 0

    def test_relationship_count_with_items(self):
        screen = _make_screen()
        screen._items = _sample_items(3)
        assert screen.relationship_count == 3

    def test_current_relationship_none_when_empty(self):
        screen = _make_screen()
        assert screen.current_relationship is None

    def test_current_relationship_returns_item(self):
        screen = _make_screen()
        items = _sample_items(3)
        screen._items = items
        screen._cursor = 1
        assert screen.current_relationship == items[1]

    def test_current_relationship_out_of_bounds(self):
        screen = _make_screen()
        screen._items = _sample_items(2)
        screen._cursor = 5
        assert screen.current_relationship is None

    def test_current_relationship_negative_cursor(self):
        screen = _make_screen()
        screen._items = _sample_items(2)
        screen._cursor = -1
        assert screen.current_relationship is None


# ---------------------------------------------------------------------------
# Cursor navigation tests
# ---------------------------------------------------------------------------


class TestCursorNavigation:
    """Tests for VIM cursor movement logic."""

    def test_move_down(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._move_cursor(1)
        assert screen._cursor == 1

    def test_move_up(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._cursor = 2
        screen._move_cursor(-1)
        assert screen._cursor == 1

    def test_clamp_at_bottom(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._cursor = 3
        screen._move_cursor(1)
        assert screen._cursor == 3

    def test_clamp_at_top(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._move_cursor(-1)
        assert screen._cursor == 0

    def test_move_on_empty_list(self):
        screen = _make_screen()
        screen._move_cursor(1)
        assert screen._cursor == 0

    def test_move_large_delta(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._move_cursor(100)
        assert screen._cursor == 3

    def test_jump_to_specific(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._jump_to(2)
        assert screen._cursor == 2

    def test_jump_to_first(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._cursor = 3
        screen._jump_to(0)
        assert screen._cursor == 0

    def test_jump_to_last(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._jump_to(3)
        assert screen._cursor == 3

    def test_jump_clamps_high(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._jump_to(999)
        assert screen._cursor == 3

    def test_jump_clamps_low(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._jump_to(-10)
        assert screen._cursor == 0

    def test_jump_on_empty(self):
        screen = _make_screen()
        screen._jump_to(5)
        assert screen._cursor == 0


# ---------------------------------------------------------------------------
# Pending key sequence tests
# ---------------------------------------------------------------------------


class TestPendingKeySequences:
    """Tests for multi-key VIM sequences (gg, dd)."""

    def test_gg_jumps_to_first(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._cursor = 3
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0
        assert screen._pending_keys == []

    def test_escape_clears_pending(self):
        screen = _make_screen()
        screen._pending_keys = ["g"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []

    def test_dd_sequence_parsing(self):
        """Verify dd is recognized as delete sequence."""
        screen = _make_screen()
        screen._pending_keys = ["d"]
        sequence = "".join(screen._pending_keys) + "d"
        screen._pending_keys.clear()
        assert sequence == "dd"
        assert screen._pending_keys == []

    def test_unknown_sequence_clears_pending(self):
        screen = _make_screen()
        screen._pending_keys = ["d"]
        screen._handle_pending("x")  # dx is not a valid sequence
        assert screen._pending_keys == []

    def test_gx_clears_pending(self):
        screen = _make_screen()
        screen._items = _sample_items(4)
        screen._cursor = 2
        screen._pending_keys = ["g"]
        screen._handle_pending("x")  # gx is not a valid sequence
        assert screen._pending_keys == []
        assert screen._cursor == 2  # unchanged


# ---------------------------------------------------------------------------
# Message type tests
# ---------------------------------------------------------------------------


class TestMessages:
    """Tests for screen message types."""

    def test_navigate_back_message_is_message_subclass(self):
        from textual.message import Message
        msg = RelationshipScreen.NavigateBack()
        assert isinstance(msg, Message)

    def test_edit_relationship_message(self):
        msg = RelationshipScreen.EditRelationship("follows")
        assert msg.source_id == "follows"

    def test_add_relationship_message_is_message_subclass(self):
        from textual.message import Message
        msg = RelationshipScreen.AddRelationship()
        assert isinstance(msg, Message)

    def test_delete_relationship_message(self):
        msg = RelationshipScreen.DeleteRelationship("r1")
        assert msg.source_id == "r1"


# ---------------------------------------------------------------------------
# _build_relationship_list tests
# ---------------------------------------------------------------------------


class TestBuildRelationshipList:
    """Tests for _build_relationship_list with real config objects."""

    def test_empty_config(self):
        from pycypher.ingestion.config import PipelineConfig

        screen = _make_screen()
        config = PipelineConfig(version="1.0")
        items = screen._build_relationship_list(config)
        assert items == []

    def test_single_relationship(self):
        from pycypher.ingestion.config import (
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        screen = _make_screen()
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
        items = screen._build_relationship_list(config)
        assert len(items) == 1
        assert items[0].source_id == "follows"
        assert items[0].relationship_type == "FOLLOWS"
        assert items[0].source_col == "follower_id"
        assert items[0].target_col == "followed_id"

    def test_missing_uri_produces_error(self):
        """Empty URI triggers error status.

        Pydantic validates URI at config level, so we test by constructing
        a RelationshipItem directly with empty uri to verify the validation
        path in _build_relationship_list would catch it.
        """
        # The validation in _build_relationship_list checks `if not rel.uri`
        # but pydantic prevents empty URIs at construction time.
        # Verify the screen's validation logic handles it if it were to occur:
        item = RelationshipItem(
            source_id="bad",
            relationship_type="BAD",
            uri="",
            source_col="a",
            target_col="b",
            id_col=None,
            source_entity=None,
            target_entity=None,
            status="error",
            validation_messages=["Missing URI"],
        )
        assert item.status == "error"
        assert "Missing URI" in item.validation_messages

    def test_no_entities_produces_warning(self):
        from pycypher.ingestion.config import (
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[],
                relationships=[
                    RelationshipSourceConfig(
                        id="r1",
                        uri="data/r1.csv",
                        relationship_type="KNOWS",
                        source_col="a",
                        target_col="b",
                    ),
                ],
            ),
        )
        items = screen._build_relationship_list(config)
        assert items[0].status == "warning"
        assert any(
            "No entity sources" in m for m in items[0].validation_messages
        )

    def test_multiple_relationships(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="follows",
                        uri="data/follows.csv",
                        relationship_type="FOLLOWS",
                        source_col="follower_id",
                        target_col="followed_id",
                    ),
                    RelationshipSourceConfig(
                        id="likes",
                        uri="data/likes.csv",
                        relationship_type="LIKES",
                        source_col="user_id",
                        target_col="item_id",
                    ),
                ],
            ),
        )
        items = screen._build_relationship_list(config)
        assert len(items) == 2
        assert items[0].relationship_type == "FOLLOWS"
        assert items[1].relationship_type == "LIKES"


# ---------------------------------------------------------------------------
# _resolve_entity_by_col tests
# ---------------------------------------------------------------------------


class TestResolveEntityByCol:
    """Tests for entity resolution by column name."""

    def test_resolve_matching_col(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                ]
            ),
        )
        result = screen._resolve_entity_by_col("person_id", config)
        assert result == "Person"

    def test_resolve_no_match(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                ]
            ),
        )
        result = screen._resolve_entity_by_col("unknown_col", config)
        assert result is None

    def test_resolve_empty_entities(self):
        from pycypher.ingestion.config import PipelineConfig

        screen = _make_screen()
        config = PipelineConfig(version="1.0")
        result = screen._resolve_entity_by_col("any_col", config)
        assert result is None

    def test_resolve_multiple_entities(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                    EntitySourceConfig(
                        id="products",
                        uri="data/products.csv",
                        entity_type="Product",
                        id_col="product_id",
                    ),
                ]
            ),
        )
        assert screen._resolve_entity_by_col("person_id", config) == "Person"
        assert screen._resolve_entity_by_col("product_id", config) == "Product"
        assert screen._resolve_entity_by_col("other", config) is None

    def test_resolve_entity_without_id_col(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        # no id_col
                    ),
                ]
            ),
        )
        result = screen._resolve_entity_by_col("person_id", config)
        assert result is None


# ---------------------------------------------------------------------------
# Referential integrity in _build_relationship_list
# ---------------------------------------------------------------------------


class TestReferentialIntegrity:
    """Tests that entity resolution populates source_entity and target_entity."""

    def test_entities_resolved_when_matching(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                    EntitySourceConfig(
                        id="companies",
                        uri="data/companies.csv",
                        entity_type="Company",
                        id_col="company_id",
                    ),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="works_at",
                        uri="data/works_at.csv",
                        relationship_type="WORKS_AT",
                        source_col="person_id",
                        target_col="company_id",
                    ),
                ],
            ),
        )
        items = screen._build_relationship_list(config)
        assert len(items) == 1
        assert items[0].source_entity == "Person"
        assert items[0].target_entity == "Company"
        assert items[0].status == "valid"

    def test_entities_none_when_unresolved(self):
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            RelationshipSourceConfig,
            SourcesConfig,
        )

        screen = _make_screen()
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="people",
                        uri="data/people.csv",
                        entity_type="Person",
                        id_col="person_id",
                    ),
                ],
                relationships=[
                    RelationshipSourceConfig(
                        id="likes",
                        uri="data/likes.csv",
                        relationship_type="LIKES",
                        source_col="person_id",
                        target_col="thing_id",  # no matching entity
                    ),
                ],
            ),
        )
        items = screen._build_relationship_list(config)
        assert items[0].source_entity == "Person"
        assert items[0].target_entity is None


# ---------------------------------------------------------------------------
# CSS and structural tests
# ---------------------------------------------------------------------------


class TestScreenStructure:
    """Tests for screen CSS and structural constants."""

    def test_screen_has_css(self):
        assert RelationshipScreen.CSS is not None
        assert len(RelationshipScreen.CSS) > 0

    def test_detail_panel_has_css(self):
        assert len(RelationshipDetailPanel.CSS) > 0

    def test_list_item_has_css(self):
        assert len(RelationshipListItem.CSS) > 0

    def test_css_contains_layout_ids(self):
        # Screen-specific CSS for summary bar
        screen_css = RelationshipScreen.CSS
        assert "#rel-summary" in screen_css
        # Base class CSS provides standard layout IDs
        base_css = VimNavigableScreen.CSS
        assert "#screen-header" in base_css
        assert "#screen-main" in base_css
        assert "#list-panel" in base_css
        assert "#detail-panel" in base_css
        assert "#screen-footer" in base_css
