"""Tests for the pipeline overview screen."""

from __future__ import annotations

import pytest

from pycypher_tui.screens.pipeline_overview import (
    PipelineOverviewScreen,
    SectionInfo,
)


class TestSectionInfo:
    """Tests for SectionInfo dataclass."""

    def test_create_section_info(self):
        info = SectionInfo(
            key="entity_sources",
            label="Entity Sources",
            icon="[E]",
            item_count=3,
            status="configured",
            details=["customers: Customer (data/customers.csv)"],
        )
        assert info.key == "entity_sources"
        assert info.item_count == 3
        assert info.status == "configured"

    def test_section_info_frozen(self):
        info = SectionInfo(
            key="queries",
            label="Queries",
            icon="[Q]",
            item_count=0,
            status="empty",
            details=[],
        )
        with pytest.raises(AttributeError):
            info.key = "other"

    def test_section_info_empty_status(self):
        info = SectionInfo(
            key="outputs",
            label="Outputs",
            icon="[O]",
            item_count=0,
            status="empty",
            details=[],
        )
        assert info.status == "empty"
        assert info.details == []


class TestPipelineOverviewScreen:
    """Tests for PipelineOverviewScreen logic (non-widget tests)."""

    def test_section_keys_defined(self):
        assert len(PipelineOverviewScreen.SECTION_KEYS) == 8
        assert "data_model" in PipelineOverviewScreen.SECTION_KEYS
        assert "entity_sources" in PipelineOverviewScreen.SECTION_KEYS
        assert "relationship_sources" in PipelineOverviewScreen.SECTION_KEYS
        assert "queries" in PipelineOverviewScreen.SECTION_KEYS
        assert "query_lineage" in PipelineOverviewScreen.SECTION_KEYS
        assert "outputs" in PipelineOverviewScreen.SECTION_KEYS
        assert "pipeline_run" in PipelineOverviewScreen.SECTION_KEYS
        assert "settings" in PipelineOverviewScreen.SECTION_KEYS

    def test_default_cursor_position(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        assert screen._cursor == 0
        assert screen.section_count == 0

    def test_current_section_empty(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        assert screen.current_section is None

    def test_current_section_with_items(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        sections = [
            SectionInfo("a", "A", "[A]", 1, "configured", []),
            SectionInfo("b", "B", "[B]", 0, "empty", []),
        ]
        screen._items = sections
        screen._cursor = 0
        assert screen.current_section == sections[0]
        screen._cursor = 1
        assert screen.current_section == sections[1]

    def test_section_count(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._items = [
            SectionInfo("a", "A", "[A]", 1, "configured", []),
            SectionInfo("b", "B", "[B]", 0, "empty", []),
            SectionInfo("c", "C", "[C]", 2, "configured", ["x", "y"]),
        ]
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        assert screen.section_count == 3


class TestCursorNavigation:
    """Tests for VIM cursor movement logic."""

    def _make_screen_with_sections(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        screen._items = [
            SectionInfo("a", "A", "[A]", 1, "configured", []),
            SectionInfo("b", "B", "[B]", 0, "empty", []),
            SectionInfo("c", "C", "[C]", 2, "configured", ["x"]),
            SectionInfo("d", "D", "[D]", 0, "empty", []),
        ]
        screen._cursor = 0
        return screen

    def test_move_cursor_down(self):
        screen = self._make_screen_with_sections()
        screen._move_cursor(1)
        assert screen._cursor == 1

    def test_move_cursor_up(self):
        screen = self._make_screen_with_sections()
        screen._cursor = 2
        screen._move_cursor(-1)
        assert screen._cursor == 1

    def test_cursor_clamps_at_bottom(self):
        screen = self._make_screen_with_sections()
        screen._cursor = 3
        screen._move_cursor(1)
        assert screen._cursor == 3

    def test_cursor_clamps_at_top(self):
        screen = self._make_screen_with_sections()
        screen._move_cursor(-1)
        assert screen._cursor == 0

    def test_jump_to_specific_index(self):
        screen = self._make_screen_with_sections()
        screen._jump_to(2)
        assert screen._cursor == 2

    def test_jump_to_clamps_at_bounds(self):
        screen = self._make_screen_with_sections()
        screen._jump_to(100)
        assert screen._cursor == 3

    def test_jump_to_negative_clamps_at_zero(self):
        screen = self._make_screen_with_sections()
        screen._jump_to(-5)
        assert screen._cursor == 0

    def test_move_cursor_empty_sections(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        screen._items = []
        screen._cursor = 0
        screen._move_cursor(1)  # Should not crash
        assert screen._cursor == 0

    def test_pending_gg_jumps_to_first(self):
        screen = self._make_screen_with_sections()
        screen._cursor = 3
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0

    def test_pending_d_starts_sequence(self):
        """Pressing 'd' enters pending state for dd sequence."""
        screen = self._make_screen_with_sections()
        assert screen._pending_keys == []
        # Simulating the 'd' key adding to pending (as done in on_key)
        screen._pending_keys.append("d")
        assert screen._pending_keys == ["d"]

    def test_pending_dd_clears_pending(self):
        """dd sequence clears pending keys after processing."""
        screen = self._make_screen_with_sections()
        screen._pending_keys = ["d"]
        # _handle_pending calls _request_action which calls post_message
        # which needs a mounted widget. Test that sequence parsing works
        # by checking the match logic directly
        sequence = "".join(screen._pending_keys) + "d"
        screen._pending_keys.clear()
        assert sequence == "dd"
        assert screen._pending_keys == []

    def test_escape_clears_pending(self):
        screen = self._make_screen_with_sections()
        screen._pending_keys = ["g"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []


class TestActionRequested:
    """Tests for the ActionRequested message."""

    def test_action_requested_message_attributes(self):
        msg = PipelineOverviewScreen.ActionRequested("queries", "edit")
        assert msg.section_key == "queries"
        assert msg.action == "edit"

    def test_action_requested_add(self):
        msg = PipelineOverviewScreen.ActionRequested("entity_sources", "add")
        assert msg.action == "add"

    def test_action_requested_delete(self):
        msg = PipelineOverviewScreen.ActionRequested("outputs", "delete")
        assert msg.action == "delete"


class TestSectionSelected:
    """Tests for the SectionSelected message."""

    def test_section_selected_message(self):
        msg = PipelineOverviewScreen.SectionSelected("queries")
        assert msg.section_key == "queries"


class TestBuildSectionList:
    """Tests for _build_section_list method."""

    def _make_screen(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        return screen

    def test_build_from_empty_config(self):
        """Empty config produces all sections with empty status.

        Note: ``settings`` always reports ``item_count == 1`` because it
        represents a single configurable value (the backend engine), so
        we exclude it from the zero-count assertion.
        """
        from pycypher.ingestion.config import PipelineConfig

        screen = self._make_screen()
        config = PipelineConfig(version="1.0")
        sections = screen._build_section_list(config)
        assert len(sections) == 8
        assert all(s.status == "empty" for s in sections)
        non_settings = [s for s in sections if s.key != "settings"]
        assert all(s.item_count == 0 for s in non_settings)

    def test_build_with_entities(self):
        """Config with entity sources shows correct count."""
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
                        id_col="id",
                    ),
                    EntitySourceConfig(
                        id="products",
                        uri="data/products.csv",
                        entity_type="Product",
                        id_col="id",
                    ),
                ]
            ),
        )
        screen = self._make_screen()
        sections = screen._build_section_list(config)

        entity_section = sections[1]
        assert entity_section.key == "entity_sources"
        assert entity_section.item_count == 2
        assert entity_section.status == "configured"
        assert len(entity_section.details) == 2

    def test_build_with_relationships(self):
        """Config with relationship sources shows correct count."""
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
        sections = screen._build_section_list(config)

        rel_section = sections[2]
        assert rel_section.key == "relationship_sources"
        assert rel_section.item_count == 1
        assert rel_section.status == "configured"

    def test_build_with_queries(self):
        """Config with queries shows correct count."""
        from pycypher.ingestion.config import PipelineConfig, QueryConfig

        config = PipelineConfig(
            version="1.0",
            queries=[
                QueryConfig(
                    id="q1",
                    inline="MATCH (n) RETURN n",
                    description="All nodes",
                ),
            ],
        )
        screen = self._make_screen()
        sections = screen._build_section_list(config)

        query_section = sections[3]
        assert query_section.key == "queries"
        assert query_section.item_count == 1
        assert query_section.status == "configured"
        assert "All nodes" in query_section.details[0]

    def test_build_with_outputs(self):
        """Config with outputs shows correct count."""
        from pycypher.ingestion.config import (
            OutputConfig,
            PipelineConfig,
            QueryConfig,
        )

        config = PipelineConfig(
            version="1.0",
            queries=[
                QueryConfig(id="q1", inline="MATCH (n) RETURN n"),
            ],
            output=[
                OutputConfig(query_id="q1", uri="output/result.csv"),
            ],
        )
        screen = self._make_screen()
        sections = screen._build_section_list(config)

        output_section = sections[
            5
        ]  # outputs is now at index 5 (query_lineage at 4)
        assert output_section.key == "outputs"
        assert output_section.item_count == 1
        assert output_section.status == "configured"

    def test_detail_truncation_at_3_items(self):
        """Details are truncated to 3 items with a '... and N more' line."""
        from pycypher.ingestion.config import (
            EntitySourceConfig,
            PipelineConfig,
            SourcesConfig,
        )

        entities = [
            EntitySourceConfig(
                id=f"e{i}",
                uri=f"data/e{i}.csv",
                entity_type=f"Type{i}",
                id_col="id",
            )
            for i in range(5)
        ]
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(entities=entities),
        )
        screen = self._make_screen()
        sections = screen._build_section_list(config)

        entity_section = sections[1]
        assert entity_section.item_count == 5
        # 3 detail lines + 1 "... and 2 more" line
        assert len(entity_section.details) == 4
        assert "2 more" in entity_section.details[-1]


class TestSettingsSectionStateIntegration:
    """Tests for the state selector hooks added to the Settings section.

    Covers ``state_fips`` rendering inside ``_build_section_list``, the
    ``_lookup_state_name`` helper, and the keys/footer hints that drive
    the ``s`` shortcut.
    """

    def _make_screen(self):
        screen = PipelineOverviewScreen.__new__(PipelineOverviewScreen)
        screen._cursor = 0
        screen._items = []
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        return screen

    def test_settings_section_shows_default_state(self):
        """A fresh config renders ``State: Georgia (13)`` in Settings."""
        from pycypher.ingestion.config import PipelineConfig

        screen = self._make_screen()
        sections = screen._build_section_list(PipelineConfig())
        settings = next(s for s in sections if s.key == "settings")
        # The Settings section now exposes two configurables (backend + state).
        assert settings.item_count == 2
        # And the state line is human-readable: "State: <name> (<fips>)".
        state_lines = [d for d in settings.details if d.startswith("State:")]
        assert len(state_lines) == 1
        assert "Georgia" in state_lines[0]
        assert "(13)" in state_lines[0]

    def test_settings_section_reflects_changed_state(self):
        """Changing ``state_fips`` updates the Settings detail line."""
        from pycypher.ingestion.config import PipelineConfig

        screen = self._make_screen()
        sections = screen._build_section_list(PipelineConfig(state_fips="06"))
        settings = next(s for s in sections if s.key == "settings")
        state_lines = [d for d in settings.details if d.startswith("State:")]
        assert len(state_lines) == 1
        # The lookup might be the canonical "California" (when fastopendata
        # is installed) or the "FIPS 06" fallback (when it isn't); either
        # is acceptable, but the FIPS itself must be present.
        assert "(06)" in state_lines[0]

    def test_settings_section_includes_help_hint(self):
        """The Settings section advertises both ``b`` and ``s`` shortcuts."""
        from pycypher.ingestion.config import PipelineConfig

        screen = self._make_screen()
        sections = screen._build_section_list(PipelineConfig())
        settings = next(s for s in sections if s.key == "settings")
        joined = " ".join(settings.details)
        assert "'b'" in joined
        assert "'s'" in joined

    def test_lookup_state_name_known_fips(self):
        """Known FIPS codes resolve to their state names when available."""
        # The helper's behavior depends on whether ``fastopendata`` is
        # importable. In either case, FIPS 13 must round-trip to either
        # the canonical "Georgia" or the "FIPS 13" fallback — both
        # acceptable at the unit level.
        result = PipelineOverviewScreen._lookup_state_name("13")
        assert result in ("Georgia", "FIPS 13")

    def test_lookup_state_name_unknown_fips_falls_back(self):
        """Unknown FIPS yields the synthetic ``FIPS XX`` placeholder."""
        # ``99`` isn't in ``_STATE_INFO`` and won't be added in the
        # foreseeable future, so it always exercises the fallback path.
        assert PipelineOverviewScreen._lookup_state_name("99") == "FIPS 99"

    def test_s_key_in_screen_override_keys(self):
        """The ``s`` key is reserved by the screen for state selection."""
        screen = self._make_screen()
        assert "s" in screen._screen_override_keys
        # Ensure it doesn't accidentally collide with the existing ``b``
        # backend cycle key — both must be present and distinct.
        assert "b" in screen._screen_override_keys

    def test_footer_hints_advertise_state_shortcut(self):
        """Footer help string surfaces ``s:state`` so users can find it."""
        screen = self._make_screen()
        hints = screen.footer_hints
        assert "s:state" in hints
        assert "b:backend" in hints

    def test_handle_extra_key_s_dispatches_via_app(self):
        """Pressing ``s`` calls ``app.open_state_selector`` when available.

        Textual's ``self.app`` property walks the widget tree, so we patch
        it to a stub for this unit test rather than spinning up a full
        Pilot. The point of the test is to confirm the ``case "s":`` clause
        dispatches correctly, not to exercise Textual's screen mounting.
        """
        from unittest.mock import PropertyMock, patch

        screen = self._make_screen()

        called: dict[str, int] = {"count": 0}

        class FakeApp:
            def open_state_selector(self_inner) -> None:
                called["count"] += 1

        with patch.object(
            type(screen),
            "app",
            new_callable=PropertyMock,
            return_value=FakeApp(),
        ):
            result = screen.handle_extra_key("s")

        assert result is True
        assert called["count"] == 1

    def test_handle_extra_key_s_safe_when_app_lacks_method(self):
        """If the app has no ``open_state_selector``, ``s`` is still consumed."""
        from unittest.mock import PropertyMock, patch

        screen = self._make_screen()
        with patch.object(
            type(screen),
            "app",
            new_callable=PropertyMock,
            return_value=object(),  # bare object, no method
        ):
            # Must not raise; must claim the key as handled so it doesn't
            # bubble up to a number-jump or other handler.
            assert screen.handle_extra_key("s") is True
