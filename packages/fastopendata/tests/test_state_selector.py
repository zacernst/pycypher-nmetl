"""Tests for the state selector screen.

Covers the parts that don't require a full Textual pilot harness:
- the ``_load_state_info`` helper (real catalog vs. fallback)
- the ``StateSelected`` message contract
- the static ``_build_item`` rendering helper
- the fallback dictionary's content (so the screen stays usable when
  ``fastopendata`` isn't installed).
"""

from __future__ import annotations

import sys
from unittest.mock import patch

import pytest

from fastopendata.tui.state_selector import (
    StateSelector,
    _FALLBACK_STATE_INFO,
    _load_state_info,
)


class TestLoadStateInfo:
    """Tests for the ``_load_state_info`` helper."""

    def test_returns_full_catalog_when_fastopendata_available(self):
        """When ``fastopendata`` imports cleanly, the full state catalog is used.

        The full catalog contains all 50 states + DC; the fallback contains
        only 6 — so size alone disambiguates the two paths.
        """
        info, full = _load_state_info()
        # In this test environment fastopendata IS installed, so we must
        # take the full path.
        assert full is True
        # Sanity: at least all 50 states + DC.
        assert len(info) >= 51
        # Spot-check well-known FIPS codes.
        assert info["13"][1] == "Georgia"
        assert info["06"][1] == "California"
        assert info["48"][1] == "Texas"

    def test_falls_back_to_minimal_dict_on_import_error(self):
        """When ``fastopendata.etl.state_pipeline`` cannot be imported,
        ``_load_state_info`` falls back to the hardcoded dict and reports
        ``full=False`` so the UI can warn the user.
        """
        # Hide the module from importlib for the duration of this call.
        with patch.dict(sys.modules, {"fastopendata.etl.state_pipeline": None}):
            info, full = _load_state_info()
        assert full is False
        # The fallback dict is small and matches _FALLBACK_STATE_INFO.
        assert info == _FALLBACK_STATE_INFO

    def test_fallback_dict_is_non_empty(self):
        """The fallback dict must have enough states to be useful."""
        # We use 6 states + DC as the floor — anything less and the
        # screen would be effectively unusable without the optional dep.
        assert len(_FALLBACK_STATE_INFO) >= 5
        # And Georgia (the default FIPS) must always be reachable.
        assert "13" in _FALLBACK_STATE_INFO
        assert _FALLBACK_STATE_INFO["13"] == ("GA", "Georgia")


class TestStateSelectedMessage:
    """Contract tests for the ``StateSelector.StateSelected`` message."""

    def test_carries_fips_and_name(self):
        """Message exposes ``state_fips`` and ``state_name`` attributes."""
        msg = StateSelector.StateSelected(state_fips="06", state_name="California")
        assert msg.state_fips == "06"
        assert msg.state_name == "California"

    def test_message_is_a_textual_message(self):
        """The message subclasses Textual's ``Message`` so post_message works."""
        from textual.message import Message

        msg = StateSelector.StateSelected(state_fips="13", state_name="Georgia")
        assert isinstance(msg, Message)


class TestBuildItem:
    """Tests for the static ``_build_item`` rendering helper."""

    def test_build_item_shows_fips_name_and_abbrev(self):
        """The label includes the FIPS, full name, and abbreviation.

        ListItem doesn't ``compose`` until it's mounted in an App, so we
        inspect the children passed at construction time directly.
        """
        item = StateSelector._build_item("06", "CA", "California")
        from textual.widgets import Label
        # ListItem stores its constructor children in ``_nodes`` /
        # ``children`` once attached, but pre-mount it's the
        # ``_pending_children`` we passed in.
        children = list(getattr(item, "_pending_children", []) or item.children)
        labels = [c for c in children if isinstance(c, Label)]
        assert len(labels) == 1
        text = str(labels[0].renderable)
        assert "06" in text
        assert "California" in text
        assert "CA" in text

    def test_build_item_stashes_fips_and_name(self):
        """The item carries the picked FIPS and name as attrs for selection."""
        item = StateSelector._build_item("48", "TX", "Texas")
        assert getattr(item, "_state_fips") == "48"
        assert getattr(item, "_state_name") == "Texas"


class TestScreenBindings:
    """Confirm the navigation contract advertised in the docstring."""

    def test_q_and_escape_bound_to_cancel(self):
        """Both ``q`` and Escape dismiss the screen without selecting."""
        keys = {b.key: b.action for b in StateSelector.BINDINGS}
        assert keys.get("q") == "cancel"
        assert keys.get("escape") == "cancel"

    def test_jk_navigation_bound(self):
        """``j``/``k`` move the cursor (matching the rest of the TUI)."""
        keys = {b.key: b.action for b in StateSelector.BINDINGS}
        assert keys.get("j") == "cursor_down"
        assert keys.get("k") == "cursor_up"


class TestEndToEndConfigUpdate:
    """Cross-cutting Phase 3 check: the state selector wiring updates
    ``PipelineConfig.state_fips`` end-to-end via the ConfigManager API
    that the app handler uses on selection.

    We don't drive the Textual screen here — that needs a pilot — but
    we do exercise the same code path the handler runs once it receives
    the message.
    """

    def test_selection_updates_pipeline_config_state_fips(self):
        """Simulating the app handler's logic flips ``state_fips`` correctly."""
        from pycypher_tui.config.pipeline import ConfigManager

        mgr = ConfigManager()
        # Pretend the user just picked California in the StateSelector.
        msg = StateSelector.StateSelected(state_fips="06", state_name="California")
        mgr.set_state_fips(msg.state_fips)

        cfg = mgr.get_config()
        assert cfg.state_fips == "06"
        assert mgr.get_state_fips() == "06"

    def test_selection_round_trip_for_multiple_states(self):
        """Switching states multiple times leaves the config in the last state."""
        from pycypher_tui.config.pipeline import ConfigManager

        mgr = ConfigManager()
        for fips in ("06", "48", "36", "13"):
            mgr.set_state_fips(fips)
            assert mgr.get_state_fips() == fips
        # Last write wins.
        assert mgr.get_config().state_fips == "13"
