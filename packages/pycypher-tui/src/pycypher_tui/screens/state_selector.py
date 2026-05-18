"""US state selector screen for the pipeline overview Settings section.

Lets the user pick which state to operate on, so the application can
auto-populate per-state source URIs (contracts, crosswalks, TIGER files).
The screen pulls its state list from
``fastopendata.etl.state_pipeline._STATE_INFO`` when available, with a
small hardcoded fallback so the TUI keeps working when the optional
``fastopendata`` package is not installed.

The screen posts a :class:`StateSelector.StateSelected` message via
``self.app.post_message`` so the message survives this screen being
dismissed in the same tick.
"""

from __future__ import annotations

import logging

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.css.query import NoMatches
from textual.message import Message
from textual.screen import Screen
from textual.widgets import Label, ListItem, ListView

logger = logging.getLogger(__name__)


# Minimal fallback list used when ``fastopendata`` is not installed.  Keeps
# the most common states + DC reachable so the screen is still usable in
# environments without the optional ``fod`` extra.
_FALLBACK_STATE_INFO: dict[str, tuple[str, str]] = {
    "06": ("CA", "California"),
    "12": ("FL", "Florida"),
    "13": ("GA", "Georgia"),
    "17": ("IL", "Illinois"),
    "36": ("NY", "New York"),
    "48": ("TX", "Texas"),
}


def _load_state_info() -> tuple[dict[str, tuple[str, str]], bool]:
    """Try to import the canonical state info, fall back to a small dict.

    Returns ``(state_info, full_catalog_available)``.
    """
    try:
        from fastopendata.etl.state_pipeline import _STATE_INFO  # type: ignore
    except ImportError:
        return dict(_FALLBACK_STATE_INFO), False
    return dict(_STATE_INFO), True


class StateSelector(Screen):
    """Browse all US states by FIPS code and select one.

    Navigation:
        j / Down       Move to next state
        k / Up         Move to previous state
        Enter          Select current state
        q / Escape     Dismiss without selecting
    """

    CSS = """
    StateSelector {
        align: center middle;
    }

    #state-container {
        width: 60%;
        height: 80%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    #state-title {
        text-style: bold;
        color: #7aa2f7;
        height: 1;
        margin-bottom: 1;
    }

    #state-hint {
        color: #565f89;
        height: 1;
        margin-bottom: 1;
    }

    #state-list {
        height: 1fr;
        border: solid #283457;
    }

    #state-list > ListItem {
        padding: 0 1;
        height: auto;
    }

    #state-list > ListItem.--highlight {
        background: #283457;
    }

    .state-row {
        color: #c0caf5;
    }

    .state-empty {
        color: #565f89;
        padding: 1 2;
    }
    """

    BINDINGS = [
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
        Binding("escape", "cancel", "Cancel", show=True),
        Binding("q", "cancel", "Cancel", show=True),
    ]

    class StateSelected(Message):
        """Posted when the user picks a state from the list."""

        def __init__(self, state_fips: str, state_name: str) -> None:
            super().__init__()
            self.state_fips = state_fips
            self.state_name = state_name

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._state_info: dict[str, tuple[str, str]] = {}
        self._full_catalog: bool = False
        # Ordered (fips, abbr, name) tuples shown in the list.
        self._entries: list[tuple[str, str, str]] = []

    # --- Layout ---

    def compose(self) -> ComposeResult:
        with Vertical(id="state-container"):
            yield Label("Select State", id="state-title")
            yield Label(
                "j/k or arrows to navigate · Enter to select · q/Esc to cancel",
                id="state-hint",
            )
            yield ListView(id="state-list")

    # --- Lifecycle ---

    def on_mount(self) -> None:
        self._state_info, self._full_catalog = _load_state_info()

        try:
            list_view = self.query_one("#state-list", ListView)
        except NoMatches:
            logger.debug("on_mount: #state-list not found")
            return

        if not self._state_info:
            list_view.append(
                ListItem(
                    Label(
                        "No states available.",
                        classes="state-empty",
                    )
                )
            )
            return

        # Sort by state name for human-friendly browsing.
        self._entries = sorted(
            ((fips, abbr, name) for fips, (abbr, name) in self._state_info.items()),
            key=lambda row: row[2],
        )

        if not self._full_catalog:
            list_view.append(
                ListItem(
                    Label(
                        "(fastopendata not installed — showing common states only)",
                        classes="state-empty",
                    )
                )
            )

        for fips, abbr, name in self._entries:
            list_view.append(self._build_item(fips, abbr, name))

        list_view.focus()

    # --- Item rendering ---

    @staticmethod
    def _build_item(fips: str, abbr: str, name: str) -> ListItem:
        """Build a ListItem for a single state row."""
        item = ListItem(
            Label(f"{fips} — {name} ({abbr})", classes="state-row")
        )
        # Stash identifying info on the widget so on_list_view_selected
        # can map back to the underlying entry.
        item._state_fips = fips  # type: ignore[attr-defined]
        item._state_name = name  # type: ignore[attr-defined]
        return item

    # --- Actions ---

    def action_cursor_down(self) -> None:
        try:
            self.query_one("#state-list", ListView).action_cursor_down()
        except NoMatches:
            logger.debug("action_cursor_down: #state-list not found")

    def action_cursor_up(self) -> None:
        try:
            self.query_one("#state-list", ListView).action_cursor_up()
        except NoMatches:
            logger.debug("action_cursor_up: #state-list not found")

    def action_cancel(self) -> None:
        self.dismiss()

    # --- Selection handling ---

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Translate a ListView selection into a StateSelected message."""
        state_fips = getattr(event.item, "_state_fips", None)
        state_name = getattr(event.item, "_state_name", None)
        if not state_fips or not state_name:
            return

        message = self.StateSelected(
            state_fips=state_fips,
            state_name=state_name,
        )
        # Send through the app so the message survives this screen being
        # dismissed in the same tick.
        self.app.post_message(message)
        self.dismiss()
