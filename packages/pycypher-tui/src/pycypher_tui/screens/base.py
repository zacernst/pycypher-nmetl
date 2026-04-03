"""Base screen abstractions eliminating duplication across list-detail screens.

Provides:
- VimNavigableScreen: Base for all screens with VIM j/k navigation, cursor
  management, ModeManager command dispatch, and two-column layout.
- BaseListItem: Generic focusable list item widget.
- BaseDetailPanel: Generic right-side detail panel widget.

Key handling is routed through ModeManager (from the app's modal system)
so that screens act as command dispatchers rather than key handlers.
NormalMode produces command strings like ``navigate:down`` and
``edit:delete_line`` which VimNavigableScreen translates into cursor
movement and CRUD callbacks.  Screen-specific keys that conflict with
NormalMode's text-editor semantics (e.g. ``a`` for "add item" rather
than "append after cursor") are intercepted before reaching ModeManager.

These abstractions consolidate ~300 lines of identical code that was
previously copy-pasted across DataSourcesScreen, EntityTablesScreen,
RelationshipScreen, and TemplateBrowserScreen.
"""

from __future__ import annotations

import logging
import re
from abc import abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, Static

logger = logging.getLogger(__name__)

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.widgets.dialog import ConfirmDialog, DialogResult

if TYPE_CHECKING:
    from pycypher_tui.modes.base import KeyResult

T = TypeVar("T")


class BaseListItem(Static, Generic[T]):
    """Focusable list item with consistent highlight behavior.

    Subclasses must:
    - Accept a data item and store it
    - Override compose() to render the item's display
    """

    focused: reactive[bool] = reactive(False)

    CSS = """
    BaseListItem {
        width: 100%;
        padding: 0 2;
    }

    BaseListItem.item-focused {
        background: #283457;
    }
    """

    def watch_focused(self, focused: bool) -> None:
        if focused:
            self.add_class("item-focused")
        else:
            self.remove_class("item-focused")


class BaseDetailPanel(Static):
    """Right-side detail panel with consistent styling.

    Subclasses override update_item() to render item details.
    """

    CSS = """
    BaseDetailPanel {
        width: 1fr;
        height: 100%;
        padding: 1 2;
        border-left: solid #283457;
    }

    BaseDetailPanel .detail-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        margin-bottom: 1;
    }

    BaseDetailPanel .detail-row {
        width: 100%;
        color: #a9b1d6;
    }

    BaseDetailPanel .detail-section {
        text-style: bold;
        color: #e0af68;
        width: 100%;
        margin-top: 1;
    }

    BaseDetailPanel .detail-label {
        color: #565f89;
    }

    BaseDetailPanel .detail-value {
        color: #c0caf5;
    }
    """

    def __init__(self, empty_message: str = "(no item selected)", **kwargs) -> None:
        super().__init__(**kwargs)
        self._empty_message = empty_message

    def compose(self) -> ComposeResult:
        yield Label(self._empty_message, classes="detail-title")

    async def _clear_and_show_empty(self) -> None:
        """Reset panel to empty state."""
        await self.remove_children()
        await self.mount(Label(self._empty_message, classes="detail-title"))


class VimNavigableScreen(Widget, Generic[T]):
    """Base screen providing VIM-style list navigation over items of type T.

    Eliminates the ~200 lines of duplicated cursor management, key handling,
    pending key sequences, and list rendering that existed across all
    list-detail screens.

    Subclasses must implement:
    - screen_title / breadcrumb_text: Display strings
    - footer_hints: Key hint text for the footer
    - load_items(): Load items from config
    - create_list_item(item, id): Create a widget for a list item
    - update_detail(item): Update the detail panel
    - create_detail_panel(): Create the detail panel widget
    - on_edit(item): Handle Enter/l on current item
    - on_add(): Handle 'a' key
    - on_delete(item): Handle 'dd' on current item

    Subclasses may override:
    - handle_extra_key(key): Handle screen-specific keys beyond the standard set
    - list_panel_id / detail_panel_id: CSS IDs for the panels
    - empty_list_message: Message when no items exist
    """

    class NavigateBack(Message):
        """Request to navigate back to parent screen."""

    def __init__(self, config_manager: ConfigManager, **kwargs) -> None:
        super().__init__(**kwargs)
        self._config_manager = config_manager
        self._cursor: int = 0
        self._items: list[T] = []
        self._pending_keys: list[str] = []
        self._search_pattern: str = ""
        self._search_matches: list[int] = []
        self._search_match_idx: int = -1

    # --- Subclass configuration (override these) ---

    @property
    @abstractmethod
    def screen_title(self) -> str:
        """Title displayed in the screen header."""

    @property
    @abstractmethod
    def breadcrumb_text(self) -> str:
        """Breadcrumb path text (e.g., 'Pipeline > Data Sources')."""

    @property
    @abstractmethod
    def footer_hints(self) -> str:
        """Key hint text for the screen footer."""

    @property
    def list_panel_id(self) -> str:
        return "list-panel"

    @property
    def detail_panel_id(self) -> str:
        return "detail-panel"

    @property
    def empty_list_message(self) -> str:
        return "No items configured.\nPress 'a' to add."

    # --- Item access ---

    @property
    def item_count(self) -> int:
        return len(self._items)

    @property
    def current_item(self) -> T | None:
        if 0 <= self._cursor < self.item_count:
            return self._items[self._cursor]
        return None

    @property
    def items(self) -> list[T]:
        return self._items

    @property
    def config_manager(self) -> ConfigManager:
        return self._config_manager

    # --- Abstract methods for subclasses ---

    @abstractmethod
    def load_items(self) -> list[T]:
        """Load and return the list of items from the config manager."""

    @abstractmethod
    def create_list_item(self, item: T, item_id: str) -> BaseListItem:
        """Create a list item widget for the given data item.

        Args:
            item: The data item to display.
            item_id: A unique CSS-safe ID string for this widget.
        """

    @abstractmethod
    def create_detail_panel(self) -> BaseDetailPanel:
        """Create the detail panel widget for this screen."""

    @abstractmethod
    def update_detail_panel(self, item: T | None) -> None:
        """Update the detail panel to show the given item (or clear it)."""

    @abstractmethod
    def get_item_id(self, item: T) -> str:
        """Return a unique CSS-safe identifier string for an item."""

    @abstractmethod
    def on_edit(self, item: T) -> None:
        """Handle edit action (Enter/l) on the given item."""

    @abstractmethod
    def on_add(self) -> None:
        """Handle add action ('a' key)."""

    @abstractmethod
    def on_delete(self, item: T) -> None:
        """Handle delete action ('dd') on the given item."""

    def handle_extra_key(self, key: str) -> bool:
        """Handle screen-specific keys beyond standard VIM navigation.

        Returns True if the key was handled, False otherwise.
        Override in subclasses for screen-specific keys (e.g., Tab for filters).
        """
        return False

    def get_item_search_text(self, item: T) -> str:
        """Return a searchable text representation of an item.

        Override in subclasses for meaningful search across item fields.
        Default uses ``get_item_id``.
        """
        return self.get_item_id(item)

    # --- Search integration ---

    def apply_search(self, pattern: str) -> None:
        """Apply a search pattern and jump to the first match.

        Called when the user submits a ``/pattern`` search from command mode.
        """
        self._search_pattern = pattern
        self._search_matches.clear()
        self._search_match_idx = -1

        if not pattern:
            return

        try:
            regex = re.compile(pattern, re.IGNORECASE)
        except re.error:
            # Invalid regex — treat as literal substring
            regex = re.compile(re.escape(pattern), re.IGNORECASE)

        for i, item in enumerate(self._items):
            text = self.get_item_search_text(item)
            if regex.search(text):
                self._search_matches.append(i)

        if self._search_matches:
            self._search_match_idx = 0
            self._jump_to(self._search_matches[0])

    def search_next(self) -> None:
        """Jump to the next search match (``n`` key)."""
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx + 1) % len(self._search_matches)
        self._jump_to(self._search_matches[self._search_match_idx])

    def search_prev(self) -> None:
        """Jump to the previous search match (``N`` key)."""
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx - 1) % len(self._search_matches)
        self._jump_to(self._search_matches[self._search_match_idx])

    @property
    def search_status(self) -> str:
        """Human-readable search status for display."""
        if not self._search_pattern:
            return ""
        if not self._search_matches:
            return f"/{self._search_pattern} (no matches)"
        return (
            f"/{self._search_pattern} "
            f"[{self._search_match_idx + 1}/{len(self._search_matches)}]"
        )

    # --- Delete confirmation ---

    def _confirm_and_delete(self, item: T) -> None:
        """Show a confirmation dialog before deleting an item.

        Uses the ConfirmDialog widget, preventing accidental data loss
        from the ``dd`` key sequence.
        """
        item_id = self.get_item_id(item)

        async def _on_confirm(response):
            if response.result == DialogResult.CONFIRMED:
                await self.on_delete(item)

        self.app.push_screen(
            ConfirmDialog(
                title="Confirm Delete",
                body=f"Delete '{item_id}'?",
            ),
            callback=_on_confirm,
        )

    # --- Register integration ---

    def _yank_current_item(self) -> None:
        """Yank the current item's ID into the register file.

        If the app has a register file available, stores the item ID in
        the unnamed and yank registers.
        """
        item = self.current_item
        if item is None:
            return

        text = self.get_item_id(item)
        try:
            registers = getattr(self.app, "register_file", None)
            if registers is not None:
                registers.yank(text)
        except AttributeError:
            # App not fully mounted or register_file missing
            pass

    # --- Layout ---

    def compose(self) -> ComposeResult:
        # Load items during compose to avoid DuplicateIds from mount() in on_mount
        self._items = self.load_items()
        if self._cursor >= self.item_count:
            self._cursor = max(0, self.item_count - 1)

        with Vertical(id="screen-header"):
            yield Label(self.screen_title, id="screen-title")
            yield Label(self.breadcrumb_text, id="screen-breadcrumb")

        with Horizontal(id="screen-main"):
            with VerticalScroll(id=self.list_panel_id):
                if not self._items:
                    yield Label(self.empty_list_message, classes="empty-list-message")
                else:
                    for i, item in enumerate(self._items):
                        item_id = self.get_item_id(item)
                        widget = self.create_list_item(item, f"item-{item_id}")
                        if i == self._cursor:
                            widget.focused = True
                        yield widget
            yield self.create_detail_panel()

        yield Static(self.footer_hints, id="screen-footer")

    CSS = """
    VimNavigableScreen {
        layout: vertical;
    }

    #screen-header {
        width: 100%;
        height: 2;
        background: #1a1b26;
        border-bottom: solid #283457;
        padding: 0 2;
    }

    #screen-title {
        text-style: bold;
        color: #7aa2f7;
        width: 100%;
        height: 1;
    }

    #screen-breadcrumb {
        color: #565f89;
        width: 100%;
        height: 1;
    }

    #screen-main {
        width: 100%;
        height: 1fr;
    }

    #list-panel {
        width: 2fr;
        height: 100%;
    }

    #detail-panel {
        width: 1fr;
        height: 100%;
    }

    #screen-footer {
        dock: bottom;
        height: 1;
        width: 100%;
        color: #565f89;
        padding: 0 2;
    }

    .empty-list-message {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: #565f89;
    }
    """

    # --- Lifecycle ---

    def on_mount(self) -> None:
        # Items already populated in compose(); just update detail and set focus
        self.update_detail_panel(self.current_item)
        try:
            self.query_one(f"#{self.list_panel_id}", VerticalScroll).focus()
        except (NoMatches, AttributeError):
            logger.debug("on_mount: list panel #%s not found for focus", self.list_panel_id)

    async def refresh_from_config(self) -> None:
        """Reload items from config and update the display."""
        self._items = self.load_items()

        if self._cursor >= self.item_count:
            self._cursor = max(0, self.item_count - 1)

        await self._render_list()
        self.update_detail_panel(self.current_item)

    # --- List rendering ---

    async def _render_list(self) -> None:
        """Render item widgets in the list panel."""
        try:
            panel = self.query_one(f"#{self.list_panel_id}", VerticalScroll)
        except (NoMatches, AttributeError):
            logger.debug("_render_list: list panel #%s not found", self.list_panel_id)
            return

        await panel.remove_children()

        if not self._items:
            await panel.mount(Label(self.empty_list_message, classes="empty-list-message"))
            return

        for i, item in enumerate(self._items):
            item_id = self.get_item_id(item)
            widget = self.create_list_item(item, f"item-{item_id}")
            await panel.mount(widget)
            if i == self._cursor:
                widget.focused = True

    # --- Cursor management ---

    def _move_cursor(self, delta: int) -> None:
        if self.item_count == 0:
            return
        old = self._cursor
        self._cursor = max(0, min(self._cursor + delta, self.item_count - 1))
        if old != self._cursor:
            self._update_focus(old, self._cursor)
            self.update_detail_panel(self.current_item)

    def _jump_to(self, index: int) -> None:
        if self.item_count == 0:
            return
        old = self._cursor
        self._cursor = max(0, min(index, self.item_count - 1))
        if old != self._cursor:
            self._update_focus(old, self._cursor)
            self.update_detail_panel(self.current_item)

    def _update_focus(self, old_index: int = -1, new_index: int = -1) -> None:
        """Update visual focus indicator on list items.

        When *old_index* and *new_index* are provided, only the two
        affected widgets are touched — O(1) instead of O(n).
        """
        if old_index >= 0 and new_index >= 0:
            for idx, focused in ((old_index, False), (new_index, True)):
                if 0 <= idx < self.item_count:
                    item_id = self.get_item_id(self._items[idx])
                    try:
                        widget = self.query_one(f"#item-{item_id}", BaseListItem)
                        widget.focused = focused
                    except (NoMatches, AttributeError):
                        logger.debug("_update_focus: widget #item-%s not found", item_id)
        else:
            for i, item in enumerate(self._items):
                item_id = self.get_item_id(item)
                try:
                    widget = self.query_one(f"#item-{item_id}", BaseListItem)
                    widget.focused = i == self._cursor
                except (NoMatches, AttributeError):
                    logger.debug("_update_focus: widget #item-%s not found", item_id)

    # --- Key handling (routed through ModeManager) ---

    # Keys that have screen-specific meaning and must NOT be forwarded
    # to NormalMode (which would interpret them as text-editor actions).
    # Subclasses can extend via ``_screen_override_keys``.
    _SCREEN_OVERRIDE_KEYS: frozenset[str] = frozenset({
        "a",        # add item  (NormalMode: "append after cursor")
        "ctrl+f",   # page down (NormalMode: no binding)
        "ctrl+b",   # page up   (NormalMode: no binding)
        "n",        # search next match
        "N",        # search previous match
    })

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        """Additional keys this screen intercepts before ModeManager.

        Override in subclasses to add screen-specific keys (e.g. Tab).
        Must return a frozenset of Textual key identifiers.
        """
        return frozenset()

    @property
    def _all_override_keys(self) -> frozenset[str]:
        """Combined override keys (base + screen-specific), cached after first access."""
        try:
            return self.__cached_override_keys
        except AttributeError:
            self.__cached_override_keys = self._SCREEN_OVERRIDE_KEYS | self._screen_override_keys
            return self.__cached_override_keys

    def _get_mode_manager(self):
        """Retrieve the ModeManager from the running Textual App.

        Returns None when the screen is not mounted (e.g. in unit tests
        that bypass ``__init__`` via ``__new__``).
        """
        try:
            app = self.app
            return getattr(app, "mode_manager", None)
        except (AttributeError, TypeError):
            return None

    def on_key(self, event) -> None:
        """Route key events through ModeManager, then dispatch commands.

        Flow:
        1. If ModeManager has pending keys (multi-key sequence in
           progress), forward the key to ModeManager unconditionally.
        2. If the key is a screen-level override (e.g. ``a`` for add),
           handle it directly without touching ModeManager.
        3. Otherwise route through ModeManager.  If it returns a command
           string, dispatch it.  If it requests a mode transition, let
           the event bubble up to the App (which owns mode transitions).
        4. If ModeManager didn't handle the key, try ``handle_extra_key``.

        When no ModeManager is available (unit tests that create screens
        via ``__new__``), keys are not processed here — tests call internal
        methods directly (``_move_cursor``, ``_jump_to``, ``_handle_pending``).
        """
        mgr = self._get_mode_manager()

        if mgr is None:
            return

        # Step 1: pending multi-key sequences in ModeManager take priority
        from pycypher_tui.modes.base import ModeType

        if mgr.current_type != ModeType.NORMAL:
            # Non-NORMAL modes are handled entirely by the App
            return

        normal_mode = mgr.get_mode(ModeType.NORMAL)
        if normal_mode._pending_keys:
            result = mgr.handle_key(event.key)
            if result.handled:
                self._dispatch_command(result.command)
                event.prevent_default()
                event.stop()
            return

        # Step 2: screen-level override keys (cached to avoid set union per keystroke)
        if event.key in self._all_override_keys:
            handled = self._handle_screen_key(event.key)
            if handled:
                event.prevent_default()
                event.stop()
            return

        # Step 3: route through ModeManager
        result = mgr.handle_key(event.key)

        if result.transition_to is not None:
            # Mode transition -- let the App's on_key handle it.
            # ModeManager already executed the transition, so just stop.
            event.prevent_default()
            event.stop()
            return

        if result.handled:
            self._dispatch_command(result.command)
            # Escape with no command means "navigate back"
            if event.key == "escape" and result.command is None:
                self.post_message(self.NavigateBack())
            event.prevent_default()
            event.stop()
            return

        # Step 4: unhandled by ModeManager -- try screen extras
        if self.handle_extra_key(event.key):
            event.prevent_default()
            event.stop()

    def _handle_screen_key(self, key: str) -> bool:
        """Handle a screen-level override key.

        Returns True if the key was consumed.
        """
        match key:
            case "a":
                self.on_add()
                return True
            case "ctrl+f":
                self._move_cursor(5)
                return True
            case "ctrl+b":
                self._move_cursor(-5)
                return True
            case "n":
                self.search_next()
                return True
            case "N":
                self.search_prev()
                return True
            case _:
                return self.handle_extra_key(key)

    def _dispatch_command(self, command: str | None) -> None:
        """Translate a ModeManager command string into a screen action.

        Command strings follow the ``category:action`` convention
        produced by NormalMode.
        """
        if command is None:
            return

        match command:
            # Navigation
            case "navigate:down":
                self._move_cursor(1)
            case "navigate:up":
                self._move_cursor(-1)
            case "navigate:left":
                self.post_message(self.NavigateBack())
            case "navigate:right":
                item = self.current_item
                if item is not None:
                    self.on_edit(item)
            case "navigate:first":
                self._jump_to(0)
            case "navigate:last":
                self._jump_to(self.item_count - 1)

            # Actions
            case "action:confirm":
                item = self.current_item
                if item is not None:
                    self.on_edit(item)

            # Editing — delete with confirmation dialog
            case "edit:delete_line":
                item = self.current_item
                if item is not None:
                    self._confirm_and_delete(item)

            # Clipboard — yank current item ID to register
            case "clipboard:yank":
                self._yank_current_item()

            # Clipboard — paste (no-op for list screens)
            case "clipboard:paste":
                pass

            # Everything else: no-op for list screens
            case _:
                # Handle ex-commands that come through (search results)
                if command.startswith("ex:/"):
                    pattern = command[4:]
                    self.apply_search(pattern)
                elif command == "search:next":
                    self.search_next()
                elif command == "search:prev":
                    self.search_prev()

    # --- Multi-key sequence handling (used by tests and fallback) ---

    def _handle_pending(self, key: str) -> None:
        """Handle multi-key sequences (gg, dd)."""
        if key == "escape":
            self._pending_keys.clear()
            return

        sequence = "".join(self._pending_keys) + key
        self._pending_keys.clear()

        match sequence:
            case "gg":
                self._jump_to(0)
            case "dd":
                item = self.current_item
                if item is not None:
                    self._confirm_and_delete(item)
