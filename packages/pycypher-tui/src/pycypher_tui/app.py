"""Main PyCypher TUI application."""

from __future__ import annotations

import logging
from pathlib import Path

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal
from textual.css.query import NoMatches
from textual.events import Key
from textual.reactive import reactive
from textual.widgets import Header, Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.widgets.dialog import ConfirmDialog, DialogResult
from pycypher_tui.config.validation import CachedValidator
from pycypher_tui.modes.base import ModeType
from pycypher_tui.modes.manager import ModeManager
from pycypher_tui.modes.registers import RegisterFile
from pycypher_tui.modes.search_replace import parse_substitute_command
from pycypher_tui.screens.base import VimNavigableScreen
from pycypher_tui.screens.editable_base import VimEditableScreen
from pycypher_tui.screens.data_model import DataModelScreen
from pycypher_tui.screens.data_sources import DataSourcesScreen
from pycypher_tui.screens.entity_browser import EntityBrowserScreen
from pycypher_tui.screens.entity_editor import EntityEditorScreen
from pycypher_tui.screens.pipeline_overview import PipelineOverviewScreen
from pycypher_tui.screens.pipeline_testing import PipelineTestingScreen
from pycypher_tui.screens.query_lineage import QueryLineageScreen
from pycypher_tui.screens.relationship_browser import RelationshipBrowserScreen
from pycypher_tui.screens.relationship_editor import RelationshipEditorScreen
from pycypher_tui.screens.source_mapper import DataSourceMapperScreen

try:
    from fastopendata.tui.fod_catalog import FodCatalogScreen
except ImportError:
    FodCatalogScreen = None  # type: ignore[assignment,misc]
try:
    from pycypher_tui.widgets.help_system import HelpRegistry, HelpScreen
except ImportError:
    # help_system module may have been removed/relocated
    HelpRegistry = None  # type: ignore[assignment,misc]
    HelpScreen = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class ModeIndicator(Static):
    """Status bar widget showing the current VIM mode."""

    mode_name: reactive[str] = reactive("NORMAL")
    mode_color: reactive[str] = reactive("#7aa2f7")

    def render(self) -> str:
        return f" {self.mode_name} "

    def watch_mode_color(self, color: str) -> None:
        self.styles.background = color
        self.styles.color = "#1a1b26"


class CommandLine(Static):
    """Command line widget for ex-commands."""

    text: reactive[str] = reactive("")

    def render(self) -> str:
        return self.text if self.text else ""


class StatusBar(Static):
    """Bottom status bar with mode, file path, and hints."""

    file_path: reactive[str] = reactive("")
    validation_status: reactive[str] = reactive("")

    def compose(self) -> ComposeResult:
        with Horizontal(id="status-bar-content"):
            yield ModeIndicator(id="mode-indicator")
            yield Label("", id="status-file-path")
            yield Label("", id="status-validation")
            yield Label("", id="status-hints")

    def update_file_path(self, path: str) -> None:
        self.file_path = path
        try:
            self.query_one("#status-file-path", Label).update(
                f" {path}" if path else ""
            )
        except NoMatches:
            logger.debug("StatusBar: #status-file-path not mounted yet")

    def update_validation(self, status: str) -> None:
        self.validation_status = status
        try:
            self.query_one("#status-validation", Label).update(
                f" {status}" if status else ""
            )
        except NoMatches:
            logger.debug("StatusBar: #status-validation not mounted yet")

    def update_hints(self, hints: str) -> None:
        try:
            self.query_one("#status-hints", Label).update(
                f" {hints}" if hints else ""
            )
        except NoMatches:
            logger.debug("StatusBar: #status-hints not mounted yet")


class PyCypherTUI(App):
    """VIM-style TUI for PyCypher ETL pipeline configuration.

    A terminal-based interface for building and editing ETL
    pipeline configurations with VIM-style modal navigation.
    """

    TITLE = "PyCypher ETL Configuration"
    SUB_TITLE = "VIM-style Pipeline Builder"

    CSS = """
    #status-bar-content {
        dock: bottom;
        height: 1;
        width: 100%;
    }

    #mode-indicator {
        width: auto;
        padding: 0 1;
        text-style: bold;
        background: #7aa2f7;
        color: #1a1b26;
    }

    #status-file-path {
        width: 1fr;
        color: #a9b1d6;
    }

    #status-validation {
        width: auto;
        color: #9ece6a;
    }

    #status-hints {
        width: auto;
        color: #565f89;
    }

    #command-line {
        dock: bottom;
        height: 1;
        display: none;
        color: #c0caf5;
        background: #1a1b26;
    }

    #command-line.visible {
        display: block;
    }

    #main-content {
        width: 100%;
        height: 1fr;
    }

    #welcome-message {
        content-align: center middle;
        width: 100%;
        height: 100%;
        color: #565f89;
    }
    """

    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=False),
    ]

    # Named screens (for push_screen("name")).  ``fod_catalog`` is only
    # registered when the optional ``fastopendata`` dependency imported
    # successfully at module load.
    SCREENS = {}  # type: dict[str, type]
    if FodCatalogScreen is not None:
        SCREENS["fod_catalog"] = FodCatalogScreen

    def __init__(
        self,
        config_path: str | Path | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mode_manager = ModeManager()
        self.register_file = RegisterFile()
        self.validator = CachedValidator()
        self.config_path = Path(config_path) if config_path else None
        self._config_manager: ConfigManager | None = None
        self._help_registry = (
            HelpRegistry() if HelpRegistry is not None else None
        )
        self.mode_manager.add_listener(self._on_mode_change)

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Static(
                "Welcome to PyCypher TUI\n\n"
                "Press : for commands, i for insert mode\n"
                "Press :q to quit, :e <file> to open",
                id="welcome-message",
            ),
            id="main-content",
        )
        yield StatusBar(id="status-bar")
        yield CommandLine(id="command-line")

    async def on_mount(self) -> None:
        """Initialize the application on mount."""
        self._update_mode_display()
        if self.config_path and self._config_manager is None:
            await self._open_config(str(self.config_path))

    async def on_key(self, event: Key) -> None:
        """Route key events through the mode manager and active content.

        Architecture:
        - In NORMAL mode, routes keys through ModeManager to produce
          command strings, then forwards those commands to the active
          content widget (VimNavigableScreen or PipelineOverviewScreen).
          Keys not handled by ModeManager are forwarded directly to
          the active content for screen-specific handling (e.g. number
          keys for section jump, ctrl+f/b for page navigation).
        - Non-NORMAL modes (INSERT, COMMAND, VISUAL) are handled
          entirely by ModeManager since they don't involve screen
          navigation.
        """
        current_mode = self.mode_manager.current_type

        if current_mode == ModeType.NORMAL:
            result = self.mode_manager.handle_key(event.key)

            if result.command:
                await self._execute_command(result.command)

            if result.handled:
                # Escape in NORMAL mode with no command: forward to
                # the active content as "navigate back" so screens
                # can return to their parent.
                if event.key == "escape" and not result.command:
                    self._dispatch_to_content("navigate:left")
                event.prevent_default()
                event.stop()
            else:
                # ModeManager didn't handle it — forward to active content
                # for screen-specific keys (number keys, ctrl+f/b, etc.)
                if self._forward_key_to_content(event.key):
                    event.prevent_default()
                    event.stop()

            self._update_command_line()
            return

        # Non-NORMAL modes: ModeManager handles everything
        result = self.mode_manager.handle_key(event.key)

        if result.handled:
            event.prevent_default()
            event.stop()

        if result.command:
            await self._execute_command(result.command)

        # Escape in any mode navigates back once the mode transitions to
        # NORMAL.  Without this, the user has to press Escape twice: once
        # to leave INSERT/COMMAND, then again to navigate back.
        if event.key == "escape" and self.mode_manager.current_type == ModeType.NORMAL:
            self._dispatch_to_content("navigate:left")

        self._update_command_line()

    def _on_mode_change(self, old_mode: ModeType, new_mode: ModeType) -> None:
        """Callback for mode transitions."""
        self._update_mode_display()
        self._update_command_line()
        self._update_hints(new_mode)

    def _update_mode_display(self) -> None:
        """Update the mode indicator in the status bar."""
        try:
            indicator = self.query_one("#mode-indicator", ModeIndicator)
            indicator.mode_name = self.mode_manager.display_name
            indicator.mode_color = self.mode_manager.style_color
        except NoMatches:
            logger.debug("_update_mode_display: #mode-indicator not found")

    def _update_command_line(self) -> None:
        """Show/hide command line based on current mode."""
        try:
            cmd_line = self.query_one("#command-line", CommandLine)
            if self.mode_manager.current_type == ModeType.COMMAND:
                from pycypher_tui.modes.command import (
                    CommandMode,
                )

                cmd_mode = self.mode_manager.get_mode(ModeType.COMMAND)
                assert isinstance(cmd_mode, CommandMode)
                cmd_line.text = cmd_mode.display_text
                cmd_line.add_class("visible")
            else:
                cmd_line.text = ""
                cmd_line.remove_class("visible")
        except NoMatches:
            logger.debug("_update_command_line: #command-line not found")

    def _update_hints(self, mode: ModeType) -> None:
        """Update contextual hints for the current mode."""
        hints = {
            ModeType.NORMAL: "hjkl:move  i:insert  ::command  ?:help",
            ModeType.INSERT: "Esc:normal  Type to edit",
            ModeType.VISUAL: "hjkl:select  y:yank  d:delete  Esc:cancel",
            ModeType.COMMAND: "Enter:execute  Esc:cancel",
        }
        try:
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_hints(hints.get(mode, ""))
        except NoMatches:
            logger.debug("_update_hints: #status-bar not found")

    async def _execute_command(self, command: str) -> None:
        """Execute a command string from the mode system.

        Commands follow the format 'category:action'.
        Ex-commands start with 'ex:'.
        Navigation/action commands are forwarded to the active content.
        """
        if command.startswith("ex:"):
            await self._execute_ex_command(command[3:])
        elif command == "command:search":
            # Set the command mode prefix to / for search
            from pycypher_tui.modes.command import CommandMode

            cmd_mode = self.mode_manager.get_mode(ModeType.COMMAND)
            if isinstance(cmd_mode, CommandMode):
                cmd_mode.prefix = "/"
        else:
            # Forward navigation/action/clipboard commands to active content
            self._dispatch_to_content(command)

    def _dispatch_to_content(self, command: str) -> None:
        """Forward a command to the active content widget.

        Tries VimNavigableScreen first (has _dispatch_command), then
        PipelineOverviewScreen (has _dispatch_command added for integration).
        """
        vim_screen = self._find_active_vim_screen()
        if vim_screen is not None:
            vim_screen._dispatch_command(command)
            return

        overview = self._find_active_overview()
        if overview is not None:
            overview._dispatch_command(command)

    def _forward_key_to_content(self, key: str) -> bool:
        """Forward an unhandled key to the active content widget.

        Used for screen-specific keys that ModeManager doesn't handle
        (e.g. number keys for section jump, ctrl+f/b for paging, 'a' for add).

        Returns True if the key was handled.
        """
        vim_screen = self._find_active_vim_screen()
        if vim_screen is not None:
            return vim_screen._handle_screen_key(key)

        overview = self._find_active_overview()
        if overview is not None:
            return overview._handle_extra_key(key)

        return False

    def _find_active_overview(self) -> PipelineOverviewScreen | None:
        """Find the currently mounted PipelineOverviewScreen, if any."""
        try:
            return self.query_one(PipelineOverviewScreen)
        except NoMatches:
            logger.debug(
                "_find_active_overview: no PipelineOverviewScreen mounted"
            )
            return None

    async def _execute_ex_command(self, command: str) -> None:
        """Execute an ex-mode command."""
        # Handle /search pattern (prefix is / not :)
        if command.startswith("/"):
            pattern = command[1:]
            self._dispatch_search(pattern)
            return

        cmd = command.lstrip(":")

        match cmd:
            case "q" | "quit":
                if self._config_manager and self._config_manager.is_dirty():
                    self._confirm_quit_dirty()
                else:
                    self.exit()
            case "q!" | "quit!":
                self.exit()
            case "w" | "write":
                self._save_config()
            case "wq":
                self._save_config()
                self.exit()
            case "help":
                self._show_help("index")
            case "registers" | "reg":
                self._show_registers()
            case "nohlsearch" | "noh":
                self._clear_search()
            case "u" | "undo":
                await self._undo()
            case "redo":
                await self._redo()
            case _:
                if cmd.startswith("e "):
                    filepath = cmd[2:].strip()
                    await self._open_config(filepath)
                elif cmd.startswith("help "):
                    topic = cmd[5:].strip()
                    self._show_help(topic if topic else "index")
                elif cmd.startswith("s") or cmd.startswith("%s"):
                    self._execute_substitute(cmd)
                else:
                    self._show_status(f"Unknown command: {cmd}")
                    self.notify(f"Unknown command: :{cmd}", severity="warning")

    def _dispatch_search(self, pattern: str) -> None:
        """Forward a /search pattern to the active VimNavigableScreen."""
        screen = self._find_active_vim_screen()
        if screen is not None:
            screen.apply_search(pattern)
            self._show_status(screen.search_status)

    def _clear_search(self) -> None:
        """Clear search highlighting (:noh)."""
        screen = self._find_active_vim_screen()
        if screen is not None:
            screen.apply_search("")
            self._show_status("")

    def _show_registers(self) -> None:
        """Display register contents in the status bar."""
        nonempty = self.register_file.list_nonempty()
        if not nonempty:
            self._show_status("(all registers empty)")
        else:
            parts = [f'"{name}: {val[:30]}' for name, val in nonempty.items()]
            self._show_status("  ".join(parts))

    def _execute_substitute(self, cmd: str) -> None:
        """Execute a :s or :%s substitution command (status display only)."""
        parsed = parse_substitute_command(cmd)
        if parsed is None:
            self._show_status(f"Invalid substitute: {cmd}")
            return
        # Substitution operates on text; for config screens show the pattern match count
        screen = self._find_active_vim_screen()
        if screen is not None:
            import re as _re

            try:
                regex = _re.compile(
                    parsed.pattern,
                    _re.IGNORECASE if parsed.case_insensitive else 0,
                )
            except _re.error as e:
                self._show_status(f"Regex error: {e}")
                return
            count = sum(
                1
                for item in screen.items
                if regex.search(screen.get_item_search_text(item))
            )
            self._show_status(f"/{parsed.pattern}/ matched {count} item(s)")
        else:
            self._show_status("No active screen for substitution")

    def _find_active_vim_screen(self) -> VimNavigableScreen | None:
        """Find the currently mounted VimNavigableScreen, if any."""
        try:
            return self.query_one(VimNavigableScreen)
        except NoMatches:
            logger.debug(
                "_find_active_vim_screen: no VimNavigableScreen mounted"
            )
            return None

    def _show_status(self, text: str) -> None:
        """Show a status message in the validation area."""
        try:
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_validation(text)
        except NoMatches:
            logger.debug("_show_status: #status-bar not found")

    def _confirm_quit_dirty(self) -> None:
        """Warn user about unsaved changes before quitting."""

        def _on_response(response):
            if response.result == DialogResult.CONFIRMED:
                self.exit()

        self.push_screen(
            ConfirmDialog(
                title="Unsaved Changes",
                body="You have unsaved changes. Quit anyway? (Use :wq to save and quit)",
            ),
            callback=_on_response,
        )

    def _save_config(self) -> None:
        """Save current configuration to file."""
        if self._config_manager is None:
            self._show_status("No config loaded")
            return
        try:
            self._config_manager.save(self.config_path)
            self._show_status("Saved")
            self.notify("Config saved", severity="information")
        except (OSError, ValueError) as exc:
            self._show_status(f"Save failed: {exc}")
            self.notify(f"Save failed: {exc}", severity="error")

    async def _undo(self) -> None:
        """Undo last config change."""
        if self._config_manager is None:
            return
        if not self._config_manager.can_undo():
            self._show_status("Nothing to undo")
            return
        self._config_manager.undo()
        await self._refresh_active_screen()
        self._show_status("Undo")

    async def _redo(self) -> None:
        """Redo last undone config change."""
        if self._config_manager is None:
            return
        if not self._config_manager.can_redo():
            self._show_status("Nothing to redo")
            return
        self._config_manager.redo()
        await self._refresh_active_screen()
        self._show_status("Redo")

    async def _refresh_active_screen(self) -> None:
        """Refresh the currently active VimNavigableScreen after config change."""
        screen = self._find_active_vim_screen()
        if screen is not None:
            await screen.refresh_from_config()

    async def _open_config(self, filepath: str) -> None:
        """Open a configuration file and show the overview screen."""
        self.config_path = Path(filepath)

        # Load config via ConfigManager
        try:
            if self.config_path.exists():
                self._config_manager = ConfigManager.from_file(
                    self.config_path
                )
                self._show_status(f"Opened {self.config_path.name}")
            else:
                self._config_manager = ConfigManager()
                self._show_status(f"New config: {self.config_path.name}")
        except (OSError, ValueError) as exc:
            self._show_status(f"Open failed: {exc}")
            self.notify(f"Failed to open config: {exc}", severity="error")
            return

        # Update status bar
        try:
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_file_path(str(self.config_path))
        except NoMatches:
            logger.debug("_open_config: #status-bar not found for path update")

        # Show the overview screen in main content
        await self._show_overview()

    async def _show_overview(self) -> None:
        """Display the pipeline overview in the main content area."""
        if self._config_manager is None:
            return

        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            overview = PipelineOverviewScreen(
                config_manager=self._config_manager,
            )
            # Mount as widget rather than push as screen,
            # keeping the status bar and command line visible
            await main_content.mount(overview)
        except NoMatches:
            logger.debug("_show_overview: #main-content container not found")

    async def on_pipeline_overview_screen_section_selected(
        self, event: PipelineOverviewScreen.SectionSelected
    ) -> None:
        """Handle section drill-down from the overview screen."""
        if self._config_manager is None:
            return

        match event.section_key:
            case "data_model":
                await self._show_data_model()
            case "entity_sources":
                await self._show_entity_browser()
            case "relationship_sources":
                await self._show_relationship_browser()
            case "source_mappings":
                await self._show_source_mapper()
            case "query_lineage":
                await self._show_query_lineage()
            case "pipeline_run":
                await self._show_pipeline_run()
            case _:
                try:
                    status_bar = self.query_one("#status-bar", StatusBar)
                    status_bar.update_hints(f"Selected: {event.section_key}")
                except NoMatches:
                    logger.debug("section_selected: #status-bar not found")

    async def _show_data_sources(self) -> None:
        """Show the data sources configuration screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = DataSourcesScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_data_sources: #main-content container not found"
            )

    async def _show_data_model(self) -> None:
        """Show the data model overview screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = DataModelScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug("_show_data_model: #main-content container not found")

    async def _show_query_lineage(self) -> None:
        """Show the query lineage and data flow visualization screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = QueryLineageScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_query_lineage: #main-content container not found"
            )

    async def _show_pipeline_run(self) -> None:
        """Show the pipeline run/testing screen (dry run + real execution)."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = PipelineTestingScreen(
                config_manager=self._config_manager,
                config_path=self.config_path,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_pipeline_run: #main-content container not found"
            )

    async def _show_entity_browser(self) -> None:
        """Show the entity browser screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = EntityBrowserScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_entity_browser: #main-content container not found"
            )

    async def _show_entity_editor(self, entity_type: str) -> None:
        """Show the entity editor screen for a specific entity type."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = EntityEditorScreen(
                config_manager=self._config_manager,
                entity_type=entity_type,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_entity_editor: #main-content container not found"
            )

    async def _show_relationship_browser(self) -> None:
        """Show the relationship browser screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = RelationshipBrowserScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_relationship_browser: #main-content container not found"
            )

    async def _show_relationship_editor(self, relationship_type: str) -> None:
        """Show the relationship editor screen for a specific relationship type."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = RelationshipEditorScreen(
                config_manager=self._config_manager,
                relationship_type=relationship_type,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_relationship_editor: #main-content container not found"
            )

    async def _show_source_mapper(self) -> None:
        """Show the data source mapper screen."""
        if self._config_manager is None:
            return
        try:
            main_content = self.query_one("#main-content", Container)
            await main_content.remove_children()
            screen = DataSourceMapperScreen(
                config_manager=self._config_manager,
            )
            await main_content.mount(screen)
        except NoMatches:
            logger.debug(
                "_show_source_mapper: #main-content container not found"
            )

    async def on_data_source_mapper_screen_drill_down(
        self, event: DataSourceMapperScreen.DrillDown
    ) -> None:
        """Handle drill-down from source mapper to entity/relationship editor."""
        if event.mapping_type == "entity":
            await self._show_entity_browser()
        else:
            await self._show_relationship_browser()

    async def on_entity_browser_screen_drill_down(
        self, event: EntityBrowserScreen.DrillDown
    ) -> None:
        """Handle drill-down from entity browser to entity editor."""
        await self._show_entity_editor(event.entity_type)

    async def on_relationship_browser_screen_drill_down(
        self, event: RelationshipBrowserScreen.DrillDown
    ) -> None:
        """Handle drill-down from relationship browser to relationship editor."""
        await self._show_relationship_editor(event.relationship_type)

    async def on_data_model_screen_drill_down(
        self, event: DataModelScreen.DrillDown
    ) -> None:
        """Handle drill-down from data model screen."""
        if event.node_type == "entity":
            await self._show_entity_editor(event.label)
        elif event.node_type == "relationship":
            await self._show_relationship_editor(event.label)
        else:
            await self._show_data_sources()

    async def on_query_lineage_screen_drill_down(
        self, event: QueryLineageScreen.DrillDown
    ) -> None:
        """Handle drill-down from query lineage screen to specific components."""
        # For now, just navigate to data sources - could be enhanced to route
        # to specific screens based on component type
        await self._show_data_sources()

    async def on_vim_navigable_screen_navigate_back(
        self, event: VimNavigableScreen.NavigateBack
    ) -> None:
        """Handle back navigation from any VimNavigableScreen."""
        await self._show_overview()

    def open_fod_catalog(self) -> None:
        """Push the FastOpenData catalog screen if available.

        Called from DataSourcesScreen when the user presses ``f``.
        Falls back to a notification when the optional ``fod`` extra
        is not installed.
        """
        if FodCatalogScreen is None:
            self.notify(
                "FastOpenData not installed.  "
                "Install with: uv pip install pycypher-tui[fod]",
                severity="warning",
            )
            return
        self.push_screen(FodCatalogScreen())

    async def on_fod_catalog_screen_dataset_selected(
        self, event: "FodCatalogScreen.DatasetSelected"
    ) -> None:
        """Register a catalog selection as an entity data source."""
        if self._config_manager is None:
            self.notify(
                "Open a config file first (:e <file>) before adding sources.",
                severity="warning",
            )
            return

        if not event.uri:
            self.notify(
                f"'{event.dataset_name}' has no file on disk yet — "
                "run the FOD pipeline to materialize it.",
                severity="warning",
            )
            return

        # Sanitize the dataset name into a config-friendly source id.
        source_id = event.dataset_name.strip().lower().replace(" ", "_")
        try:
            self._config_manager.add_entity_source(
                source_id,
                event.uri,
                event.entity_type,
            )
        except (ValueError, KeyError) as exc:
            self.notify(
                f"Failed to add source '{source_id}': {exc}",
                severity="error",
            )
            return

        self.notify(
            f"Added entity source '{source_id}' "
            f"({event.entity_type}) from FOD catalog.",
            severity="information",
        )

        # If the user is currently on the data sources screen, refresh it
        # so the new source appears immediately.
        try:
            screen = self.query_one(DataSourcesScreen)
            await screen.refresh_from_config()
        except NoMatches:
            logger.debug(
                "on_fod_catalog_screen_dataset_selected: "
                "DataSourcesScreen not mounted; skipping refresh"
            )

    async def on_vim_editable_screen_form_cancelled(
        self, event: VimEditableScreen.FormCancelled
    ) -> None:
        """Navigate back to the appropriate parent screen when a form is cancelled."""
        active = self._find_active_vim_screen()
        if isinstance(active, EntityEditorScreen):
            await self._show_entity_browser()
        elif isinstance(active, RelationshipEditorScreen):
            await self._show_relationship_browser()
        else:
            await self._show_overview()

    async def on_vim_editable_screen_form_submitted(
        self, event: VimEditableScreen.FormSubmitted
    ) -> None:
        """Navigate back to the appropriate parent screen after a form is submitted."""
        active = self._find_active_vim_screen()
        if isinstance(active, EntityEditorScreen):
            await self._show_entity_browser()
        elif isinstance(active, RelationshipEditorScreen):
            await self._show_relationship_browser()
        else:
            await self._show_overview()

    def _show_help(self, topic: str = "index") -> None:
        """Show the help screen for the given topic."""
        if HelpScreen is None or self._help_registry is None:
            return
        self.push_screen(HelpScreen(registry=self._help_registry, topic=topic))


def main() -> None:
    """Entry point for the TUI application."""
    import sys

    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    app = PyCypherTUI(config_path=config_path)
    app.run()


if __name__ == "__main__":
    main()
