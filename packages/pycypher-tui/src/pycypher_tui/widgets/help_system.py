"""Help system for the TUI application.

Provides :help and :help <topic> ex-commands with built-in documentation
for navigation, modes, commands, and configuration.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Static

_BUILTIN_TOPICS: dict[str, str] = {
    "index": (
        "PyCypher TUI Help\n"
        "=================\n\n"
        "Navigation\n"
        "  h/j/k/l    Move left/down/up/right\n"
        "  gg         Jump to first item\n"
        "  G          Jump to last item\n"
        "  Enter      Select / drill into section\n"
        "  h          Go back to previous screen\n"
        "  Ctrl-f/b   Page down / page up\n\n"
        "Modes\n"
        "  i / a / o  Enter INSERT mode\n"
        "  v          Enter VISUAL mode\n"
        "  :          Enter COMMAND mode\n"
        "  Escape     Return to NORMAL mode\n\n"
        "Commands  (press : then type)\n"
        "  :w         Save configuration\n"
        "  :e <path>  Open configuration file\n"
        "  :q         Quit\n"
        "  :wq        Save and quit\n"
        "  :help      Show this help\n"
        "  :help <t>  Show help for topic <t>\n"
        "  /pattern   Search forward\n"
        "  :noh       Clear search highlight\n"
        "  :u         Undo\n"
        "  :redo      Redo\n\n"
        "Editing\n"
        "  dd         Delete current item\n"
        "  y          Yank (copy)\n"
        "  p          Paste\n"
        "  Tab        Cycle filter (on data sources)\n\n"
        "Topics: :help navigation, :help modes, :help commands, :help editing\n\n"
        "Press q or Escape to close."
    ),
    "navigation": (
        "Navigation Help\n"
        "===============\n\n"
        "  h          Move left / go back\n"
        "  j          Move down\n"
        "  k          Move up\n"
        "  l          Move right / drill in\n"
        "  gg         Jump to first item\n"
        "  G          Jump to last item\n"
        "  Ctrl-f     Page down\n"
        "  Ctrl-b     Page up\n"
        "  Enter      Confirm / select\n"
        "  Tab        Cycle filter (data sources)\n\n"
        "Press q or Escape to close."
    ),
    "modes": (
        "VIM Modes Help\n"
        "==============\n\n"
        "  NORMAL     Default mode — navigation and commands\n"
        "  INSERT     Text editing — type to insert characters\n"
        "  VISUAL     Selection mode — extend selection with h/j/k/l\n"
        "  COMMAND    Ex-command mode — type commands after :\n\n"
        "Transitions:\n"
        "  i          NORMAL → INSERT (before cursor)\n"
        "  a          NORMAL → INSERT (after cursor)\n"
        "  o          NORMAL → INSERT (new line below)\n"
        "  v          NORMAL → VISUAL\n"
        "  :          NORMAL → COMMAND\n"
        "  Escape     Any mode → NORMAL\n\n"
        "Press q or Escape to close."
    ),
    "commands": (
        "Ex-Commands Help\n"
        "================\n\n"
        "  :w              Save configuration to file\n"
        "  :e <path>       Open configuration file\n"
        "  :q              Quit application\n"
        "  :wq             Save and quit\n"
        "  :help [topic]   Show help (topics: navigation, modes,\n"
        "                  commands, editing)\n"
        "  :registers      Show register contents\n"
        "  :noh            Clear search highlighting\n"
        "  :u              Undo last change\n"
        "  :redo           Redo undone change\n"
        "  :s/pat/rep/     Substitute pattern\n"
        "  :%s/pat/rep/g   Substitute all occurrences\n\n"
        "Press q or Escape to close."
    ),
    "editing": (
        "Editing Help\n"
        "============\n\n"
        "  dd         Delete current item (with confirmation)\n"
        "  y          Yank (copy) current item\n"
        "  p          Paste yanked item\n"
        "  u          Undo last change\n"
        "  Ctrl-r     Redo\n\n"
        "In INSERT mode:\n"
        "  Type       Insert characters\n"
        "  Backspace  Delete character before cursor\n"
        "  Delete     Delete character at cursor\n"
        "  Enter      Insert newline\n"
        "  Tab        Insert tab\n"
        "  Escape     Return to NORMAL mode\n\n"
        "Press q or Escape to close."
    ),
}


class HelpRegistry:
    """Registry for help content and topics.

    Pre-loaded with built-in topics. Custom topics can be registered
    to extend the help system.
    """

    def __init__(self) -> None:
        self._topics: dict[str, str] = dict(_BUILTIN_TOPICS)

    def register_topic(self, topic: str, content: str) -> None:
        """Register help content for a topic."""
        self._topics[topic] = content

    def get_topic(self, topic: str) -> str | None:
        """Get help content for a topic."""
        return self._topics.get(topic)

    def list_topics(self) -> list[str]:
        """List all registered topics."""
        return sorted(self._topics.keys())


class HelpScreen(Screen):
    """Full-screen help display, dismissed with q or Escape."""

    CSS = """
    HelpScreen {
        align: center middle;
    }
    .help-content {
        width: 70;
        max-height: 80%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
        overflow-y: auto;
    }
    """

    def __init__(
        self,
        registry: HelpRegistry | None = None,
        topic: str = "index",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.registry = registry or HelpRegistry()
        self.topic = topic

    def compose(self) -> ComposeResult:
        content = self.registry.get_topic(self.topic)
        if content is None:
            available = ", ".join(self.registry.list_topics())
            content = (
                f"Unknown help topic: {self.topic}\n\n"
                f"Available topics: {available}\n\n"
                "Press q or Escape to close."
            )
        yield Static(content, classes="help-content")

    def on_key(self, event) -> None:
        """Dismiss help on q or Escape."""
        if event.key in ("escape", "q"):
            self.dismiss()
