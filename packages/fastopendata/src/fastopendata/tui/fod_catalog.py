"""FastOpenData dataset catalog screen.

Lets the user browse FastOpenData's catalog of datasets and pick one to
add as a TUI data source. Pulls catalog metadata from
``fastopendata.config.Config`` lazily so the TUI keeps working when the
optional ``fod`` extra is not installed.

The screen posts a :class:`FodCatalogScreen.DatasetSelected` message
with enough information for ``DataSourcesScreen`` to register the
chosen file as an entity source.
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


# Formats that ``ContextBuilder`` knows how to ingest as a single file.
# Shapefile / JSON dump formats need extra processing and are filtered out.
_ALLOWED_FORMATS: frozenset[str] = frozenset({"CSV", "PBF/CSV"})


def _entity_type_from_name(name: str) -> str:
    """Derive a CamelCase entity type from a dataset name.

    The first character of each ``_``-separated part is uppercased; the
    rest of each part is preserved verbatim so embedded digits/letters
    such as ``1yr`` are not mangled.

    Examples:
        ``tract_puma_crosswalk`` -> ``TractPumaCrosswalk``
        ``acs_pums_1yr_persons`` -> ``AcsPums1yrPersons``
    """
    parts = [p for p in name.split("_") if p]
    return "".join(p[0].upper() + p[1:] for p in parts)


class FodCatalogScreen(Screen):
    """Browse the FastOpenData catalog and select a dataset.

    Navigation:
        j / Down       Move to next dataset
        k / Up         Move to previous dataset
        Enter          Select current dataset
        q / Escape     Dismiss without selecting
    """

    CSS = """
    FodCatalogScreen {
        align: center middle;
    }

    #fod-container {
        width: 90%;
        height: 90%;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }

    #fod-title {
        text-style: bold;
        color: #7aa2f7;
        height: 1;
        margin-bottom: 1;
    }

    #fod-hint {
        color: #565f89;
        height: 1;
        margin-bottom: 1;
    }

    #fod-list {
        height: 1fr;
        border: solid #283457;
    }

    #fod-list > ListItem {
        padding: 0 1;
        height: auto;
    }

    #fod-list > ListItem.--highlight {
        background: #283457;
    }

    .fod-row-ok {
        color: #c0caf5;
    }

    .fod-row-missing {
        color: #f7768e;
    }

    .fod-empty {
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

    class DatasetSelected(Message):
        """Posted when the user picks a dataset from the catalog."""

        def __init__(
            self,
            dataset_name: str,
            uri: str,
            entity_type: str,
            description: str = "",
        ) -> None:
            super().__init__()
            self.dataset_name = dataset_name
            self.uri = uri
            self.entity_type = entity_type
            self.description = description

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._datasets: dict = {}
        self._fod_available: bool = False
        self._data_path = None
        # Ordered (name, dataset, on_disk) tuples shown in the list.
        self._entries: list[tuple[str, object, bool]] = []

    # --- Layout ---

    def compose(self) -> ComposeResult:
        with Vertical(id="fod-container"):
            yield Label("FastOpenData Catalog", id="fod-title")
            yield Label(
                "j/k or arrows to navigate · Enter to select · q/Esc to cancel",
                id="fod-hint",
            )
            yield ListView(id="fod-list")

    # --- Lifecycle ---

    def on_mount(self) -> None:
        try:
            from fastopendata.config import Config

            fod_config = Config()
            self._datasets = fod_config.datasets
            self._data_path = fod_config.data_path
            self._fod_available = True
        except ImportError:
            self._fod_available = False
            self._datasets = {}

        try:
            list_view = self.query_one("#fod-list", ListView)
        except NoMatches:
            logger.debug("on_mount: #fod-list not found")
            return

        if not self._fod_available:
            list_view.append(
                ListItem(
                    Label(
                        "fastopendata not available — install with:\n"
                        "  uv pip install pycypher-tui[fod]",
                        classes="fod-empty",
                    )
                )
            )
            return

        # Filter to single-file CSV-like datasets that ContextBuilder can read.
        for name, dataset in self._datasets.items():
            if not dataset.output_file:
                continue
            if dataset.format not in _ALLOWED_FORMATS:
                continue
            on_disk = (self._data_path / dataset.output_file).exists()
            self._entries.append((name, dataset, on_disk))

        if not self._entries:
            list_view.append(
                ListItem(
                    Label(
                        "No CSV datasets available in the FOD catalog.",
                        classes="fod-empty",
                    )
                )
            )
            return

        for name, dataset, on_disk in self._entries:
            list_view.append(self._build_item(name, dataset, on_disk))

        list_view.focus()

    # --- Item rendering ---

    @staticmethod
    def _build_item(name: str, dataset, on_disk: bool) -> ListItem:
        """Build a ListItem for a single dataset row."""
        missing_tag = "" if on_disk else "  [NOT ON DISK]"
        line1 = f"{name}  ({dataset.format}, {dataset.approx_size}){missing_tag}"
        description = (dataset.description or "").strip()
        text = f"{line1}\n  {description}" if description else line1
        cls = "fod-row-ok" if on_disk else "fod-row-missing"
        item = ListItem(Label(text, classes=cls))
        # Stash the dataset name on the widget so on_list_view_selected
        # can map back to the underlying entry.
        item._dataset_name = name  # type: ignore[attr-defined]
        return item

    # --- Actions ---

    def action_cursor_down(self) -> None:
        try:
            self.query_one("#fod-list", ListView).action_cursor_down()
        except NoMatches:
            logger.debug("action_cursor_down: #fod-list not found")

    def action_cursor_up(self) -> None:
        try:
            self.query_one("#fod-list", ListView).action_cursor_up()
        except NoMatches:
            logger.debug("action_cursor_up: #fod-list not found")

    def action_cancel(self) -> None:
        self.dismiss()

    # --- Selection handling ---

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        """Translate a ListView selection into a DatasetSelected message."""
        dataset_name = getattr(event.item, "_dataset_name", None)
        if not dataset_name or dataset_name not in self._datasets:
            return

        dataset = self._datasets[dataset_name]
        uri = ""
        if dataset.output_file and self._data_path is not None:
            uri = str(self._data_path / dataset.output_file)

        message = self.DatasetSelected(
            dataset_name=dataset_name,
            uri=uri,
            entity_type=_entity_type_from_name(dataset_name),
            description=(dataset.description or "").strip(),
        )
        # Send through the app so the message survives this screen being
        # dismissed in the same tick.
        self.app.post_message(message)
        self.dismiss()
