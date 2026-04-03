"""Data preview dialog for TUI with async loading and tabular display.

Provides a modal dialog that displays sampled data from data sources with:
- Async loading with loading indicators
- Tabular data display with scrolling
- Schema information (column types, row count)
- Column statistics (null counts, unique counts)
- Error handling for invalid sources
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.widgets import DataTable, Label, LoadingIndicator, Static, TabbedContent, TabPane
from textual.worker import Worker, get_current_worker

from pycypher.ingestion.data_preview import DataSampler, PreviewCache, SamplingStrategy
from pycypher_tui.widgets.dialog import DialogResponse, DialogResult, VimDialog

logger = logging.getLogger(__name__)


@dataclass
class PreviewData:
    """Container for preview results."""

    schema_info: dict | None = None
    sample_data: list[dict] | None = None
    column_stats: dict | None = None
    error: str | None = None


class DataPreviewDialog(VimDialog):
    """Modal dialog displaying data source preview with async loading.

    Features:
    - Tabbed interface (Sample Data, Schema, Statistics)
    - Async data loading with loading indicators
    - Error handling and user-friendly error messages
    - VIM keybindings (Escape to close, Tab to switch tabs)
    """

    CSS = """
    DataPreviewDialog {
        align: center middle;
    }

    #dialog-container {
        width: 90;
        height: 30;
        border: thick $accent;
        padding: 1;
        background: $surface;
    }

    #preview-content {
        width: 100%;
        height: 1fr;
    }

    .loading-container {
        width: 100%;
        height: 100%;
        content-align: center middle;
    }

    .error-container {
        width: 100%;
        height: 100%;
        content-align: center middle;
        color: #f7768e;
    }

    .preview-table {
        width: 100%;
        height: 100%;
    }

    .schema-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .stats-info {
        width: 100%;
        height: 100%;
        padding: 1;
    }

    .info-row {
        width: 100%;
        color: #a9b1d6;
        margin-bottom: 1;
    }

    .info-label {
        color: #7aa2f7;
        text-style: bold;
    }

    .stat-header {
        color: #e0af68;
        text-style: bold;
        margin-top: 1;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        source_uri: str,
        source_id: str = "",
        **kwargs,
    ) -> None:
        title = f"Data Preview: {source_id}" if source_id else "Data Preview"
        super().__init__(title=title, **kwargs)
        self.source_uri = source_uri
        self.source_id = source_id
        self._preview_data: PreviewData | None = None
        self._cache = PreviewCache(max_size=16)
        self._worker: Worker | None = None

    def compose(self) -> ComposeResult:
        with Container(id="dialog-container"):
            yield Label(self.dialog_title, id="dialog-title")
            with TabbedContent(id="preview-content"):
                with TabPane("Sample Data", id="tab-data"):
                    yield LoadingIndicator(id="data-loading")
                with TabPane("Schema", id="tab-schema"):
                    yield LoadingIndicator(id="schema-loading")
                with TabPane("Statistics", id="tab-stats"):
                    yield LoadingIndicator(id="stats-loading")
            yield from self.compose_buttons()

    def compose_buttons(self) -> ComposeResult:
        with Horizontal(id="dialog-buttons"):
            yield Static("Press Escape to close", classes="dialog-hint")

    def on_mount(self) -> None:
        """Start data loading in background thread (blocking I/O)."""
        self._worker = self.run_worker(self._load_preview_data, thread=True, exclusive=True)

    def on_unmount(self) -> None:
        """Cancel loading worker when dialog is unmounted."""
        if self._worker is not None and not self._worker.is_finished:
            self._worker.cancel()

    def _load_preview_data(self) -> None:
        """Load preview data in a background thread (blocking I/O)."""
        try:
            # Create sampler with cache
            sampler = DataSampler(self.source_uri, cache=self._cache)

            # Load schema info
            schema = sampler.schema()
            schema_info = {
                "columns": list(zip(schema.column_names, schema.column_types)),
                "row_count": schema.row_count,
            }

            # Sample data (first 100 rows)
            sample_table = sampler.sample(n=100, strategy=SamplingStrategy.HEAD)
            sample_df = sample_table.to_pandas()
            sample_data = sample_df.to_dict('records')

            # Column statistics
            column_stats = sampler.all_column_stats()

            self._preview_data = PreviewData(
                schema_info=schema_info,
                sample_data=sample_data,
                column_stats=column_stats,
            )

            # Update UI on main thread
            self.call_after_refresh(self._update_preview_display)

        except Exception as exc:
            logger.exception("Failed to load preview data for %s", self.source_uri)
            self._preview_data = PreviewData(error=str(exc))
            self.call_after_refresh(self._update_preview_display)

    def _update_preview_display(self) -> None:
        """Update the preview display with loaded data."""
        if self._preview_data is None:
            return

        if self._preview_data.error:
            self._show_error(self._preview_data.error)
            return

        # Update each tab
        self._update_data_tab()
        self._update_schema_tab()
        self._update_stats_tab()

    def _show_error(self, error: str) -> None:
        """Show error message in all tabs."""
        error_widget = Container(
            Label("Error loading data:", classes="info-label"),
            Label(error, classes="info-row"),
            classes="error-container"
        )

        for tab_id in ["tab-data", "tab-schema", "tab-stats"]:
            try:
                tab = self.query_one(f"#{tab_id}")
                tab.remove_children()
                tab.mount(error_widget)
            except Exception:
                logger.debug("Failed to update tab %s with error", tab_id)

    def _update_data_tab(self) -> None:
        """Update the sample data tab with table."""
        if not self._preview_data or not self._preview_data.sample_data:
            return

        try:
            tab = self.query_one("#tab-data")
            tab.remove_children()

            # Create data table
            table = DataTable(classes="preview-table")

            if self._preview_data.sample_data:
                # Add columns from first row
                first_row = self._preview_data.sample_data[0]
                for col_name in first_row.keys():
                    table.add_column(col_name, key=col_name)

                # Add rows
                for row in self._preview_data.sample_data:
                    table.add_row(*[str(row.get(col, "")) for col in first_row.keys()])

            tab.mount(table)

        except Exception as exc:
            logger.exception("Failed to update data tab: %s", exc)

    def _update_schema_tab(self) -> None:
        """Update the schema tab with column information."""
        if not self._preview_data or not self._preview_data.schema_info:
            return

        try:
            tab = self.query_one("#tab-schema")
            tab.remove_children()

            schema_container = VerticalScroll(classes="schema-info")
            schema = self._preview_data.schema_info
            schema_container.mount(Label(f"Total Rows: {schema['row_count']:,}", classes="info-row"))
            schema_container.mount(Label(f"Total Columns: {len(schema['columns'])}", classes="info-row"))
            schema_container.mount(Label("", classes="info-row"))  # spacer
            schema_container.mount(Label("Columns:", classes="info-label"))

            for col_name, col_type in schema["columns"]:
                schema_container.mount(Label(f"  {col_name}: {col_type}", classes="info-row"))

            tab.mount(schema_container)

        except Exception as exc:
            logger.exception("Failed to update schema tab: %s", exc)

    def _update_stats_tab(self) -> None:
        """Update the statistics tab with column stats."""
        if not self._preview_data or not self._preview_data.column_stats:
            return

        try:
            tab = self.query_one("#tab-stats")
            tab.remove_children()

            # Create stats container
            stats_container = VerticalScroll(classes="stats-info")

            for col_name, stats in self._preview_data.column_stats.items():
                stats_container.mount(Label(f"{col_name}:", classes="stat-header"))
                stats_container.mount(Label(f"  Type: {stats.dtype}", classes="info-row"))
                stats_container.mount(Label(f"  Null Count: {stats.null_count:,}", classes="info-row"))
                stats_container.mount(Label(f"  Unique Count: {stats.unique_count:,}", classes="info-row"))

                if stats.min_value is not None:
                    stats_container.mount(Label(f"  Min: {stats.min_value}", classes="info-row"))
                if stats.max_value is not None:
                    stats_container.mount(Label(f"  Max: {stats.max_value}", classes="info-row"))

                stats_container.mount(Label("", classes="info-row"))  # spacer

            tab.mount(stats_container)

        except Exception as exc:
            logger.exception("Failed to update stats tab: %s", exc)

    def on_key(self, event) -> None:
        """Handle keyboard input."""
        match event.key:
            case "escape" | "q":
                self.dismiss(DialogResponse(DialogResult.CANCELLED))
                event.prevent_default()
                event.stop()
            case "tab":
                # Let TabbedContent handle tab switching
                pass
            case _:
                super().on_key(event)