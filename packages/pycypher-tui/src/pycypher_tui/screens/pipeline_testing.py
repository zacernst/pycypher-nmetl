"""Pipeline Testing and Preview Screen.

Provides dry run execution, execution plan visualization, error diagnosis,
and comprehensive pipeline validation with VIM-style navigation.

Refactored to use VimNavigableScreen for consistent VIM navigation,
ModeManager integration, search, and register/clipboard support.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable

from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.css.query import NoMatches
from textual.message import Message
from textual.reactive import reactive
from textual.widgets import Label, Static

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.base import (
    BaseDetailPanel,
    BaseListItem,
    VimNavigableScreen,
)

logger = logging.getLogger(__name__)


# ─── Data Models ──────────────────────────────────────────────────────────────


class StepStatus(Enum):
    """Status of a pipeline execution step."""

    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    WARNING = auto()
    ERROR = auto()
    SKIPPED = auto()


@dataclass
class ExecutionStep:
    """A single step in the pipeline execution plan."""

    name: str
    description: str
    step_type: str  # "load", "transform", "query", "output"
    status: StepStatus = StepStatus.PENDING
    duration_ms: float = 0.0
    row_count: int = 0
    error_message: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def status_icon(self) -> str:
        match self.status:
            case StepStatus.PENDING:
                return "-"
            case StepStatus.RUNNING:
                return "~"
            case StepStatus.SUCCESS:
                return "+"
            case StepStatus.WARNING:
                return "!"
            case StepStatus.ERROR:
                return "x"
            case StepStatus.SKIPPED:
                return "."

    @property
    def status_label(self) -> str:
        return self.status.name.lower()


@dataclass
class DiagnosticEntry:
    """A diagnostic entry from error analysis."""

    severity: str  # "error", "warning", "info"
    category: str  # "syntax", "data", "config", "runtime"
    message: str
    suggestion: str = ""
    location: str = ""

    @property
    def severity_icon(self) -> str:
        match self.severity:
            case "error":
                return "x"
            case "warning":
                return "!"
            case "info":
                return "i"
            case _:
                return "?"


@dataclass
class ExecutionPlan:
    """Complete execution plan for a pipeline."""

    steps: list[ExecutionStep] = field(default_factory=list)
    diagnostics: list[DiagnosticEntry] = field(default_factory=list)
    total_duration_ms: float = 0.0
    started_at: str = ""
    completed_at: str = ""

    @property
    def step_count(self) -> int:
        return len(self.steps)

    @property
    def error_count(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.WARNING)

    @property
    def success_count(self) -> int:
        return sum(1 for s in self.steps if s.status == StepStatus.SUCCESS)

    @property
    def is_complete(self) -> bool:
        return all(
            s.status not in (StepStatus.PENDING, StepStatus.RUNNING)
            for s in self.steps
        )

    @property
    def has_errors(self) -> bool:
        return self.error_count > 0

    @property
    def summary(self) -> str:
        parts = []
        if self.success_count:
            parts.append(f"{self.success_count} passed")
        if self.warning_count:
            parts.append(f"{self.warning_count} warnings")
        if self.error_count:
            parts.append(f"{self.error_count} errors")
        skipped = sum(1 for s in self.steps if s.status == StepStatus.SKIPPED)
        if skipped:
            parts.append(f"{skipped} skipped")
        return ", ".join(parts) if parts else "No steps"


# ─── Pipeline Plan Builder ────────────────────────────────────────────────────


def build_execution_plan(config_manager: ConfigManager) -> ExecutionPlan:
    """Build an execution plan from the current pipeline configuration.

    Analyzes the config and creates steps for each data source load,
    query execution, and output write.
    """
    plan = ExecutionPlan()
    cfg = config_manager.get_config()

    # Validation step
    plan.steps.append(ExecutionStep(
        name="Validate Configuration",
        description="Check pipeline configuration validity",
        step_type="validate",
    ))

    # Entity source loading steps
    if cfg.sources:
        for entity in cfg.sources.entities:
            plan.steps.append(ExecutionStep(
                name=f"Load {entity.id}",
                description=f"Load {entity.entity_type} from {entity.uri}",
                step_type="load",
            ))

        # Relationship source loading steps
        for rel in cfg.sources.relationships:
            plan.steps.append(ExecutionStep(
                name=f"Load {rel.id}",
                description=f"Load {rel.relationship_type} from {rel.uri}",
                step_type="load",
            ))

    # Query execution steps
    for query in cfg.queries:
        desc = query.description or query.id
        plan.steps.append(ExecutionStep(
            name=f"Execute {query.id}",
            description=f"Run query: {desc}",
            step_type="query",
        ))

    # Output steps
    for output in cfg.output:
        plan.steps.append(ExecutionStep(
            name=f"Write {output.query_id}",
            description=f"Output to {output.uri}",
            step_type="output",
        ))

    return plan


def run_dry_execution(config_manager: ConfigManager) -> ExecutionPlan:
    """Run a dry execution of the pipeline, validating each step.

    Does not actually load data or execute queries. Instead validates
    that each step can proceed based on configuration.
    """
    plan = build_execution_plan(config_manager)
    cfg = config_manager.get_config()
    start = time.monotonic()

    for step in plan.steps:
        step_start = time.monotonic()
        step.status = StepStatus.RUNNING

        match step.step_type:
            case "validate":
                result = config_manager.validate()
                if result.is_valid:
                    step.status = StepStatus.SUCCESS
                else:
                    if result.errors:
                        step.status = StepStatus.ERROR
                        step.error_message = "; ".join(
                            str(e) for e in result.errors[:3]
                        )
                        for err in result.errors:
                            plan.diagnostics.append(DiagnosticEntry(
                                severity="error",
                                category="config",
                                message=str(err),
                                suggestion="Check pipeline configuration",
                            ))
                    else:
                        step.status = StepStatus.WARNING
                    for warn in result.warnings:
                        step.warnings.append(str(warn))
                        plan.diagnostics.append(DiagnosticEntry(
                            severity="warning",
                            category="config",
                            message=str(warn),
                        ))

            case "load":
                # Validate source exists in config
                step.status = StepStatus.SUCCESS

            case "query":
                # Try to parse the query
                query_id = step.name.replace("Execute ", "")
                query_cfg = next(
                    (q for q in cfg.queries if q.id == query_id), None
                )
                if query_cfg and query_cfg.inline:
                    from pycypher import validate_query
                    errors = validate_query(query_cfg.inline)
                    if not errors:
                        step.status = StepStatus.SUCCESS
                    else:
                        step.status = StepStatus.ERROR
                        step.error_message = errors[0].message
                        for err in errors:
                            plan.diagnostics.append(DiagnosticEntry(
                                severity="error",
                                category="syntax",
                                message=f"Query '{query_id}': {err.message}",
                                suggestion="Check Cypher syntax",
                                location=f"query.{query_id}",
                            ))
                elif query_cfg and query_cfg.source:
                    step.status = StepStatus.SUCCESS
                    step.warnings.append("External query file not validated in dry run")
                else:
                    step.status = StepStatus.ERROR
                    step.error_message = f"Query '{query_id}' has no inline or source"
                    plan.diagnostics.append(DiagnosticEntry(
                        severity="error",
                        category="config",
                        message=f"Query '{query_id}' missing content",
                        suggestion="Add inline query or source file path",
                        location=f"query.{query_id}",
                    ))

            case "output":
                step.status = StepStatus.SUCCESS

        step.duration_ms = (time.monotonic() - step_start) * 1000

    plan.total_duration_ms = (time.monotonic() - start) * 1000
    return plan


def run_real_execution(
    config_manager: ConfigManager,
    config_path: Path | None = None,
    on_step_change: Callable[[ExecutionStep, ExecutionPlan], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> ExecutionPlan:
    """Execute the pipeline against the live runtime, updating per-step status.

    Mirrors ``pycypher.cli.pipeline.run_impl`` but iterates the steps from
    ``build_execution_plan`` so the existing TUI list/detail UI works
    unchanged.  ``on_step_change`` is invoked at every status transition so
    the caller (the TUI) can refresh the display in real time.
    """
    from urllib.parse import urlparse

    from pycypher.cli.pipeline import (  # lazy: heavy imports
        _OUTPUT_ERRORS,
        _QUERY_EXEC_ERRORS,
    )
    from pycypher.ingestion.context_builder import ContextBuilder
    from pycypher.ingestion.output_writer import write_dataframe_to_uri
    from pycypher.ingestion.security import mask_uri_credentials
    from pycypher.star import Star

    plan = build_execution_plan(config_manager)
    cfg = config_manager.get_config()
    config_dir = config_path.parent if config_path else Path.cwd()

    def _resolve_uri(uri: str) -> str:
        """Resolve a relative file path URI against the config directory.

        URIs with explicit schemes (s3://, https://, postgres://, etc.) are
        returned unchanged. Bare paths and ``file://`` URIs are resolved
        relative to ``config_dir`` so the TUI works correctly regardless of
        the launch directory.
        """
        parsed = urlparse(uri)
        scheme = parsed.scheme.lower()
        # Single-character schemes are Windows drive letters, not URI schemes
        if scheme and len(scheme) > 1 and scheme != "file":
            return uri
        path_part = parsed.path if scheme == "file" else uri
        path_obj = Path(path_part)
        if path_obj.is_absolute():
            return str(path_obj)
        return str((config_dir / path_obj).resolve())

    start = time.monotonic()

    def notify(step: ExecutionStep) -> None:
        if on_step_change is not None:
            on_step_change(step, plan)

    def cancelled() -> bool:
        return cancel_check is not None and cancel_check()

    def fail_remaining(reason: str) -> None:
        for s in plan.steps:
            if s.status == StepStatus.PENDING:
                s.status = StepStatus.SKIPPED
                s.error_message = reason
                notify(s)

    # Index lookups so we can find the right step quickly
    load_steps_by_id: dict[str, ExecutionStep] = {}
    for s in plan.steps:
        if s.step_type == "load":
            load_steps_by_id[s.name.removeprefix("Load ")] = s
    query_steps_by_id: dict[str, ExecutionStep] = {}
    for s in plan.steps:
        if s.step_type == "query":
            query_steps_by_id[s.name.removeprefix("Execute ")] = s
    output_steps_by_query_id: dict[str, list[ExecutionStep]] = {}
    for s in plan.steps:
        if s.step_type == "output":
            # "Write <query_id>" — multiple sinks share the same query_id key
            qid = s.name.removeprefix("Write ")
            output_steps_by_query_id.setdefault(qid, []).append(s)

    # ---- Validate ----
    validate_step = next(
        (s for s in plan.steps if s.step_type == "validate"), None
    )
    if validate_step is not None:
        if cancelled():
            fail_remaining("Cancelled before execution")
            plan.total_duration_ms = (time.monotonic() - start) * 1000
            return plan
        validate_step.status = StepStatus.RUNNING
        notify(validate_step)
        v_start = time.monotonic()
        result = config_manager.validate()
        validate_step.duration_ms = (time.monotonic() - v_start) * 1000
        if result.is_valid:
            validate_step.status = StepStatus.SUCCESS
        else:
            validate_step.status = StepStatus.ERROR
            validate_step.error_message = "; ".join(
                str(e) for e in result.errors[:3]
            )
            for err in result.errors:
                plan.diagnostics.append(
                    DiagnosticEntry(
                        severity="error",
                        category="config",
                        message=str(err),
                        suggestion="Fix configuration before running",
                    )
                )
            notify(validate_step)
            fail_remaining("Skipped: validation failed")
            plan.total_duration_ms = (time.monotonic() - start) * 1000
            return plan
        notify(validate_step)

    # ---- Build context (loads each entity / relationship source) ----
    builder = ContextBuilder()

    if cfg.sources:
        for entity_src in cfg.sources.entities:
            if cancelled():
                fail_remaining("Cancelled")
                plan.total_duration_ms = (time.monotonic() - start) * 1000
                return plan
            step = load_steps_by_id.get(entity_src.id)
            if step is None:
                continue
            step.status = StepStatus.RUNNING
            notify(step)
            ls = time.monotonic()
            try:
                builder.add_entity(
                    entity_src.entity_type,
                    _resolve_uri(entity_src.uri),
                    id_col=entity_src.id_col,
                    query=entity_src.query,
                )
                step.status = StepStatus.SUCCESS
            except Exception as exc:  # noqa: BLE001 - UI boundary
                step.status = StepStatus.ERROR
                step.error_message = (
                    f"{type(exc).__name__}: {exc} "
                    f"(uri={mask_uri_credentials(entity_src.uri)})"
                )
                plan.diagnostics.append(
                    DiagnosticEntry(
                        severity="error",
                        category="data",
                        message=step.error_message,
                        location=f"sources.entities.{entity_src.id}",
                        suggestion="Check the source URI and file permissions",
                    )
                )
                step.duration_ms = (time.monotonic() - ls) * 1000
                notify(step)
                fail_remaining("Skipped: source load failed")
                plan.total_duration_ms = (time.monotonic() - start) * 1000
                return plan
            step.duration_ms = (time.monotonic() - ls) * 1000
            notify(step)

        for rel_src in cfg.sources.relationships:
            if cancelled():
                fail_remaining("Cancelled")
                plan.total_duration_ms = (time.monotonic() - start) * 1000
                return plan
            step = load_steps_by_id.get(rel_src.id)
            if step is None:
                continue
            step.status = StepStatus.RUNNING
            notify(step)
            ls = time.monotonic()
            try:
                builder.add_relationship(
                    rel_src.relationship_type,
                    _resolve_uri(rel_src.uri),
                    source_col=rel_src.source_col,
                    target_col=rel_src.target_col,
                    id_col=rel_src.id_col,
                    query=rel_src.query,
                )
                step.status = StepStatus.SUCCESS
            except Exception as exc:  # noqa: BLE001 - UI boundary
                step.status = StepStatus.ERROR
                step.error_message = (
                    f"{type(exc).__name__}: {exc} "
                    f"(uri={mask_uri_credentials(rel_src.uri)})"
                )
                plan.diagnostics.append(
                    DiagnosticEntry(
                        severity="error",
                        category="data",
                        message=step.error_message,
                        location=f"sources.relationships.{rel_src.id}",
                        suggestion="Check the source URI and file permissions",
                    )
                )
                step.duration_ms = (time.monotonic() - ls) * 1000
                notify(step)
                fail_remaining("Skipped: source load failed")
                plan.total_duration_ms = (time.monotonic() - start) * 1000
                return plan
            step.duration_ms = (time.monotonic() - ls) * 1000
            notify(step)

    try:
        context = builder.build(backend=cfg.backend_engine)
    except (ValueError, RuntimeError, KeyError) as exc:
        plan.diagnostics.append(
            DiagnosticEntry(
                severity="error",
                category="runtime",
                message=f"Context build failed: {exc}",
                suggestion="Inspect ContextBuilder configuration",
            )
        )
        fail_remaining("Skipped: context build failed")
        plan.total_duration_ms = (time.monotonic() - start) * 1000
        return plan

    star = Star(context=context)

    # ---- Queries + outputs ----
    for q in cfg.queries:
        if cancelled():
            fail_remaining("Cancelled")
            plan.total_duration_ms = (time.monotonic() - start) * 1000
            return plan
        q_step = query_steps_by_id.get(q.id)
        if q_step is None:
            continue

        q_step.status = StepStatus.RUNNING
        notify(q_step)
        qs = time.monotonic()

        # Load query text
        try:
            if q.inline is not None:
                query_text = q.inline
            elif q.source is not None:
                query_path = (config_dir / q.source).resolve()
                try:
                    query_path.relative_to(config_dir.resolve())
                except ValueError as exc:
                    raise ValueError(
                        f"Query source {q.source!r} escapes the config directory"
                    ) from exc
                query_text = query_path.read_text(encoding="utf-8")
            else:
                raise ValueError(
                    f"Query {q.id!r} has neither 'inline' nor 'source'"
                )
        except (
            ValueError,
            FileNotFoundError,
            PermissionError,
            UnicodeDecodeError,
            OSError,
        ) as exc:
            q_step.status = StepStatus.ERROR
            q_step.error_message = f"Query load failed: {exc}"
            q_step.duration_ms = (time.monotonic() - qs) * 1000
            plan.diagnostics.append(
                DiagnosticEntry(
                    severity="error",
                    category="config",
                    message=q_step.error_message,
                    location=f"query.{q.id}",
                )
            )
            notify(q_step)
            # Mark this query's outputs as skipped, but continue to next query
            for o_step in output_steps_by_query_id.get(q.id, []):
                o_step.status = StepStatus.SKIPPED
                o_step.error_message = "Skipped: query load failed"
                notify(o_step)
            continue

        # Execute query
        try:
            result_df = star.execute_query(query_text)
        except _QUERY_EXEC_ERRORS as exc:
            q_step.status = StepStatus.ERROR
            q_step.error_message = f"{type(exc).__name__}: {exc}"
            q_step.duration_ms = (time.monotonic() - qs) * 1000
            plan.diagnostics.append(
                DiagnosticEntry(
                    severity="error",
                    category="runtime",
                    message=q_step.error_message,
                    location=f"query.{q.id}",
                    suggestion="Check Cypher syntax and bound parameters",
                )
            )
            notify(q_step)
            for o_step in output_steps_by_query_id.get(q.id, []):
                o_step.status = StepStatus.SKIPPED
                o_step.error_message = "Skipped: query execution failed"
                notify(o_step)
            continue

        n_rows = len(result_df) if result_df is not None else 0
        q_step.row_count = n_rows
        q_step.status = StepStatus.SUCCESS
        q_step.duration_ms = (time.monotonic() - qs) * 1000
        notify(q_step)

        # Outputs for this query
        sinks = [o for o in cfg.output if o.query_id == q.id]
        sink_steps = output_steps_by_query_id.get(q.id, [])
        for sink, o_step in zip(sinks, sink_steps, strict=False):
            if cancelled():
                fail_remaining("Cancelled")
                plan.total_duration_ms = (time.monotonic() - start) * 1000
                return plan
            o_step.status = StepStatus.RUNNING
            notify(o_step)
            os_start = time.monotonic()
            try:
                write_dataframe_to_uri(
                    result_df, _resolve_uri(sink.uri), sink.format,
                )
                o_step.row_count = n_rows
                o_step.status = StepStatus.SUCCESS
            except _OUTPUT_ERRORS as exc:
                o_step.status = StepStatus.ERROR
                o_step.error_message = (
                    f"{type(exc).__name__}: {exc} "
                    f"(uri={mask_uri_credentials(sink.uri)})"
                )
                plan.diagnostics.append(
                    DiagnosticEntry(
                        severity="error",
                        category="runtime",
                        message=o_step.error_message,
                        location=f"output.{q.id}",
                        suggestion="Check output path is writable",
                    )
                )
            o_step.duration_ms = (time.monotonic() - os_start) * 1000
            notify(o_step)

    plan.total_duration_ms = (time.monotonic() - start) * 1000
    return plan


# ─── Widgets ──────────────────────────────────────────────────────────────────


class StepListItem(BaseListItem[ExecutionStep]):
    """Single step entry in the list."""

    CSS = """
    StepListItem {
        width: 100%;
        height: 1;
        padding: 0 2;
        color: #a9b1d6;
    }

    StepListItem.item-focused {
        background: #364a82;
        color: #c0caf5;
        text-style: bold;
    }

    StepListItem.step-error {
        color: #f7768e;
    }

    StepListItem.step-warning {
        color: #e0af68;
    }

    StepListItem.step-success {
        color: #9ece6a;
    }
    """

    def __init__(self, step: ExecutionStep, **kwargs) -> None:
        super().__init__(**kwargs)
        self.step = step

    def compose(self) -> ComposeResult:
        timing = f" ({self.step.duration_ms:.1f}ms)" if self.step.duration_ms > 0 else ""
        text = f" {self.step.status_icon} {self.step.name:<30} {self.step.status_label:<8}{timing}"
        yield Label(text)

    def on_mount(self) -> None:
        match self.step.status:
            case StepStatus.ERROR:
                self.add_class("step-error")
            case StepStatus.WARNING:
                self.add_class("step-warning")
            case StepStatus.SUCCESS:
                self.add_class("step-success")


class StepDetailPanel(BaseDetailPanel):
    """Right-side detail panel showing selected step details and diagnostics."""

    def __init__(self, **kwargs) -> None:
        super().__init__(empty_message="Press 'r' to run dry execution", **kwargs)

    def update_step(self, step: ExecutionStep | None, diagnostics: list[DiagnosticEntry] | None = None) -> None:
        """Update the detail panel with step information and related diagnostics."""
        self.remove_children()

        if step is None:
            self.mount(Label("Press 'r' to run dry execution", classes="detail-title"))
            return

        self.mount(Label(f"{step.status_icon}  {step.name}", classes="detail-title"))
        self.mount(Label(f"  Type: {step.step_type}", classes="detail-row"))
        self.mount(Label(f"  Status: {step.status_label}", classes="detail-row"))
        self.mount(Label(f"  Description: {step.description}", classes="detail-row"))

        if step.duration_ms > 0:
            self.mount(Label(f"  Duration: {step.duration_ms:.1f}ms", classes="detail-row"))

        if step.error_message:
            self.mount(Label("  Error", classes="detail-section"))
            self.mount(Label(f"    {step.error_message}", classes="detail-row"))

        if step.warnings:
            self.mount(Label("  Warnings", classes="detail-section"))
            for warn in step.warnings:
                self.mount(Label(f"    ! {warn}", classes="detail-row"))

        # Show related diagnostics
        if diagnostics:
            self.mount(Label("  Diagnostics", classes="detail-section"))
            for diag in diagnostics:
                location = f" [{diag.location}]" if diag.location else ""
                self.mount(Label(f"    {diag.severity_icon} {diag.message}{location}", classes="detail-row"))
                if diag.suggestion:
                    self.mount(Label(f"      Suggestion: {diag.suggestion}", classes="detail-row"))


# ─── Screen ───────────────────────────────────────────────────────────────────


class PipelineTestingScreen(VimNavigableScreen[ExecutionStep]):
    """Pipeline testing and execution screen.

    VIM Navigation (via VimNavigableScreen + ModeManager):
        j/k         - Move between steps
        Enter/l     - View step details
        r           - Run dry execution (validation only)
        R           - Run real execution (load, query, write outputs)
        c           - Cancel running execution between steps
        gg/G        - Jump to first/last step
        /pattern    - Search steps
        n/N         - Next/previous search match
        q/Escape    - Close screen
    """

    CSS = """
    PipelineTestingScreen {
        layout: vertical;
    }

    #summary-bar {
        dock: bottom;
        width: 100%;
        height: 1;
        padding: 0 2;
    }
    """

    class TestCompleted(Message):
        """Posted when a dry run completes."""

        def __init__(self, plan: ExecutionPlan) -> None:
            super().__init__()
            self.plan = plan

    def __init__(
        self,
        config_manager: ConfigManager | None = None,
        config_path: Path | None = None,
        **kwargs,
    ) -> None:
        cm = config_manager or ConfigManager()
        super().__init__(config_manager=cm, **kwargs)
        self._plan: ExecutionPlan | None = None
        self._config_path = config_path
        self._cancel_requested = False
        self._is_running = False
        # Lock used to serialize per-step UI refreshes pushed from the
        # worker thread. Without this, two refreshes can interleave and
        # produce DuplicateIds when both call `_render_list` in parallel.
        import asyncio as _asyncio

        self._refresh_lock = _asyncio.Lock()

    # --- VimNavigableScreen configuration ---

    @property
    def screen_title(self) -> str:
        return "Pipeline Testing"

    @property
    def breadcrumb_text(self) -> str:
        return "Pipeline > Testing & Preview"

    @property
    def footer_hints(self) -> str:
        return (
            " j/k:nav  r:dry  R:run  c:cancel  Enter:details  /search  q:close"
        )

    @property
    def empty_list_message(self) -> str:
        return "Press 'r' for dry run, 'R' to execute the pipeline"

    # --- VimNavigableScreen abstract method implementations ---

    def load_items(self) -> list[ExecutionStep]:
        if self._plan:
            return list(self._plan.steps)
        return []

    def create_list_item(self, item: ExecutionStep, item_id: str) -> BaseListItem:
        return StepListItem(item, id=item_id)

    def create_detail_panel(self) -> BaseDetailPanel:
        return StepDetailPanel(id=self.detail_panel_id)

    def update_detail_panel(self, item: ExecutionStep | None) -> None:
        try:
            detail = self.query_one(f"#{self.detail_panel_id}", StepDetailPanel)
            # Find diagnostics related to this step
            diagnostics = None
            if item and self._plan and self._plan.diagnostics:
                step_name = item.name.replace("Execute ", "").replace("Load ", "")
                diagnostics = [
                    d for d in self._plan.diagnostics
                    if step_name in d.message or step_name in d.location
                ]
            detail.update_step(item, diagnostics)
        except (NoMatches, AttributeError):
            logger.debug("update_detail_panel: #%s not found", self.detail_panel_id)

    def get_item_id(self, item: ExecutionStep) -> str:
        return item.name.replace(" ", "-").lower()

    def get_item_search_text(self, item: ExecutionStep) -> str:
        return f"{item.name} {item.description} {item.step_type} {item.status_label}"

    def on_edit(self, item: ExecutionStep) -> None:
        pass  # View details shown in detail panel

    def on_add(self) -> None:
        pass  # Not applicable for testing screen

    def on_delete(self, item: ExecutionStep) -> None:
        pass  # Not applicable for testing screen

    # --- Screen-specific key overrides ---

    @property
    def _screen_override_keys(self) -> frozenset[str]:
        return frozenset({"r", "R", "c", "q"})

    def handle_extra_key(self, key: str) -> bool:
        match key:
            case "r":
                if not self._is_running:
                    self.run_worker(self._run_dry_execution(), exclusive=True)
                return True
            case "R":
                if not self._is_running:
                    self._cancel_requested = False
                    self.run_worker(
                        self._run_real_execution,
                        exclusive=True,
                        thread=True,
                    )
                return True
            case "c":
                if self._is_running:
                    self._cancel_requested = True
                    self._show_summary_text(
                        "Cancelling after current step…", color="#e0af68"
                    )
                return True
            case "q":
                self.app.pop_screen()
                return True
            case _:
                return False

    # --- Layout override: add summary bar ---

    def compose(self) -> ComposeResult:
        yield from super().compose()
        self.call_after_refresh(self._mount_summary_bar)

    def _mount_summary_bar(self) -> None:
        """Mount summary bar widget after initial compose."""
        try:
            self.query_one("#summary-bar")
        except NoMatches:
            try:
                footer = self.query_one("#screen-footer", Static)
                # `mount_before` belongs to the parent widget, not the
                # target widget itself.
                parent = footer.parent
                if parent is not None:
                    parent.mount(
                        Static("", id="summary-bar"), before=footer,
                    )
            except NoMatches:
                logger.debug("_mount_summary_bar: #screen-footer not found")

    # --- Execution ---

    async def _run_dry_execution(self) -> None:
        """Execute dry run and update display."""
        self._plan = run_dry_execution(self._config_manager)
        await self.refresh_from_config()
        self._update_summary()
        self.post_message(self.TestCompleted(self._plan))

    def _run_real_execution(self) -> None:
        """Execute the pipeline for real (runs on a worker thread)."""
        self._is_running = True
        try:

            def on_step_change(
                _step: ExecutionStep, plan: ExecutionPlan
            ) -> None:
                # Marshal the UI update back onto the event loop.
                self.app.call_from_thread(self._on_step_changed, plan)

            def cancel_check() -> bool:
                return self._cancel_requested

            try:
                plan = run_real_execution(
                    self._config_manager,
                    config_path=self._config_path,
                    on_step_change=on_step_change,
                    cancel_check=cancel_check,
                )
            except Exception as exc:  # noqa: BLE001 - any failure should surface
                logger.exception("Real execution failed")
                self.app.call_from_thread(
                    self._show_summary_text,
                    f"Run failed: {exc}",
                    "#f7768e",
                )
                return

            self._plan = plan
            self.app.call_from_thread(self._on_step_changed, plan)
            self.app.call_from_thread(
                self.post_message, self.TestCompleted(plan)
            )
        finally:
            self._is_running = False

    async def _on_step_changed(self, plan: ExecutionPlan) -> None:
        """Update list + summary after a step transitions (runs on event loop).

        Held under ``_refresh_lock`` so concurrent refreshes from the
        worker thread don't interleave and produce DuplicateIds.
        """
        self._plan = plan
        async with self._refresh_lock:
            try:
                await self.refresh_from_config()
            except Exception:  # noqa: BLE001 - UI refresh must never crash worker
                logger.exception("Failed to refresh step list")
            self._update_summary()

    def _show_summary_text(self, text: str, color: str = "#a9b1d6") -> None:
        """Set the summary bar text directly (used for status / errors)."""
        try:
            bar = self.query_one("#summary-bar", Static)
            bar.update(f" {text}")
            bar.styles.color = color
        except NoMatches:
            logger.debug("_show_summary_text: #summary-bar not found")

    def _update_summary(self) -> None:
        """Update summary bar."""
        if not self._plan:
            return

        summary = self._plan.summary
        timing = f" ({self._plan.total_duration_ms:.1f}ms)"
        color = "#f7768e" if self._plan.has_errors else "#9ece6a"
        self._show_summary_text(f"{summary}{timing}", color)

    # --- Backward compatibility ---

    @property
    def plan(self) -> ExecutionPlan | None:
        return self._plan

    @property
    def cursor(self) -> int:
        return self._cursor
