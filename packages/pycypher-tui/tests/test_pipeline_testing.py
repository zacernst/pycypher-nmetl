"""Tests for the pipeline testing and preview system."""

from __future__ import annotations

import pytest
from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    ProjectConfig,
    QueryConfig,
    RelationshipSourceConfig,
    SourcesConfig,
)

from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.pipeline_testing import (
    DiagnosticEntry,
    ExecutionPlan,
    ExecutionStep,
    PipelineTestingScreen,
    StepStatus,
    build_execution_plan,
    run_dry_execution,
)

# ─── ExecutionStep ────────────────────────────────────────────────────────────


class TestExecutionStep:
    def test_default_status(self):
        step = ExecutionStep(name="test", description="desc", step_type="load")
        assert step.status == StepStatus.PENDING
        assert step.duration_ms == 0.0

    def test_status_icons(self):
        step = ExecutionStep(name="t", description="d", step_type="load")
        step.status = StepStatus.PENDING
        assert step.status_icon == "-"
        step.status = StepStatus.RUNNING
        assert step.status_icon == "~"
        step.status = StepStatus.SUCCESS
        assert step.status_icon == "+"
        step.status = StepStatus.WARNING
        assert step.status_icon == "!"
        step.status = StepStatus.ERROR
        assert step.status_icon == "x"
        step.status = StepStatus.SKIPPED
        assert step.status_icon == "."

    def test_status_label(self):
        step = ExecutionStep(name="t", description="d", step_type="load")
        step.status = StepStatus.SUCCESS
        assert step.status_label == "success"

    def test_error_message(self):
        step = ExecutionStep(
            name="t", description="d", step_type="query",
            status=StepStatus.ERROR, error_message="Syntax error"
        )
        assert step.error_message == "Syntax error"

    def test_warnings_list(self):
        step = ExecutionStep(name="t", description="d", step_type="load")
        step.warnings.append("Missing column")
        assert len(step.warnings) == 1


# ─── DiagnosticEntry ──────────────────────────────────────────────────────────


class TestDiagnosticEntry:
    def test_error_diagnostic(self):
        diag = DiagnosticEntry(
            severity="error",
            category="syntax",
            message="Invalid query syntax",
            suggestion="Check MATCH clause",
            location="query.q1",
        )
        assert diag.severity_icon == "x"
        assert diag.location == "query.q1"

    def test_warning_diagnostic(self):
        diag = DiagnosticEntry(
            severity="warning",
            category="config",
            message="Missing description",
        )
        assert diag.severity_icon == "!"

    def test_info_diagnostic(self):
        diag = DiagnosticEntry(
            severity="info",
            category="runtime",
            message="Using default settings",
        )
        assert diag.severity_icon == "i"

    def test_unknown_severity(self):
        diag = DiagnosticEntry(
            severity="unknown",
            category="config",
            message="test",
        )
        assert diag.severity_icon == "?"


# ─── ExecutionPlan ────────────────────────────────────────────────────────────


class TestExecutionPlan:
    def test_empty_plan(self):
        plan = ExecutionPlan()
        assert plan.step_count == 0
        assert plan.error_count == 0
        assert plan.is_complete
        assert not plan.has_errors
        assert plan.summary == "No steps"

    def test_plan_with_steps(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.SUCCESS),
            ExecutionStep("s3", "d3", "output", status=StepStatus.WARNING),
        ])
        assert plan.step_count == 3
        assert plan.success_count == 2
        assert plan.warning_count == 1
        assert plan.error_count == 0
        assert plan.is_complete
        assert not plan.has_errors

    def test_plan_with_errors(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.ERROR),
        ])
        assert plan.has_errors
        assert plan.error_count == 1

    def test_plan_not_complete(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.PENDING),
        ])
        assert not plan.is_complete

    def test_summary_format(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.ERROR),
            ExecutionStep("s3", "d3", "output", status=StepStatus.WARNING),
        ])
        summary = plan.summary
        assert "1 passed" in summary
        assert "1 errors" in summary
        assert "1 warnings" in summary


# ─── build_execution_plan ─────────────────────────────────────────────────────


class TestBuildExecutionPlan:
    def _make_config_manager(self, config: PipelineConfig) -> ConfigManager:
        from pycypher.ingestion.pipeline_builder import PipelineBuilder
        builder = PipelineBuilder.from_config(config)
        return ConfigManager(builder=builder)

    def test_empty_config(self):
        mgr = self._make_config_manager(PipelineConfig(version="1.0"))
        plan = build_execution_plan(mgr)
        # Should have at least the validation step
        assert plan.step_count >= 1
        assert plan.steps[0].step_type == "validate"

    def test_entity_sources_create_load_steps(self):
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="customers", uri="data/c.csv",
                        entity_type="Customer", id_col="id",
                    ),
                    EntitySourceConfig(
                        id="products", uri="data/p.csv",
                        entity_type="Product", id_col="id",
                    ),
                ]
            ),
        )
        mgr = self._make_config_manager(config)
        plan = build_execution_plan(mgr)
        load_steps = [s for s in plan.steps if s.step_type == "load"]
        assert len(load_steps) == 2
        assert "customers" in load_steps[0].name

    def test_relationships_create_load_steps(self):
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                relationships=[
                    RelationshipSourceConfig(
                        id="follows", uri="data/f.csv",
                        relationship_type="FOLLOWS",
                        source_col="a", target_col="b",
                    ),
                ]
            ),
        )
        mgr = self._make_config_manager(config)
        plan = build_execution_plan(mgr)
        load_steps = [s for s in plan.steps if s.step_type == "load"]
        assert len(load_steps) == 1

    def test_queries_create_execution_steps(self):
        config = PipelineConfig(
            version="1.0",
            queries=[
                QueryConfig(id="q1", inline="MATCH (n) RETURN n"),
                QueryConfig(id="q2", inline="MATCH (n) RETURN count(n)"),
            ],
        )
        mgr = self._make_config_manager(config)
        plan = build_execution_plan(mgr)
        query_steps = [s for s in plan.steps if s.step_type == "query"]
        assert len(query_steps) == 2

    def test_outputs_create_write_steps(self):
        config = PipelineConfig(
            version="1.0",
            queries=[QueryConfig(id="q1", inline="MATCH (n) RETURN n")],
            output=[OutputConfig(query_id="q1", uri="output/result.csv")],
        )
        mgr = self._make_config_manager(config)
        plan = build_execution_plan(mgr)
        output_steps = [s for s in plan.steps if s.step_type == "output"]
        assert len(output_steps) == 1

    def test_full_pipeline_step_order(self):
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="c", uri="d.csv", entity_type="C", id_col="id",
                    ),
                ],
            ),
            queries=[QueryConfig(id="q1", inline="MATCH (n) RETURN n")],
            output=[OutputConfig(query_id="q1", uri="out.csv")],
        )
        mgr = self._make_config_manager(config)
        plan = build_execution_plan(mgr)
        types = [s.step_type for s in plan.steps]
        # validate should come first, then load, query, output
        assert types[0] == "validate"
        assert "load" in types
        assert "query" in types
        assert "output" in types


# ─── run_dry_execution ────────────────────────────────────────────────────────


class TestRunDryExecution:
    def _make_config_manager(self, config: PipelineConfig) -> ConfigManager:
        from pycypher.ingestion.pipeline_builder import PipelineBuilder
        builder = PipelineBuilder.from_config(config)
        return ConfigManager(builder=builder)

    def test_dry_run_empty_config(self):
        mgr = self._make_config_manager(PipelineConfig(version="1.0"))
        plan = run_dry_execution(mgr)
        assert plan.is_complete
        assert plan.total_duration_ms >= 0

    def test_dry_run_valid_query(self):
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="c", uri="d.csv", entity_type="C", id_col="id",
                    ),
                ],
            ),
            queries=[
                QueryConfig(id="q1", inline="MATCH (n) RETURN n"),
            ],
            output=[OutputConfig(query_id="q1", uri="out.csv")],
        )
        mgr = self._make_config_manager(config)
        plan = run_dry_execution(mgr)
        assert plan.is_complete
        query_steps = [s for s in plan.steps if s.step_type == "query"]
        assert len(query_steps) == 1
        assert query_steps[0].status == StepStatus.SUCCESS

    def test_dry_run_invalid_query_creates_diagnostic(self):
        config = PipelineConfig(
            version="1.0",
            queries=[
                QueryConfig(id="bad", inline="NOT VALID CYPHER !!!"),
            ],
        )
        mgr = self._make_config_manager(config)
        plan = run_dry_execution(mgr)
        query_steps = [s for s in plan.steps if s.step_type == "query"]
        assert len(query_steps) == 1
        assert query_steps[0].status == StepStatus.ERROR
        assert len(plan.diagnostics) >= 1
        assert any(d.category == "syntax" for d in plan.diagnostics)

    def test_dry_run_timing(self):
        mgr = self._make_config_manager(PipelineConfig(version="1.0"))
        plan = run_dry_execution(mgr)
        assert plan.total_duration_ms >= 0
        for step in plan.steps:
            assert step.duration_ms >= 0

    def test_dry_run_all_steps_have_status(self):
        config = PipelineConfig(
            version="1.0",
            sources=SourcesConfig(
                entities=[
                    EntitySourceConfig(
                        id="c", uri="d.csv", entity_type="C", id_col="id",
                    ),
                ],
            ),
            queries=[QueryConfig(id="q1", inline="MATCH (n) RETURN n")],
        )
        mgr = self._make_config_manager(config)
        plan = run_dry_execution(mgr)
        for step in plan.steps:
            assert step.status not in (StepStatus.PENDING, StepStatus.RUNNING)


# ─── PipelineTestingScreen ────────────────────────────────────────────────────


class TestPipelineTestingScreen:
    def _make_screen(self, plan=None):
        """Create a screen via __new__ with VimNavigableScreen attributes."""
        screen = PipelineTestingScreen.__new__(PipelineTestingScreen)
        screen._config_manager = ConfigManager()
        screen._plan = plan
        screen._cursor = 0
        screen._items = plan.steps if plan else []
        screen._pending_keys = []
        screen._search_pattern = ""
        screen._search_matches = []
        screen._search_match_idx = -1
        return screen

    def test_test_completed_message(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
        ])
        msg = PipelineTestingScreen.TestCompleted(plan)
        assert msg.plan.step_count == 1

    def test_screen_initial_state(self):
        screen = self._make_screen()
        assert screen.plan is None
        assert screen.cursor == 0

    def test_cursor_movement(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.SUCCESS),
            ExecutionStep("s3", "d3", "output", status=StepStatus.SUCCESS),
        ])
        screen = self._make_screen(plan)
        screen._move_cursor(1)
        assert screen._cursor == 1
        screen._move_cursor(1)
        assert screen._cursor == 2
        screen._move_cursor(1)  # Should clamp
        assert screen._cursor == 2
        screen._move_cursor(-1)
        assert screen._cursor == 1

    def test_cursor_clamps_at_bounds(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load"),
        ])
        screen = self._make_screen(plan)
        screen._move_cursor(-1)
        assert screen._cursor == 0

    def test_jump_to_start_and_end(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load", status=StepStatus.SUCCESS),
            ExecutionStep("s2", "d2", "query", status=StepStatus.SUCCESS),
            ExecutionStep("s3", "d3", "output", status=StepStatus.SUCCESS),
        ])
        screen = self._make_screen(plan)
        screen._jump_to(2)
        assert screen._cursor == 2
        screen._jump_to(0)
        assert screen._cursor == 0

    def test_gg_pending_sequence(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load"),
            ExecutionStep("s2", "d2", "query"),
            ExecutionStep("s3", "d3", "output"),
        ])
        screen = self._make_screen(plan)
        screen._cursor = 2
        screen._pending_keys = ["g"]
        screen._handle_pending("g")
        assert screen._cursor == 0
        assert screen._pending_keys == []

    def test_escape_clears_pending(self):
        screen = self._make_screen()
        screen._pending_keys = ["g"]
        screen._handle_pending("escape")
        assert screen._pending_keys == []

    def test_item_count_with_plan(self):
        plan = ExecutionPlan(steps=[
            ExecutionStep("s1", "d1", "load"),
            ExecutionStep("s2", "d2", "query"),
        ])
        screen = self._make_screen(plan)
        assert screen.item_count == 2

    def test_item_count_no_plan(self):
        screen = self._make_screen()
        assert screen.item_count == 0
