"""End-to-end Pilot test for the Run Pipeline feature.

Mounts the TUI on a tiny real config, drills into the new "Run Pipeline"
section, executes the pipeline, and asserts that steps reach SUCCESS and
the output file is written.

These tests use ``asyncio.run`` rather than ``@pytest.mark.asyncio`` so
they don't depend on pytest-asyncio being installed in the venv.
"""

from __future__ import annotations

import asyncio

from pycypher.ingestion.config import (
    EntitySourceConfig,
    OutputConfig,
    PipelineConfig,
    QueryConfig,
    SourcesConfig,
)

from pycypher_tui.app import PyCypherTUI
from pycypher_tui.config.pipeline import ConfigManager
from pycypher_tui.screens.pipeline_overview import PipelineOverviewScreen
from pycypher_tui.screens.pipeline_testing import (
    PipelineTestingScreen,
    StepStatus,
)


def _build_csv_pipeline(tmp_path):
    src = tmp_path / "people.csv"
    src.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
    out = tmp_path / "out.csv"
    cfg = PipelineConfig(
        version="1.0",
        sources=SourcesConfig(
            entities=[
                EntitySourceConfig(
                    id="people",
                    uri=str(src),
                    entity_type="Person",
                    id_col="id",
                ),
            ],
            relationships=[],
        ),
        queries=[
            QueryConfig(
                id="all_people",
                inline=(
                    "MATCH (p:Person) "
                    "RETURN p.name AS name, p.age AS age"
                ),
            ),
        ],
        output=[OutputConfig(query_id="all_people", uri=str(out))],
    )
    return cfg, out


def test_run_pipeline_section_appears_in_overview(tmp_path):
    """The new section should be reachable from the dashboard."""
    cfg, _ = _build_csv_pipeline(tmp_path)

    async def body():
        app = PyCypherTUI()
        app._config_manager = ConfigManager.from_config(cfg)
        async with app.run_test() as pilot:
            await app._show_overview()
            await pilot.pause()
            await pilot.pause()

            overview = app.query_one(PipelineOverviewScreen)
            section_keys = [s.key for s in overview._items]
            assert "pipeline_run" in section_keys

    asyncio.run(body())


def test_run_pipeline_screen_mounts_with_config_path(tmp_path):
    """Drilling into Run Pipeline mounts PipelineTestingScreen with config_path."""
    cfg, _ = _build_csv_pipeline(tmp_path)
    expected_path = tmp_path / "pipeline.yaml"

    async def body():
        app = PyCypherTUI()
        app._config_manager = ConfigManager.from_config(cfg)
        app.config_path = expected_path

        async with app.run_test() as pilot:
            await app._show_pipeline_run()
            await pilot.pause()

            screen = app.query_one(PipelineTestingScreen)
            assert screen is not None
            # The config_path was forwarded so run_real_execution can resolve
            # query 'source:' files relative to the config directory.
            assert screen._config_path == expected_path
            # No plan yet — the user hasn't pressed R.
            assert screen._plan is None
            # Real execution flag is off
            assert screen._is_running is False

    asyncio.run(body())


def test_run_pipeline_real_execution_via_worker(tmp_path):
    """Pressing R via the worker actually executes and writes the output.

    Exercises the full ``run_worker(thread=True)`` path so call_from_thread
    has a real worker thread to push onto.
    """
    cfg, out_path = _build_csv_pipeline(tmp_path)

    async def body():
        app = PyCypherTUI()
        app._config_manager = ConfigManager.from_config(cfg)
        app.config_path = tmp_path / "pipeline.yaml"

        async with app.run_test() as pilot:
            await app._show_pipeline_run()
            await pilot.pause()

            screen = app.query_one(PipelineTestingScreen)

            # Dispatch through the same code path the user hits:
            # the threaded worker invokes _run_real_execution off-thread,
            # so call_from_thread has a foreign thread to marshal from.
            screen.handle_extra_key("R")

            # Wait for the worker (and resulting UI updates) to complete.
            # The pipeline is tiny — a few hundred ms is plenty.
            for _ in range(50):
                await pilot.pause()
                if screen._plan is not None and not screen._is_running:
                    break

            assert screen._plan is not None, "worker never produced a plan"
            assert not screen._plan.has_errors, [
                (s.name, s.error_message)
                for s in screen._plan.steps
                if s.error_message
            ]
            assert any(
                s.status == StepStatus.SUCCESS for s in screen._plan.steps
            )
            assert out_path.exists()
            rows = out_path.read_text().strip().splitlines()
            assert len(rows) == 3

    asyncio.run(body())
