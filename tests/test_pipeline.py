"""Tests for the query execution pipeline abstraction.

Verifies stage composition, ordering, short-circuiting, timing,
and integration with the Star engine.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pycypher.pipeline import (
    ExecuteStage,
    ParseStage,
    Pipeline,
    PipelineContext,
    PlanStage,
    Stage,
    ValidateStage,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class RecordingStage(Stage):
    """Test helper that records calls."""

    def __init__(self, name: str = "recording") -> None:
        self.name = name
        self.call_count = 0
        self.last_ctx: PipelineContext | None = None

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        self.call_count += 1
        self.last_ctx = ctx
        ctx.metadata[f"{self.name}_called"] = True
        return ctx


class ShortCircuitStage(Stage):
    """Test helper that short-circuits the pipeline."""

    name: str = "short_circuit"

    def __init__(self, reason: str = "test") -> None:
        self.reason = reason

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        ctx.short_circuited = True
        ctx.short_circuit_reason = self.reason
        ctx.result = pd.DataFrame({"cached": [True]})
        return ctx


class ErrorStage(Stage):
    """Test helper that raises an error."""

    name: str = "error"

    def execute(self, ctx: PipelineContext) -> PipelineContext:
        msg = "Stage failed intentionally"
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# PipelineContext tests
# ---------------------------------------------------------------------------


class TestPipelineContext:
    def test_default_values(self) -> None:
        ctx = PipelineContext()
        assert ctx.query_string is None
        assert ctx.ast is None
        assert ctx.result is None
        assert ctx.parameters == {}
        assert ctx.metadata == {}
        assert ctx.stage_timings == {}
        assert ctx.short_circuited is False

    def test_with_query(self) -> None:
        ctx = PipelineContext(
            query_string="MATCH (n) RETURN n",
            query_input="MATCH (n) RETURN n",
            parameters={"x": 1},
        )
        assert ctx.query_string == "MATCH (n) RETURN n"
        assert ctx.parameters == {"x": 1}


# ---------------------------------------------------------------------------
# Stage tests
# ---------------------------------------------------------------------------


class TestStage:
    def test_recording_stage(self) -> None:
        stage = RecordingStage("test")
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert stage.call_count == 1
        assert result.metadata["test_called"] is True

    def test_short_circuit_stage(self) -> None:
        stage = ShortCircuitStage("cache hit")
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert result.short_circuited is True
        assert result.short_circuit_reason == "cache hit"
        assert result.result is not None


# ---------------------------------------------------------------------------
# Pipeline composition tests
# ---------------------------------------------------------------------------


class TestPipelineComposition:
    def test_empty_pipeline(self) -> None:
        pipeline = Pipeline([])
        star = MagicMock()
        result = pipeline.run(query="MATCH (n) RETURN n", star=star)
        assert result.result is None
        assert result.stage_timings == {}

    def test_single_stage(self) -> None:
        stage = RecordingStage("only")
        pipeline = Pipeline([stage])
        star = MagicMock()
        result = pipeline.run(query="test", star=star)
        assert stage.call_count == 1
        assert "only" in result.stage_timings

    def test_stage_ordering(self) -> None:
        stages = [RecordingStage(f"s{i}") for i in range(3)]
        pipeline = Pipeline(stages)
        star = MagicMock()
        result = pipeline.run(query="test", star=star)

        # All stages called
        for s in stages:
            assert s.call_count == 1

        # Timings recorded in order
        assert list(result.stage_timings.keys()) == ["s0", "s1", "s2"]

    def test_short_circuit_skips_remaining(self) -> None:
        s1 = RecordingStage("before")
        s2 = ShortCircuitStage("cache hit")
        s3 = RecordingStage("after")

        pipeline = Pipeline([s1, s2, s3])
        star = MagicMock()
        result = pipeline.run(query="test", star=star)

        assert s1.call_count == 1
        assert s3.call_count == 0  # Skipped!
        assert result.result is not None

    def test_error_propagation(self) -> None:
        s1 = RecordingStage("before")
        s2 = ErrorStage()
        s3 = RecordingStage("after")

        pipeline = Pipeline([s1, s2, s3])
        star = MagicMock()

        with pytest.raises(RuntimeError, match="Stage failed intentionally"):
            pipeline.run(query="test", star=star)

        assert s1.call_count == 1
        assert s3.call_count == 0

    def test_stage_names_property(self) -> None:
        pipeline = Pipeline(
            [
                RecordingStage("a"),
                RecordingStage("b"),
                RecordingStage("c"),
            ],
        )
        assert pipeline.stage_names == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Pipeline mutation tests
# ---------------------------------------------------------------------------


class TestPipelineMutation:
    def test_append(self) -> None:
        pipeline = Pipeline()
        pipeline.append(RecordingStage("a"))
        pipeline.append(RecordingStage("b"))
        assert pipeline.stage_names == ["a", "b"]

    def test_insert_before(self) -> None:
        pipeline = Pipeline([RecordingStage("a"), RecordingStage("c")])
        pipeline.insert_before("c", RecordingStage("b"))
        assert pipeline.stage_names == ["a", "b", "c"]

    def test_insert_after(self) -> None:
        pipeline = Pipeline([RecordingStage("a"), RecordingStage("c")])
        pipeline.insert_after("a", RecordingStage("b"))
        assert pipeline.stage_names == ["a", "b", "c"]

    def test_remove(self) -> None:
        pipeline = Pipeline(
            [
                RecordingStage("a"),
                RecordingStage("b"),
                RecordingStage("c"),
            ],
        )
        pipeline.remove("b")
        assert pipeline.stage_names == ["a", "c"]

    def test_insert_before_missing_raises(self) -> None:
        pipeline = Pipeline([RecordingStage("a")])
        with pytest.raises(ValueError, match="not found"):
            pipeline.insert_before("missing", RecordingStage("b"))

    def test_insert_after_missing_raises(self) -> None:
        pipeline = Pipeline([RecordingStage("a")])
        with pytest.raises(ValueError, match="not found"):
            pipeline.insert_after("missing", RecordingStage("b"))

    def test_remove_missing_raises(self) -> None:
        pipeline = Pipeline([RecordingStage("a")])
        with pytest.raises(ValueError, match="not found"):
            pipeline.remove("missing")

    def test_fluent_chaining(self) -> None:
        pipeline = (
            Pipeline()
            .append(RecordingStage("a"))
            .append(RecordingStage("b"))
            .append(RecordingStage("c"))
        )
        assert pipeline.stage_names == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Default pipeline tests
# ---------------------------------------------------------------------------


class TestDefaultPipeline:
    def test_default_stages(self) -> None:
        pipeline = Pipeline.default()
        assert pipeline.stage_names == ["parse", "validate", "plan", "execute"]

    def test_default_stage_types(self) -> None:
        pipeline = Pipeline.default()
        stages = pipeline._stages
        assert isinstance(stages[0], ParseStage)
        assert isinstance(stages[1], ValidateStage)
        assert isinstance(stages[2], PlanStage)
        assert isinstance(stages[3], ExecuteStage)

    def test_insert_custom_after_parse(self) -> None:
        pipeline = Pipeline.default()

        class LintStage(Stage):
            name = "lint"

            def execute(self, ctx: PipelineContext) -> PipelineContext:
                ctx.metadata["linted"] = True
                return ctx

        pipeline.insert_after("parse", LintStage())
        assert pipeline.stage_names == [
            "parse",
            "lint",
            "validate",
            "plan",
            "execute",
        ]


# ---------------------------------------------------------------------------
# Built-in stage unit tests
# ---------------------------------------------------------------------------


class TestParseStage:
    def test_parse_string_query(self) -> None:
        stage = ParseStage()
        ctx = PipelineContext(
            query_input="MATCH (n:Person) RETURN n",
            query_string="MATCH (n:Person) RETURN n",
        )
        result = stage.execute(ctx)
        assert result.ast is not None
        assert result.ast.__class__.__name__ == "Query"

    def test_parse_ast_passthrough(self) -> None:
        from pycypher.ast_converter import ASTConverter

        ast = ASTConverter.from_cypher("MATCH (n) RETURN n")
        stage = ParseStage()
        ctx = PipelineContext(query_input=ast)
        result = stage.execute(ctx)
        assert result.ast is ast  # Same object

    def test_parse_empty_raises(self) -> None:
        stage = ParseStage()
        ctx = PipelineContext(query_input="")
        with pytest.raises(ValueError, match="non-empty"):
            stage.execute(ctx)

    def test_parse_none_raises(self) -> None:
        stage = ParseStage()
        ctx = PipelineContext(query_input=None)
        with pytest.raises(ValueError, match="non-empty"):
            stage.execute(ctx)


class TestValidateStage:
    def test_validate_without_complexity_limit(self) -> None:
        stage = ValidateStage()
        ctx = PipelineContext(ast=MagicMock())
        result = stage.execute(ctx)
        assert "complexity_score" not in result.metadata

    def test_validate_no_ast_is_noop(self) -> None:
        stage = ValidateStage()
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert result.ast is None


class TestPlanStage:
    def test_plan_no_ast_is_noop(self) -> None:
        stage = PlanStage()
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert result.plan_analysis is None

    def test_plan_no_star_is_noop(self) -> None:
        stage = PlanStage()
        ctx = PipelineContext(ast=MagicMock())
        result = stage.execute(ctx)
        assert result.plan_analysis is None


class TestExecuteStage:
    def test_execute_no_star_raises(self) -> None:
        stage = ExecuteStage()
        ctx = PipelineContext(query_input="MATCH (n) RETURN n")
        with pytest.raises(ValueError, match="Star instance required"):
            stage.execute(ctx)

    def test_execute_no_ast_raises(self) -> None:
        star = MagicMock()
        stage = ExecuteStage()
        ctx = PipelineContext(
            query_input="MATCH (n) RETURN n",
            star=star,
        )
        with pytest.raises(ValueError, match="Parsed AST required"):
            stage.execute(ctx)

    def test_execute_delegates_to_inner_methods(self) -> None:
        """ExecuteStage dispatches to Star's inner execution methods."""
        from pycypher.ast_converter import ASTConverter

        star = MagicMock()
        expected_df = pd.DataFrame({"name": ["Alice"]})
        star._execute_query_binding_frame.return_value = expected_df
        star._query_has_mutations.return_value = False

        ast = ASTConverter.from_cypher("MATCH (n) RETURN n")
        stage = ExecuteStage()
        ctx = PipelineContext(
            query_input="MATCH (n) RETURN n",
            star=star,
            ast=ast,
        )
        result = stage.execute(ctx)
        assert result.result is expected_df
        star._execute_query_binding_frame.assert_called_once_with(ast)
        assert result.metadata["is_mutation"] is False
        assert result.metadata["parsed_ast"] is ast


# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


class TestPipelineIntegration:
    def test_full_pipeline_with_real_star(self, social_star: Any) -> None:
        """End-to-end: pipeline executes query via Star."""
        pipeline = Pipeline.default()
        result = pipeline.run(
            query="MATCH (p:Person) RETURN p.name",
            star=social_star,
        )
        assert result.result is not None
        assert len(result.result) > 0
        assert "name" in result.result.columns
        assert "parse" in result.stage_timings
        assert "execute" in result.stage_timings

    def test_pipeline_with_custom_stage(self, social_star: Any) -> None:
        """Pipeline with custom stage injected."""

        class CountStage(Stage):
            name = "count_check"

            def execute(self, ctx: PipelineContext) -> PipelineContext:
                ctx.metadata["stage_count"] = len(ctx.stage_timings) + 1
                return ctx

        pipeline = Pipeline.default()
        pipeline.insert_after("parse", CountStage())

        result = pipeline.run(
            query="MATCH (p:Person) RETURN p.name",
            star=social_star,
        )
        assert result.metadata.get("stage_count") == 2  # After parse

    def test_pipeline_timing_metadata(self, social_star: Any) -> None:
        """Verify timing metadata is populated."""
        pipeline = Pipeline.default()
        result = pipeline.run(
            query="MATCH (p:Person) RETURN p.name",
            star=social_star,
        )
        for stage_name in ["parse", "validate", "plan", "execute"]:
            assert stage_name in result.stage_timings
            assert result.stage_timings[stage_name] >= 0
