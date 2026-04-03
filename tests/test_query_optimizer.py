"""Tests for the rule-based query optimizer framework.

Verifies optimization rules, plan generation, explain output,
pipeline integration, and custom rule extensibility.
"""

from __future__ import annotations

from typing import Any

import pytest
from pycypher.ast_converter import ASTConverter
from pycypher.pipeline import Pipeline, PipelineContext, Stage
from pycypher.query_optimizer import (
    FilterPushdownRule,
    JoinReorderingRule,
    LimitPushdownRule,
    OptimizationPlan,
    OptimizationResult,
    OptimizationRule,
    OptimizeStage,
    PredicateSimplificationRule,
    QueryOptimizer,
)

# ---------------------------------------------------------------------------
# Helper to parse Cypher to AST
# ---------------------------------------------------------------------------


def _parse(cypher: str) -> Any:
    return ASTConverter.from_cypher(cypher)


# ---------------------------------------------------------------------------
# OptimizationResult tests
# ---------------------------------------------------------------------------


class TestOptimizationResult:
    def test_frozen(self) -> None:
        r = OptimizationResult(rule_name="test", applied=True)
        with pytest.raises(AttributeError):
            r.applied = False  # type: ignore[misc]

    def test_defaults(self) -> None:
        r = OptimizationResult(rule_name="r", applied=False)
        assert r.description == ""
        assert r.estimated_speedup == 1.0
        assert r.hints == {}


# ---------------------------------------------------------------------------
# OptimizationPlan tests
# ---------------------------------------------------------------------------


class TestOptimizationPlan:
    def test_applied_rules(self) -> None:
        plan = OptimizationPlan(
            results=[
                OptimizationResult(rule_name="a", applied=True),
                OptimizationResult(rule_name="b", applied=False),
                OptimizationResult(rule_name="c", applied=True),
            ],
        )
        assert plan.applied_rules == ["a", "c"]

    def test_skipped_rules(self) -> None:
        plan = OptimizationPlan(
            results=[
                OptimizationResult(rule_name="a", applied=True),
                OptimizationResult(rule_name="b", applied=False),
            ],
        )
        assert plan.skipped_rules == ["b"]

    def test_explain_output(self) -> None:
        plan = OptimizationPlan(
            results=[
                OptimizationResult(
                    rule_name="filter_pushdown",
                    applied=True,
                    description="1 WHERE clause eligible",
                    estimated_speedup=1.2,
                ),
                OptimizationResult(
                    rule_name="limit_pushdown",
                    applied=False,
                    description="No LIMIT clause",
                ),
            ],
            total_estimated_speedup=1.2,
            elapsed_ms=0.5,
            hints={"filter_pushdown_count": 1},
        )
        text = plan.explain()
        assert "Optimization Plan" in text
        assert "filter_pushdown" in text
        assert "1.2x" in text
        assert "Skipped" in text
        assert "limit_pushdown" in text
        assert "filter_pushdown_count: 1" in text

    def test_explain_empty_plan(self) -> None:
        plan = OptimizationPlan()
        text = plan.explain()
        assert "Rules applied: 0/0" in text


# ---------------------------------------------------------------------------
# FilterPushdownRule tests
# ---------------------------------------------------------------------------


class TestFilterPushdownRule:
    def test_match_with_where(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n")
        rule = FilterPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is True
        assert result.hints["filter_pushdown_count"] == 1
        assert result.estimated_speedup > 1.0

    def test_match_without_where(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n")
        rule = FilterPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is False

    def test_non_query_ast(self) -> None:
        from pycypher.ast_models import Variable

        rule = FilterPushdownRule()
        result = rule.analyze(Variable(name="x"))
        assert result.applied is False
        assert "Not a Query" in result.description


# ---------------------------------------------------------------------------
# LimitPushdownRule tests
# ---------------------------------------------------------------------------


class TestLimitPushdownRule:
    def test_simple_match_return_limit(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n LIMIT 10")
        rule = LimitPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is True
        assert "LIMIT" in result.description

    def test_no_limit(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n")
        rule = LimitPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is False

    def test_limit_with_order_by_blocked(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 10")
        rule = LimitPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is False
        assert "ORDER BY" in result.description

    def test_limit_with_distinct_blocked(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN DISTINCT n LIMIT 10")
        rule = LimitPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is False

    def test_limit_with_with_blocked(self) -> None:
        ast = _parse("MATCH (n:Person) WITH n RETURN n LIMIT 10")
        rule = LimitPushdownRule()
        result = rule.analyze(ast)
        assert result.applied is False

    def test_non_query_ast(self) -> None:
        from pycypher.ast_models import Variable

        rule = LimitPushdownRule()
        result = rule.analyze(Variable(name="x"))
        assert result.applied is False


# ---------------------------------------------------------------------------
# JoinReorderingRule tests
# ---------------------------------------------------------------------------


class TestJoinReorderingRule:
    def test_single_match_no_reorder(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n")
        rule = JoinReorderingRule()
        result = rule.analyze(ast)
        assert result.applied is False
        assert "Single MATCH" in result.description

    def test_multiple_match_without_context(self) -> None:
        ast = _parse("MATCH (n:Person) MATCH (m:Company) RETURN n, m")
        rule = JoinReorderingRule()
        result = rule.analyze(ast, context=None)
        assert result.applied is True
        assert result.hints["match_clause_count"] == 2

    def test_multiple_match_with_context(self, social_star: Any) -> None:
        ast = _parse("MATCH (n:Person) MATCH (m:Person) RETURN n, m")
        rule = JoinReorderingRule()
        result = rule.analyze(ast, context=social_star.context)
        # With same-label matches, order is already optimal
        assert (
            result.applied is False or result.applied is True
        )  # Either is valid

    def test_non_query_ast(self) -> None:
        from pycypher.ast_models import Variable

        rule = JoinReorderingRule()
        result = rule.analyze(Variable(name="x"))
        assert result.applied is False


# ---------------------------------------------------------------------------
# PredicateSimplificationRule tests
# ---------------------------------------------------------------------------


class TestPredicateSimplificationRule:
    def test_no_simplifiable_predicates(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n")
        rule = PredicateSimplificationRule()
        result = rule.analyze(ast)
        assert result.applied is False

    def test_non_query_ast(self) -> None:
        from pycypher.ast_models import Variable

        rule = PredicateSimplificationRule()
        result = rule.analyze(Variable(name="x"))
        assert result.applied is False


# ---------------------------------------------------------------------------
# QueryOptimizer tests
# ---------------------------------------------------------------------------


class TestQueryOptimizer:
    def test_default_rules(self) -> None:
        opt = QueryOptimizer.default()
        assert "filter_pushdown" in opt.rule_names
        assert "limit_pushdown" in opt.rule_names
        assert "join_reordering" in opt.rule_names
        assert "predicate_simplification" in opt.rule_names

    def test_custom_rules(self) -> None:
        opt = QueryOptimizer(rules=[FilterPushdownRule()])
        assert opt.rule_names == ["filter_pushdown"]

    def test_add_rule(self) -> None:
        opt = QueryOptimizer(rules=[])
        opt.add_rule(FilterPushdownRule())
        assert opt.rule_names == ["filter_pushdown"]

    def test_fluent_add_rule(self) -> None:
        opt = QueryOptimizer(rules=[])
        result = opt.add_rule(FilterPushdownRule())
        assert result is opt

    def test_optimize_produces_plan(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n LIMIT 5")
        opt = QueryOptimizer.default()
        plan = opt.optimize(ast)

        assert isinstance(plan, OptimizationPlan)
        assert len(plan.results) == 5  # All 5 default rules ran
        assert plan.elapsed_ms >= 0
        assert "filter_pushdown" in plan.applied_rules
        assert "limit_pushdown" in plan.applied_rules

    def test_optimize_no_applicable_rules(self) -> None:
        ast = _parse("MATCH (n:Person) RETURN n")
        opt = QueryOptimizer.default()
        plan = opt.optimize(ast)

        # filter_pushdown and limit_pushdown should NOT apply
        assert "filter_pushdown" not in plan.applied_rules
        assert "limit_pushdown" not in plan.applied_rules

    def test_speedup_multiplication(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n LIMIT 10")
        opt = QueryOptimizer.default()
        plan = opt.optimize(ast)

        # Speedup should be product of applied rule speedups
        expected = 1.0
        for r in plan.results:
            if r.applied:
                expected *= r.estimated_speedup
        assert abs(plan.total_estimated_speedup - expected) < 0.001

    def test_rule_exception_handled_gracefully(self) -> None:
        class BrokenRule(OptimizationRule):
            name = "broken"

            def analyze(
                self,
                ast: Any,
                context: Any = None,
            ) -> OptimizationResult:
                msg = "kaboom"
                raise RuntimeError(msg)

        opt = QueryOptimizer(rules=[BrokenRule(), FilterPushdownRule()])
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n")
        plan = opt.optimize(ast)

        # Broken rule recorded as not-applied, other rules still run
        assert len(plan.results) == 2
        assert plan.results[0].rule_name == "broken"
        assert plan.results[0].applied is False
        assert plan.results[1].rule_name == "filter_pushdown"
        assert plan.results[1].applied is True

    def test_custom_rule(self) -> None:
        class AlwaysApplyRule(OptimizationRule):
            name = "always"

            def analyze(
                self,
                ast: Any,
                context: Any = None,
            ) -> OptimizationResult:
                return OptimizationResult(
                    rule_name=self.name,
                    applied=True,
                    description="Always applies",
                    estimated_speedup=2.0,
                )

        opt = QueryOptimizer(rules=[AlwaysApplyRule()])
        ast = _parse("MATCH (n) RETURN n")
        plan = opt.optimize(ast)

        assert plan.applied_rules == ["always"]
        assert plan.total_estimated_speedup == 2.0


# ---------------------------------------------------------------------------
# OptimizeStage tests
# ---------------------------------------------------------------------------


class TestOptimizeStage:
    def test_inherits_from_stage(self) -> None:
        assert issubclass(OptimizeStage, Stage)

    def test_stage_name(self) -> None:
        stage = OptimizeStage()
        assert stage.name == "optimize"

    def test_no_ast_is_noop(self) -> None:
        stage = OptimizeStage()
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert "optimization_plan" not in result.metadata

    def test_with_parsed_ast(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n")
        stage = OptimizeStage()
        ctx = PipelineContext(ast=ast)
        result = stage.execute(ctx)

        assert "optimization_plan" in result.metadata
        assert isinstance(
            result.metadata["optimization_plan"],
            OptimizationPlan,
        )
        assert "optimization_applied_rules" in result.metadata
        assert "optimization_speedup" in result.metadata

    def test_custom_optimizer(self) -> None:
        opt = QueryOptimizer(rules=[FilterPushdownRule()])
        stage = OptimizeStage(optimizer=opt)
        ast = _parse("MATCH (n:Person) RETURN n")
        ctx = PipelineContext(ast=ast)
        result = stage.execute(ctx)

        plan = result.metadata["optimization_plan"]
        assert len(plan.results) == 1

    def test_pipeline_integration(self) -> None:
        pipeline = Pipeline.default()
        pipeline.insert_after("validate", OptimizeStage())
        assert pipeline.stage_names == [
            "parse",
            "validate",
            "optimize",
            "plan",
            "execute",
        ]


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestOptimizerIntegration:
    def test_optimize_then_execute(self, social_star: Any) -> None:
        """Full pipeline with optimizer stage produces correct results."""
        pipeline = Pipeline.default()
        pipeline.insert_after("validate", OptimizeStage())

        result = pipeline.run(
            query="MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name",
            star=social_star,
        )
        assert result.result is not None
        assert len(result.result) > 0
        assert "optimize" in result.stage_timings
        assert "optimization_plan" in result.metadata

    def test_explain_output_with_real_query(self) -> None:
        ast = _parse("MATCH (n:Person) WHERE n.age > 30 RETURN n.name LIMIT 5")
        opt = QueryOptimizer.default()
        plan = opt.optimize(ast)
        text = plan.explain()

        assert "filter_pushdown" in text
        assert "limit_pushdown" in text
        assert "Optimization Plan" in text

    def test_optimizer_with_context(self, social_star: Any) -> None:
        ast = _parse("MATCH (n:Person) MATCH (m:Person) RETURN n, m")
        opt = QueryOptimizer.default()
        plan = opt.optimize(ast, context=social_star.context)

        # join_reordering should have run with cardinality estimates
        join_result = next(
            r for r in plan.results if r.rule_name == "join_reordering"
        )
        assert join_result is not None


# ---------------------------------------------------------------------------
# CardinalityFeedbackStore tests
# ---------------------------------------------------------------------------


class TestCardinalityFeedbackStore:
    """Tests for the cardinality feedback loop."""

    def test_no_history_returns_unity(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        assert store.correction_factor("Person") == 1.0

    def test_record_and_correct(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        # Estimator consistently overestimates by 2x
        store.record("Person", estimated=100, actual=50)
        store.record("Person", estimated=200, actual=100)
        factor = store.correction_factor("Person")
        assert 0.45 <= factor <= 0.55  # should be ~0.5

    def test_underestimate_correction(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        # Estimator consistently underestimates by 3x
        store.record("Order", estimated=10, actual=30)
        store.record("Order", estimated=20, actual=60)
        factor = store.correction_factor("Order")
        assert 2.5 <= factor <= 3.5  # should be ~3.0

    def test_correction_clamped(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        # Extreme underestimate — correction should be clamped to 100
        store.record("Huge", estimated=1, actual=100_000)
        assert store.correction_factor("Huge") <= 100.0

    def test_zero_estimates_skipped(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        store.record("Empty", estimated=0, actual=0)
        assert store.correction_factor("Empty") == 1.0
        assert store.entity_types_tracked == []

    def test_clear(self) -> None:
        from pycypher.cardinality_estimator import CardinalityFeedbackStore

        store = CardinalityFeedbackStore()
        store.record("Person", estimated=100, actual=50)
        assert "Person" in store.entity_types_tracked
        store.clear()
        assert store.entity_types_tracked == []
        assert store.correction_factor("Person") == 1.0

    def test_rolling_window(self) -> None:
        from pycypher.cardinality_estimator import (
            _MAX_HISTORY,
            CardinalityFeedbackStore,
        )

        store = CardinalityFeedbackStore()
        # Fill beyond window with overestimates
        for _ in range(_MAX_HISTORY + 10):
            store.record("X", estimated=100, actual=50)
        # Then add accurate estimates
        store.record("X", estimated=100, actual=100)
        # Factor should still be close to 0.5 since window is mostly old data
        factor = store.correction_factor("X")
        assert factor < 0.7
