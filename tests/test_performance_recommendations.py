"""Tests for enhanced performance recommendations and workload analysis.

Validates anti-pattern detection, optimizer-aware recommendations, and
workload-level tuning suggestions in the query profiler.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from pycypher.query_profiler import (
    ProfileReport,
    _generate_recommendations,
    analyze_workload,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class FakeOptimizationPlan:
    """Minimal stand-in for OptimizationPlan."""

    hints: dict[str, Any] = field(default_factory=dict)
    applied_rules: list[str] = field(default_factory=list)


def _recs(**kwargs: Any) -> list[str]:
    """Shorthand for calling _generate_recommendations with defaults."""
    defaults = {
        "query": "MATCH (n) RETURN n",
        "total_ms": 10.0,
        "parse_ms": 1.0,
        "plan_ms": 1.0,
        "clause_timings": {},
        "row_count": 0,
        "hotspot": None,
        "optimization_plan": None,
    }
    defaults.update(kwargs)
    return _generate_recommendations(**defaults)


# ---------------------------------------------------------------------------
# Anti-pattern detection tests
# ---------------------------------------------------------------------------


class TestAntiPatternDetection:
    """Tests for query structure anti-pattern detection."""

    def test_no_where_no_limit_large_result(self) -> None:
        """Detects missing WHERE/LIMIT on large result sets."""
        recs = _recs(
            query="MATCH (p:Person) RETURN p.name",
            row_count=5000,
        )
        assert any("no WHERE or LIMIT" in r for r in recs)

    def test_no_warning_with_where(self) -> None:
        """No warning when WHERE clause is present."""
        recs = _recs(
            query="MATCH (p:Person) WHERE p.age > 30 RETURN p.name",
            row_count=5000,
        )
        assert not any("no WHERE or LIMIT" in r for r in recs)

    def test_no_warning_with_limit(self) -> None:
        """No warning when LIMIT clause is present."""
        recs = _recs(
            query="MATCH (p:Person) RETURN p.name LIMIT 100",
            row_count=5000,
        )
        assert not any("no WHERE or LIMIT" in r for r in recs)

    def test_no_warning_small_result(self) -> None:
        """No warning for small result sets even without WHERE."""
        recs = _recs(
            query="MATCH (p:Person) RETURN p.name",
            row_count=50,
        )
        assert not any("no WHERE or LIMIT" in r for r in recs)

    def test_return_star_warning(self) -> None:
        """Detects RETURN * anti-pattern."""
        recs = _recs(query="MATCH (n:Person) RETURN *")
        assert any("RETURN *" in r for r in recs)

    def test_no_return_star_warning_for_specific_columns(self) -> None:
        """No RETURN * warning for specific column projections."""
        recs = _recs(query="MATCH (n:Person) RETURN n.name, n.age")
        assert not any("RETURN *" in r for r in recs)

    def test_order_by_without_limit(self) -> None:
        """Detects ORDER BY without LIMIT."""
        recs = _recs(
            query="MATCH (p:Person) RETURN p.name ORDER BY p.age",
        )
        assert any("ORDER BY without LIMIT" in r for r in recs)

    def test_order_by_with_limit_no_warning(self) -> None:
        """No warning when ORDER BY has LIMIT."""
        recs = _recs(
            query="MATCH (p:Person) RETURN p.name ORDER BY p.age LIMIT 10",
        )
        assert not any("ORDER BY without LIMIT" in r for r in recs)

    def test_multiple_match_clauses(self) -> None:
        """Detects >2 MATCH clauses as potential cross-products."""
        recs = _recs(
            query=(
                "MATCH (a:Person) MATCH (b:Company) "
                "MATCH (c:Location) RETURN a, b, c"
            ),
        )
        assert any("3 MATCH clauses" in r for r in recs)


# ---------------------------------------------------------------------------
# Optimizer-aware recommendation tests
# ---------------------------------------------------------------------------


class TestOptimizerAwareRecommendations:
    """Tests for recommendations based on optimizer output."""

    def test_high_cardinality_backend_suggestion(self) -> None:
        """Suggests analytical backend for high cardinality."""
        plan = FakeOptimizationPlan(
            hints={"cardinality_estimates": {"Person": 200_000}},
        )
        recs = _recs(optimization_plan=plan)
        assert any("backend='duckdb'" in r or "backend='auto'" in r for r in recs)

    def test_no_backend_suggestion_low_cardinality(self) -> None:
        """No backend suggestion for low cardinality."""
        plan = FakeOptimizationPlan(
            hints={"cardinality_estimates": {"Person": 500}},
        )
        recs = _recs(optimization_plan=plan)
        assert not any("backend=" in r for r in recs)

    def test_filter_pushdown_info(self) -> None:
        """Informs about successful filter pushdown."""
        plan = FakeOptimizationPlan(
            hints={"filter_pushdown_count": 2},
            applied_rules=["FilterPushdown"],
        )
        recs = _recs(optimization_plan=plan)
        assert any("pushed down 2 filter" in r for r in recs)

    def test_limit_pushdown_info(self) -> None:
        """Informs about limit pushdown."""
        plan = FakeOptimizationPlan(
            hints={"limit_pushdown_value": 100},
            applied_rules=["LimitPushdown"],
        )
        recs = _recs(optimization_plan=plan)
        assert any("LIMIT 100 was pushed down" in r for r in recs)

    def test_join_reordering_info(self) -> None:
        """Informs about join reordering with optimal order."""
        plan = FakeOptimizationPlan(
            hints={"optimal_match_order": ["Company", "Person"]},
            applied_rules=["JoinReordering"],
        )
        recs = _recs(optimization_plan=plan)
        assert any("reordered joins" in r for r in recs)

    def test_index_scan_candidates(self) -> None:
        """Reports index scan candidates."""
        plan = FakeOptimizationPlan(
            hints={"index_scan_candidates": ["Person.name", "Company.id"]},
        )
        recs = _recs(optimization_plan=plan)
        assert any("Index scan candidates" in r for r in recs)

    def test_slow_query_no_rules_applied(self) -> None:
        """Warns when slow query has no optimizer rules applied."""
        plan = FakeOptimizationPlan(
            hints={},
            applied_rules=[],
        )
        recs = _recs(optimization_plan=plan, total_ms=600.0)
        assert any("No optimizer rules applied" in r for r in recs)

    def test_fast_query_no_rules_no_warning(self) -> None:
        """No warning for fast queries without optimizer rules."""
        plan = FakeOptimizationPlan(
            hints={},
            applied_rules=[],
        )
        recs = _recs(optimization_plan=plan, total_ms=10.0)
        assert not any("No optimizer rules applied" in r for r in recs)

    def test_none_optimization_plan_no_crash(self) -> None:
        """None optimization plan doesn't cause errors."""
        recs = _recs(optimization_plan=None)
        # Should still work, just no optimizer-aware recommendations
        assert isinstance(recs, list)


# ---------------------------------------------------------------------------
# Workload analysis tests
# ---------------------------------------------------------------------------


class TestWorkloadAnalysis:
    """Tests for aggregate workload pattern analysis."""

    def _make_report(self, **kwargs: Any) -> ProfileReport:
        """Create a ProfileReport with defaults."""
        defaults = {
            "query": "MATCH (n) RETURN n",
            "total_time_ms": 10.0,
            "parse_time_ms": 1.0,
            "plan_time_ms": 1.0,
            "clause_timings": {"Match": 5.0, "Return": 3.0},
            "row_count": 100,
            "hotspot": "Match",
            "recommendations": [],
        }
        defaults.update(kwargs)
        return ProfileReport(**defaults)

    def test_empty_history(self) -> None:
        """Empty history returns no recommendations."""
        assert analyze_workload([]) == []

    def test_slow_parse_pattern(self) -> None:
        """Detects repeated slow parse times."""
        history = [
            self._make_report(parse_time_ms=80.0) for _ in range(8)
        ] + [self._make_report(parse_time_ms=5.0) for _ in range(2)]
        recs = analyze_workload(history)
        assert any("slow parse" in r.lower() for r in recs)

    def test_tail_latency_detection(self) -> None:
        """Detects high tail latency."""
        history = [self._make_report(total_time_ms=10.0) for _ in range(95)]
        # Add outliers
        history += [self._make_report(total_time_ms=500.0) for _ in range(5)]
        recs = analyze_workload(history)
        assert any("tail latency" in r.lower() for r in recs)

    def test_large_results_pattern(self) -> None:
        """Detects consistently large result sets."""
        history = [
            self._make_report(row_count=50_000) for _ in range(8)
        ] + [self._make_report(row_count=100) for _ in range(2)]
        recs = analyze_workload(history)
        assert any("10K rows" in r for r in recs)

    def test_backend_suggestion_large_workload(self) -> None:
        """Suggests backend change for large average workload."""
        history = [self._make_report(row_count=80_000) for _ in range(10)]
        recs = analyze_workload(history)
        assert any("backend=" in r for r in recs)

    def test_hotspot_concentration(self) -> None:
        """Detects dominant clause hotspot."""
        history = [self._make_report(hotspot="Match") for _ in range(9)]
        history += [self._make_report(hotspot="Return")]
        recs = analyze_workload(history)
        assert any("Match clause is the bottleneck" in r for r in recs)

    def test_no_false_positives_healthy_workload(self) -> None:
        """No recommendations for a healthy workload."""
        history = [
            self._make_report(
                total_time_ms=10.0,
                parse_time_ms=2.0,
                row_count=100,
                hotspot="Match",
            )
            for _ in range(10)
        ]
        recs = analyze_workload(history)
        # Hotspot concentration will trigger since all are Match
        # But no timing/size issues should be flagged
        assert not any("slow parse" in r.lower() for r in recs)
        assert not any("tail latency" in r.lower() for r in recs)
        assert not any("10K rows" in r for r in recs)


# ---------------------------------------------------------------------------
# Integration test: profiler generates optimizer-aware recs
# ---------------------------------------------------------------------------


class TestProfilerIntegration:
    """Integration test verifying profiler passes optimizer plan."""

    def test_profiler_uses_optimization_plan(self) -> None:
        """QueryProfiler.profile() passes optimizer plan to recommendations."""
        import numpy as np
        import pandas as pd
        from pycypher.query_profiler import QueryProfiler
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                ID_COLUMN: np.arange(1, 11),
                "name": [f"Person_{i}" for i in range(1, 11)],
            }
        )
        et = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        ctx = Context(entity_mapping=EntityMapping(mapping={"Person": et}))
        star = Star(context=ctx)
        profiler = QueryProfiler(star=star)

        report = profiler.profile("MATCH (p:Person) RETURN p.name")
        # Should execute without error and produce a valid report
        assert report.row_count == 10
        assert isinstance(report.recommendations, list)

    def test_workload_analysis_from_profiler_history(self) -> None:
        """analyze_workload works with real profiler history."""
        import numpy as np
        import pandas as pd
        from pycypher.query_profiler import QueryProfiler
        from pycypher.relational_models import (
            ID_COLUMN,
            Context,
            EntityMapping,
            EntityTable,
        )
        from pycypher.star import Star

        df = pd.DataFrame(
            {
                ID_COLUMN: np.arange(1, 11),
                "name": [f"Person_{i}" for i in range(1, 11)],
            }
        )
        et = EntityTable(
            entity_type="Person",
            identifier="Person",
            column_names=list(df.columns),
            source_obj_attribute_map={"name": "name"},
            attribute_map={"name": "name"},
            source_obj=df,
        )
        ctx = Context(entity_mapping=EntityMapping(mapping={"Person": et}))
        star = Star(context=ctx)
        profiler = QueryProfiler(star=star)

        # Run a few queries to build history
        for _ in range(5):
            profiler.profile("MATCH (p:Person) RETURN p.name")

        recs = analyze_workload(profiler.history)
        assert isinstance(recs, list)
