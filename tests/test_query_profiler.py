"""TDD tests for query profiling and bottleneck analysis.

Validates that the QueryProfiler traces individual query execution,
identifies hot spots, and generates optimization recommendations.
"""

from __future__ import annotations

import pytest
from pycypher.query_profiler import ProfileReport, QueryProfiler


@pytest.fixture()
def profiler() -> QueryProfiler:
    """Create a profiler with test data context."""
    import pandas as pd
    from pycypher.relational_models import EntityMapping, EntityTable
    from pycypher.star import Context, Star

    persons = pd.DataFrame(
        {
            "__ID__": list(range(100)),
            "name": [f"Person_{i}" for i in range(100)],
            "age": list(range(100)),
        },
    )
    person_table = EntityTable(
        entity_type="Person",
        identifier="Person",
        column_names=["__ID__", "name", "age"],
        source_obj_attribute_map={"name": "name", "age": "age"},
        attribute_map={"name": "name", "age": "age"},
        source_obj=persons,
    )
    star = Star(
        context=Context(
            entity_mapping=EntityMapping(mapping={"Person": person_table}),
        ),
    )
    return QueryProfiler(star)


class TestProfileReportStructure:
    """Verify ProfileReport has the expected fields."""

    def test_profile_returns_report(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert isinstance(report, ProfileReport)

    def test_report_has_query(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.query == "MATCH (p:Person) RETURN p.name AS name"

    def test_report_has_total_time(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.total_time_ms > 0.0

    def test_report_has_clause_timings(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert len(report.clause_timings) >= 2
        assert "Match" in report.clause_timings
        assert "Return" in report.clause_timings

    def test_report_has_row_count(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.row_count == 100

    def test_report_has_parse_time(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.parse_time_ms >= 0.0

    def test_report_has_plan_time(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.plan_time_ms >= 0.0


class TestHotSpotDetection:
    """Verify the profiler identifies the slowest clause."""

    def test_hotspot_identified(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert report.hotspot is not None
        assert report.hotspot in report.clause_timings

    def test_hotspot_is_slowest_clause(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        max_clause = max(report.clause_timings, key=report.clause_timings.get)  # type: ignore[arg-type]
        assert report.hotspot == max_clause


class TestRecommendations:
    """Verify the profiler generates optimization recommendations."""

    def test_recommendations_is_list(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert isinstance(report.recommendations, list)

    def test_recommendations_are_strings(
        self, profiler: QueryProfiler
    ) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        for rec in report.recommendations:
            assert isinstance(rec, str)


class TestProfileReportDisplay:
    """Verify the report has a human-readable representation."""

    def test_report_str_contains_query(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        text = str(report)
        assert "MATCH" in text

    def test_report_str_contains_timing(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        text = str(report)
        assert "ms" in text

    def test_report_str_multiline(self, profiler: QueryProfiler) -> None:
        report = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        text = str(report)
        assert "\n" in text


class TestMultipleProfiles:
    """Verify profiling multiple queries works correctly."""

    def test_profile_different_queries(self, profiler: QueryProfiler) -> None:
        r1 = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        r2 = profiler.profile(
            "MATCH (p:Person) WHERE p.age > 50 RETURN p.name AS name"
        )
        assert r1.row_count != r2.row_count

    def test_profile_with_parameters(self, profiler: QueryProfiler) -> None:
        report = profiler.profile(
            "MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name",
            parameters={"min": 90},
        )
        assert report.row_count < 100


class TestRegressionDetection:
    """Verify profiler can compare runs for regression detection."""

    def test_compare_returns_delta(self, profiler: QueryProfiler) -> None:
        r1 = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        r2 = profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        delta = r2.total_time_ms - r1.total_time_ms
        # Delta can be positive or negative — just verify it's a number
        assert isinstance(delta, float)

    def test_profile_history_tracked(self, profiler: QueryProfiler) -> None:
        profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        assert len(profiler.history) == 2

    def test_history_cleared(self, profiler: QueryProfiler) -> None:
        profiler.profile("MATCH (p:Person) RETURN p.name AS name")
        profiler.clear_history()
        assert len(profiler.history) == 0
